use std::str::FromStr;
use std::sync::Arc;

use crate::api::{
    AccountNamespace, EventsNamespace, MarketNamespace, SubscriptionNamespace, TradeNamespace,
};
use crate::build::knobs::{KnobName, KnobValue, Knobs};
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::SubscriptionRegistry;
use crate::dispatch::DispatchEventBus;
use crate::dispatch::dispatch_table::Product;
use crate::dispatch::io_reactor::{IoReactorInit, run as run_io_reactor};
use crate::types::{ExpectedCompletionEvent, RequestHandle, WsUrl};

use super::completion::{CloseError, Completion, ReadyError, close_completion, ready_completion};
use super::{Client, ClientBuilder, ConfigError};

/// Classify a dead-man disarm failure into a typed cause (`None` = impossible tail).
fn classify_disarm_failure(e: &crate::TradeError) -> Option<crate::dispatch::DeadmanDisarmCause> {
    use crate::dispatch::DeadmanDisarmCause as C;
    match e {
        crate::TradeError::RateLimited { .. } => Some(C::RateLimitExceeded),
        crate::TradeError::QueueFull { .. } => Some(C::NetworkError),
        crate::TradeError::Transport { kind, .. } => Some(match kind {
            crate::TransportErrorKind::TcpConnectTimeout
            | crate::TransportErrorKind::RequestSentNoResponse => C::Timeout,
            _ => C::NetworkError,
        }),
        _ => None,
    }
}

/// Log a failed close-time dead-man disarm and, when its cause is known, publish
/// `DeadmanDisarmFailedEvent`. The disarm can resolve after the ring closes, so the
/// event may never dispatch.
fn publish_disarm_failure(
    bus: &Arc<crate::dispatch::DispatchEventBus>,
    product: crate::dispatch::dispatch_table::Product,
    e: &crate::TradeError,
) {
    tracing::warn!(?product, error = ?e, "dead-man disarm on close() failed");
    if let Some(cause) = classify_disarm_failure(e) {
        let now = bus.clock().now();
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::DeadmanDisarmFailedEvent,
            event_version: 1,
            timestamp_monotonic: now,
            request_id: None,
            payload: crate::dispatch::EventPayload::DeadmanDisarmFailedEvent {
                url: product.order_ws_url(),
                cause,
                // retry_at is wall-clock; no lossless monotonic conversion.
                retry_at_monotonic: None,
                monotonic_ts: now,
            },
        });
    }
}

fn spawn_close_disarm(
    bus: &Arc<DispatchEventBus>,
    product: Product,
    disarm: impl Future<Output = Option<crate::TradeError>> + Send + 'static,
) {
    let bus = Arc::clone(bus);
    tokio::spawn(async move {
        if let Some(e) = disarm.await {
            publish_disarm_failure(&bus, product, &e);
        }
    });
}

impl Client {
    /// Shorthand for [`ClientBuilder::new()`].
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Access the public market-data namespace.
    pub fn market(&self) -> &MarketNamespace {
        &self.market
    }

    /// Access the private account namespace. Credentials are validated at call
    /// time; the first call needing signing surfaces an `AuthError`.
    pub fn account(&self) -> &AccountNamespace {
        &self.account
    }

    /// Access the private trade namespace (order placement / cancel).
    pub fn trade(&self) -> &TradeNamespace {
        &self.trade
    }

    /// Access the WS subscription-lifecycle namespace.
    pub fn subscription(&self) -> &SubscriptionNamespace {
        &self.subscription
    }

    /// Access the unified lifecycle / event-observability namespace.
    pub fn events(&self) -> &EventsNamespace {
        &self.events
    }

    /// Shared `DispatchEventBus` reference.
    pub fn bus(&self) -> &Arc<DispatchEventBus> {
        &self.bus
    }

    /// Atomic snapshot of knob `name` (`None` for unknown).
    pub fn knob(&self, name: &str) -> Option<KnobValue> {
        self.knobs.snapshot(name)
    }

    /// Set a runtime-mutable knob. On success publishes `ConfigChangedEvent`.
    ///
    /// # Errors
    /// - [`ConfigError::ImmutableKnob`] — the knob is construction-only.
    /// - [`ConfigError::InvalidConfig`] — unknown knob name or type mismatch.
    pub fn set_knob(&self, name: &str, value: KnobValue) -> Result<(), ConfigError> {
        if Knobs::is_construction_only(name) {
            return Err(ConfigError::ImmutableKnob {
                knob: name.to_string(),
            });
        }
        if !Knobs::is_runtime_mutable(name) {
            return Err(ConfigError::InvalidConfig {
                detail: format!("unknown knob `{name}`"),
            });
        }
        let previous =
            self.knobs
                .set_runtime(name, &value)
                .ok_or_else(|| ConfigError::InvalidConfig {
                    detail: format!("knob `{name}`: value type mismatch"),
                })?;
        self.bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::ConfigChangedEvent,
            event_version: 1,
            timestamp_monotonic: self.clock.now(),
            request_id: None,
            payload: crate::dispatch::EventPayload::ConfigChangedEvent {
                knob: KnobName::from_str(name).expect("set_runtime accepted a known knob"),
                previous,
                current: value,
            },
        });
        Ok(())
    }

    /// Start the I/O reactor; connections still open lazily. The returned
    /// [`Completion`] resolves once the reactor enters its loop — never hangs.
    pub fn ready(&self) -> Completion<ReadyError> {
        let request_id = self
            .next_client_request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = RequestHandle {
            id: request_id,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };

        let mut handle_guard = self
            .io_reactor_handle
            .lock()
            .expect("io_reactor_handle lock");
        if handle_guard.is_some() {
            // Arm before synthesizing so the terminal resolves this waiter directly.
            let completion = ready_completion(handle, Arc::clone(&self.bus));
            // Idempotent re-entry via the correlated registry. Any recorded death
            // answers here — must not read as a fresh ready.
            let env = if self.bus.loop_death_observed() {
                crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::ClientFailed,
                    event_version: 1,
                    timestamp_monotonic: self.bus.clock().now(),
                    request_id: Some(request_id),
                    payload: crate::dispatch::EventPayload::ClientFailed {
                        cause: crate::dispatch::ClientFailureCause::LoopFailed,
                    },
                }
            } else {
                crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::ClientReady,
                    event_version: 1,
                    timestamp_monotonic: self.bus.clock().now(),
                    request_id: Some(request_id),
                    payload: crate::dispatch::EventPayload::ClientReady {
                        capability_snapshot: self.current_capability_snapshot(),
                    },
                }
            };
            self.bus.deliver_correlated(&env);
            return completion;
        }
        let caller_rx = match self.bus.take_caller_to_io_rx() {
            Some(rx) => rx,
            None => {
                // Input consumed but no handle recorded — resolve with ClientFailed.
                let completion = ready_completion(handle, Arc::clone(&self.bus));
                self.bus
                    .deliver_correlated(&crate::dispatch::EventEnvelope {
                        event_type: crate::dispatch::EventType::ClientFailed,
                        event_version: 1,
                        timestamp_monotonic: self.bus.clock().now(),
                        request_id: Some(request_id),
                        payload: crate::dispatch::EventPayload::ClientFailed {
                            cause: crate::dispatch::ClientFailureCause::ReactorSpawnFailed,
                        },
                    });
                return completion;
            }
        };

        let mut conns = std::collections::HashMap::with_capacity(WsUrl::ALL.len());
        for url in WsUrl::ALL {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&self.bus),
                    Arc::clone(&self.ws_factory),
                    self.supervisor.rate_budget_clone(),
                    Arc::clone(&self.clock),
                    Arc::clone(&self.jitter),
                ),
            );
        }

        let handler_registry =
            crate::dispatch::HandlerRegistry::new(Arc::clone(&self.presence_mirror));

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::with_mirror(Arc::clone(&self.subscription_mirror)),
            handler_registry,
            auth_stack: Arc::clone(&self.auth),
            state_mirrors: self.supervisor.state_mirror_clones(),
            bus_back_ref: Arc::downgrade(&self.bus),
            ready_signal: crate::dispatch::io_reactor::ReadySignal {
                request_id,
                capability_snapshot: self.current_capability_snapshot(),
            },
            connect_id_allocator: self.supervisor.handle_id_allocator(),
            order_gate: self.supervisor.order_gate(),
        };

        // Dispatch reactor before IoReactor — else correlated completions never fire.
        self.bus
            .start_dispatch_reactor(&tokio::runtime::Handle::current());

        // Arm before the reactor can emit, so the first terminal hits this waiter.
        let completion = ready_completion(handle, Arc::clone(&self.bus));
        let join = tokio::spawn(run_io_reactor(init));
        *handle_guard = Some(join);

        completion
    }

    /// Gracefully shut the client down (consumes the client). Resolves once every
    /// managed WS connection drains, or [`CloseError::Interrupted`] if a loop died or the
    /// close marker could not be posted.
    ///
    /// Disarms, best effort and without waiting, each dead-man's switch this client requested an arm
    /// for, unless a disarm requested after that arm was confirmed; none if no arm or no credentials.
    /// A Futures disarm with no caller slot beyond the close marker's is skipped: `DeadmanDisarmFailedEvent`.
    ///
    /// ```no_run
    /// # async fn ex(client: kraken_sdk::Client) -> Result<(), kraken_sdk::CloseError> {
    /// client.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use = "awaiting the returned completion is how you learn shutdown drained; use `let _ = client.close();` to fire-and-forget"]
    pub fn close(self) -> Completion<CloseError> {
        let request_id = self
            .next_client_request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = RequestHandle {
            id: request_id,
            expected_completion: ExpectedCompletionEvent::ClientClosed,
        };
        let initiated_at = self.bus.clock().now();

        // Latch teardown so a reactor ending during handshake is not LoopFailed.
        self.bus.begin_shutdown();

        // Ensure the reactor runs so close drains one path (ready() is idempotent).
        let reactor_running = self
            .io_reactor_handle
            .lock()
            .map(|g| g.is_some())
            .unwrap_or(false);
        if !reactor_running {
            let _ = self.ready();
        }

        // Arm before the marker so the reactor's terminal resolves this waiter.
        let completion = close_completion(handle, Arc::clone(&self.bus));

        if self.auth.api_key().is_some() {
            if let Some(disarm) = self.trade.futures().begin_close_disarm(&self.bus) {
                spawn_close_disarm(&self.bus, Product::Futures, disarm);
            }

            // Best-effort dead-man disarm over REST; a detached task awaits it.
            if let Some(disarm) = self.trade.begin_close_disarm() {
                spawn_close_disarm(&self.bus, Product::Spot, disarm);
            }
        }

        // One close marker; reactor fans CallClose to every managed connection and self-exits.
        match self.bus.try_post_caller_inbound_recovering_kind(
            crate::dispatch::CallerInbound::ClientClose {
                request_id,
                initiated_at,
            },
        ) {
            Ok(()) => {
                // Detach so the reactor self-exits; Drop then finds None.
                if let Ok(mut g) = self.io_reactor_handle.lock() {
                    let _ = g.take();
                }
            }
            // Marker never posted — not a drained shutdown; resolve Interrupted.
            Err((_, reject)) => {
                self.bus.on_loop_death(
                    crate::dispatch::ReactorName::Io,
                    crate::dispatch::LoopFailureCause::Cancelled,
                );
                tracing::warn!(
                    target: "kraken_sdk::client",
                    request_id,
                    ?reject,
                    "close: caller_to_io rejected the marker; close().await resolves Interrupted"
                );
            }
        }

        completion
    }

    /// Snapshot of capabilities declared at `.build()` time.
    fn current_capability_snapshot(&self) -> crate::types::CapabilitySnapshot {
        use crate::types::NamespaceName;
        let mut ns: std::collections::HashSet<NamespaceName> =
            std::collections::HashSet::with_capacity(5);
        ns.insert(NamespaceName::Market);
        ns.insert(NamespaceName::Account);
        ns.insert(NamespaceName::Trade);
        ns.insert(NamespaceName::Ws);
        crate::types::CapabilitySnapshot {
            declared_namespaces: ns,
            declared_ws_urls: WsUrl::ALL.into_iter().collect(),
            discovered_at_first_use: std::collections::HashSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, classify_disarm_failure, publish_disarm_failure};
    use crate::dispatch::DeadmanDisarmCause as C;
    use crate::dispatch::dispatch_table::Product;
    use crate::types::WsUrl;
    use crate::{TradeError, TransportErrorKind};

    #[test]
    fn a_disarm_failure_reports_the_connection_of_the_product_it_targeted() {
        let client = Client::builder().build().expect("client builds");
        let refused = TradeError::RateLimited {
            request_id: None,
            retry_after_ts: None,
        };
        for product in [Product::Spot, Product::Futures] {
            publish_disarm_failure(&client.bus, product, &refused);
        }
        let urls: Vec<WsUrl> = client
            .bus
            .test_drain_published()
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::DeadmanDisarmFailedEvent { url, .. } => Some(url),
                _ => None,
            })
            .collect();
        assert_eq!(urls, [WsUrl::Auth, WsUrl::UnifiedPrivate]);
    }

    #[test]
    fn disarm_failure_classifies_meaningful_causes_and_drops_the_impossible_tail() {
        assert_eq!(
            classify_disarm_failure(&TradeError::RateLimited {
                request_id: None,
                retry_after_ts: None
            }),
            Some(C::RateLimitExceeded)
        );
        assert_eq!(
            classify_disarm_failure(&TradeError::Transport {
                request_id: None,
                kind: TransportErrorKind::TcpConnectTimeout,
                transient: false,
            }),
            Some(C::Timeout)
        );
        assert_eq!(
            classify_disarm_failure(&TradeError::Transport {
                request_id: None,
                kind: TransportErrorKind::SocketReset,
                transient: true,
            }),
            Some(C::NetworkError)
        );
        assert_eq!(
            classify_disarm_failure(&TradeError::Transport {
                request_id: None,
                kind: TransportErrorKind::RequestSentNoResponse,
                transient: true,
            }),
            Some(C::Timeout)
        );
        assert_eq!(
            classify_disarm_failure(&TradeError::InsufficientFunds { request_id: None }),
            None
        );
    }

    use crate::ApiKey;
    use crate::test_logs::capture_logs;
    use crate::transport::{HttpTransport, TransportError};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingTransport(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl HttpTransport for RecordingTransport {
        async fn get_json(
            &self,
            _path: &str,
            _query_params: &[(&str, &str)],
        ) -> Result<serde_json::Value, TransportError> {
            Ok(serde_json::Value::Null)
        }
        async fn post_form_signed(
            &self,
            path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<serde_json::Value, TransportError> {
            self.0.lock().expect("posts lock").push(path.to_owned());
            Err(TransportError {
                kind: TransportErrorKind::TcpRefused,
                transient: false,
            })
        }
    }

    fn close_and_capture(
        credentials: Option<(ApiKey, String)>,
        futures_arm: bool,
    ) -> (String, Vec<String>) {
        let transport = Arc::new(RecordingTransport::default());
        let rig_transport = Arc::clone(&transport);
        let logs = capture_logs(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let client = {
                    let _g = crate::build::test_env::env_lock();
                    let builder = Client::builder().with_transport(rig_transport);
                    match credentials {
                        Some((key, secret)) => builder.with_api_key(key, secret),
                        None => builder,
                    }
                    .build()
                    .expect("build")
                };
                client
                    .trade()
                    .cancel_all_orders_after(60)
                    .via(crate::dispatch::Transport::Rest)
                    .await
                    .expect_err("the arm is refused");
                if futures_arm {
                    client
                        .trade()
                        .futures()
                        .cancel_all_orders_after(60)
                        .await
                        .expect_err("a Futures arm cannot be sent without credentials");
                }
                client.close().await.expect("close resolves Ok");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            });
        });
        let posts = transport.0.lock().expect("posts lock").clone();
        (logs, posts)
    }

    const DISARM_FAILED: &str = "dead-man disarm on close() failed";

    #[test]
    fn close_without_credentials_does_not_attempt_a_dead_man_disarm() {
        let (logs, posts) = close_and_capture(None, true);
        assert!(
            !logs.contains(DISARM_FAILED),
            "no disarm may be attempted without credentials; logs: {logs}"
        );
        assert!(posts.is_empty(), "unexpected signed posts: {posts:?}");
    }

    #[tokio::test]
    async fn close_without_credentials_queues_nothing_ahead_of_the_close_marker() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder()
                .with_knob(
                    "caller_to_io_capacity",
                    crate::build::knobs::KnobValue::U32(1),
                )
                .build()
                .expect("build")
        };
        client
            .trade()
            .futures()
            .cancel_all_orders_after(60)
            .await
            .expect_err("a Futures arm cannot be sent without credentials");
        tokio::time::timeout(std::time::Duration::from_secs(2), client.close())
            .await
            .expect("close must not hang")
            .expect("a disarm must not take the slot the close marker needs");
    }

    #[test]
    fn close_with_credentials_disarms_after_a_refused_spot_arm_and_logs_the_failure() {
        use base64::Engine;
        let secret = base64::engine::general_purpose::STANDARD.encode([0x01u8; 32]);

        let (logs, posts) = close_and_capture(Some((ApiKey::new("test-key"), secret)), false);
        assert_eq!(
            posts,
            [
                "/0/private/CancelAllOrdersAfter",
                "/0/private/CancelAllOrdersAfter"
            ],
            "the refused arm, then close's disarm"
        );
        assert!(
            logs.contains(DISARM_FAILED),
            "the refused disarm must log: {logs}"
        );
    }
}
