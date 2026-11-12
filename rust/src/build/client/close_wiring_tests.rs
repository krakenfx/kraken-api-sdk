use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};

use super::Client;
use crate::dispatch::{DeadmanDisarmCause, EventPayload, EventType, Transport};
use crate::transport::driveable_mock::{DriveableSocketHandle, DriveableWsSocketFactory};
use crate::transport::{HttpTransport, TransportError, TransportErrorKind, WsSocketFactoryLike};
use crate::types::WsUrl;
use crate::{ApiKey, ConnectionState, EventSubscription, KnobValue};

const BUDGET: Duration = Duration::from_secs(5);
const SETTLE_POLLS: usize = 200;

struct ScriptedRest {
    disarm_fails: bool,
    hold_token: AtomicBool,
    posted: Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl HttpTransport for ScriptedRest {
    async fn get_json(
        &self,
        _path: &str,
        _query_params: &[(&str, &str)],
    ) -> Result<Value, TransportError> {
        Ok(Value::Null)
    }

    async fn post_form_signed(
        &self,
        path: &str,
        body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<Value, TransportError> {
        self.posted
            .lock()
            .expect("posted lock")
            .push((path.to_string(), body.to_string()));
        if path.ends_with("GetWebSocketsToken") {
            while self.hold_token.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            return Ok(json!({ "error": [], "result": { "token": "tok", "expires": 900 } }));
        }
        let disarm = body.split('&').any(|param| param == "timeout=0");
        if path.ends_with("CancelAllOrdersAfter") && disarm && self.disarm_fails {
            return Err(TransportError {
                kind: TransportErrorKind::TcpConnectTimeout,
                transient: false,
            });
        }
        let trigger_time = if disarm { "0" } else { "2026-01-01T00:01:00Z" };
        Ok(json!({
            "error": [],
            "result": { "currentTime": "2026-01-01T00:00:00Z", "triggerTime": trigger_time }
        }))
    }
}

type Reported = Vec<(WsUrl, DeadmanDisarmCause)>;

struct CloseRig {
    client: Option<Client>,
    factory: Arc<DriveableWsSocketFactory>,
    rest: Arc<ScriptedRest>,
    failures: Arc<Mutex<Reported>>,
    _watch: EventSubscription,
}

async fn rig(spot_disarm_fails: bool, hold_token: bool) -> CloseRig {
    rig_with(spot_disarm_fails, hold_token, 2048).await
}

async fn rig_with(spot_disarm_fails: bool, hold_token: bool, caller_capacity: u32) -> CloseRig {
    let rest = Arc::new(ScriptedRest {
        disarm_fails: spot_disarm_fails,
        hold_token: AtomicBool::new(hold_token),
        posted: Mutex::default(),
    });
    let mut client = {
        let _g = crate::build::test_env::env_lock();
        Client::builder()
            .with_transport(Arc::clone(&rest) as Arc<dyn HttpTransport>)
            .with_api_key(ApiKey::new("test-key"), BASE64.encode(vec![0x01u8; 32]))
            .with_knob("close_timeout_ms", KnobValue::U32(200))
            .with_knob("caller_to_io_capacity", KnobValue::U32(caller_capacity))
            .build()
            .expect("valid credentials and default config")
    };
    let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&client.bus)));
    client.ws_factory = Arc::clone(&factory) as Arc<dyn WsSocketFactoryLike>;
    let failures = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&failures);
    let watch = client
        .events()
        .on(EventType::DeadmanDisarmFailedEvent, move |env| {
            if let EventPayload::DeadmanDisarmFailedEvent { url, cause, .. } = &env.payload {
                sink.lock().expect("failures lock").push((*url, *cause));
            }
        })
        .expect("subscribe to the disarm failure event");
    tokio::time::timeout(BUDGET, client.ready())
        .await
        .expect("timeout: the reactor never became ready")
        .expect("reactor ready");
    CloseRig {
        client: Some(client),
        factory,
        rest,
        failures,
        _watch: watch,
    }
}

impl CloseRig {
    fn client(&self) -> &Client {
        self.client.as_ref().expect("the client is not closed yet")
    }

    fn close(&mut self) -> super::Completion<super::CloseError> {
        self.client
            .take()
            .expect("the client is not closed yet")
            .close()
    }
}

async fn eventually<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(found) = probe() {
                return found;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: {what}"))
}

async fn drained(closed: super::Completion<super::CloseError>) {
    tokio::time::timeout(BUDGET, closed)
        .await
        .expect("timeout: the close never drained")
        .expect("a drained close");
}

async fn reported(rig: &CloseRig, at_least: usize) -> Reported {
    if at_least > 0 {
        eventually("the disarm failure was never reported", || {
            (rig.failures.lock().expect("failures lock").len() >= at_least).then_some(())
        })
        .await;
    }
    for _ in 0..SETTLE_POLLS {
        tokio::task::yield_now().await;
    }
    rig.failures.lock().expect("failures lock").clone()
}

fn deadman_frames(sock: &DriveableSocketHandle, timeout: u64) -> Vec<Value> {
    sock.sent_text()
        .into_iter()
        .filter_map(|body| {
            let frame: Value = serde_json::from_str(&body).ok()?;
            (frame["method"] == "cancel_all_orders_after" && frame["params"]["timeout"] == timeout)
                .then_some(frame)
        })
        .collect()
}

fn deadman_frame(sock: &DriveableSocketHandle, timeout: u64) -> Option<Value> {
    deadman_frames(sock, timeout).into_iter().next()
}

fn refusal(req_id: &Value) -> String {
    json!({
        "error": "Rate limit exceeded",
        "method": "cancel_all_orders_after",
        "req_id": req_id,
        "success": false,
    })
    .to_string()
}

fn confirmation_with(req_id: &Value, trigger_time: &str) -> String {
    json!({
        "method": "cancel_all_orders_after",
        "req_id": req_id,
        "result": { "currentTime": "2026-01-01T00:00:00Z", "triggerTime": trigger_time },
        "success": true,
    })
    .to_string()
}

fn confirmation(req_id: &Value) -> String {
    confirmation_with(req_id, "0")
}

fn request_arm(rig: &CloseRig) {
    let arm = rig.client().trade().futures().cancel_all_orders_after(60);
    tokio::spawn(async move {
        let _ = arm.await;
    });
}

async fn arm_futures_deadman(rig: &CloseRig) -> DriveableSocketHandle {
    request_arm(rig);
    let sock = eventually("the arm never opened the private socket", || {
        rig.factory.handle_for(WsUrl::UnifiedPrivate)
    })
    .await;
    eventually("the arm frame was never sent", || deadman_frame(&sock, 60)).await;
    sock
}

fn state(rig: &CloseRig) -> ConnectionState {
    rig.client().supervisor.current_state(WsUrl::UnifiedPrivate)
}

async fn wait_for_state(rig: &CloseRig, target: ConnectionState, what: &str) {
    eventually(what, || (state(rig) == target).then_some(())).await;
}

async fn arm_spot_deadman(rig: &CloseRig, timeout: u32) {
    tokio::time::timeout(
        BUDGET,
        rig.client()
            .trade()
            .cancel_all_orders_after(timeout)
            .via(Transport::Rest),
    )
    .await
    .expect("timeout: the Spot dead-man call never resolved")
    .expect("the Spot dead-man call succeeds");
}

fn spot_deadman_posts(rest: &ScriptedRest, timeout: u32) -> usize {
    let param = format!("timeout={timeout}");
    rest.posted
        .lock()
        .expect("posted lock")
        .iter()
        .filter(|(path, body)| {
            path == "/0/private/CancelAllOrdersAfter" && body.split('&').any(|p| p == param)
        })
        .count()
}

#[tokio::test]
async fn close_disarms_an_armed_futures_switch_and_an_armed_spot_switch() {
    let mut rig = rig(false, false).await;
    let sock = arm_futures_deadman(&rig).await;
    arm_spot_deadman(&rig, 60).await;

    let closed = rig.close();

    let frame = eventually("close never wrote the Futures disarm", || {
        deadman_frame(&sock, 0)
    })
    .await;
    assert_eq!(frame["params"]["asset_class"], "derivatives");
    eventually("close never posted the Spot disarm", || {
        (spot_deadman_posts(&rig.rest, 0) > 0).then_some(())
    })
    .await;
    drained(closed).await;
    assert_eq!(reported(&rig, 0).await, Reported::new());
    assert_eq!(spot_deadman_posts(&rig.rest, 0), 1);
}

async fn close_with_free_caller_slots(
    free: u32,
) -> (
    CloseRig,
    DriveableSocketHandle,
    super::Completion<super::CloseError>,
) {
    let mut rig = rig_with(false, false, 4).await;
    let sock = arm_futures_deadman(&rig).await;
    for req_id in 0..u64::from(4 - free) {
        rig.client()
            .bus
            .try_post_caller_inbound(crate::dispatch::CallerInbound::AbandonWsRequest { req_id })
            .expect("fill the caller queue");
    }
    let closed = rig.close();
    (rig, sock, closed)
}

#[tokio::test]
async fn close_keeps_the_last_caller_slot_for_its_marker_and_reports_the_unsent_disarm() {
    let (rig, sock, closed) = close_with_free_caller_slots(1).await;

    assert_eq!(
        reported(&rig, 1).await,
        vec![(WsUrl::UnifiedPrivate, DeadmanDisarmCause::NetworkError)]
    );
    drained(closed).await;
    assert!(deadman_frame(&sock, 0).is_none());
}

#[tokio::test]
async fn close_sends_the_futures_disarm_and_its_marker_when_two_caller_slots_are_free() {
    let (rig, sock, closed) = close_with_free_caller_slots(2).await;

    eventually("close never wrote the Futures disarm", || {
        deadman_frame(&sock, 0)
    })
    .await;
    drained(closed).await;
    assert_eq!(reported(&rig, 0).await, Reported::new());
}

#[tokio::test]
async fn close_sends_no_disarm_when_nothing_was_armed() {
    let mut rig = rig(true, false).await;

    let closed = rig.close();
    drained(closed).await;

    assert_eq!(
        rig.factory.created_count(),
        0,
        "an unarmed close must not open any socket"
    );
    assert_eq!(
        reported(&rig, 0).await,
        Reported::new(),
        "an unarmed close sends no disarm, so none can fail"
    );
    assert_eq!(spot_deadman_posts(&rig.rest, 0), 0);
}

#[tokio::test]
async fn close_sends_no_spot_disarm_once_the_client_disarmed_its_switch() {
    let mut rig = rig(false, false).await;
    arm_spot_deadman(&rig, 60).await;
    arm_spot_deadman(&rig, 0).await;

    let closed = rig.close();
    drained(closed).await;

    assert_eq!(reported(&rig, 0).await, Reported::new());
    assert_eq!(
        spot_deadman_posts(&rig.rest, 0),
        1,
        "only the caller's own disarm was posted"
    );
}

#[tokio::test]
async fn a_late_disarm_reply_does_not_clear_an_arm_requested_after_it() {
    let mut rig = rig(false, false).await;
    let sock = arm_futures_deadman(&rig).await;
    let first_arm = deadman_frame(&sock, 60).expect("the first arm frame");
    sock.emit_text(confirmation_with(
        &first_arm["req_id"],
        "2026-01-01T00:01:00Z",
    ));
    wait_for_state(
        &rig,
        ConnectionState::Open,
        "the first arm never opened the private socket",
    )
    .await;
    let disarm = rig.client().trade().futures().cancel_all_orders_after(0);
    let disarm = tokio::spawn(async move { disarm.await });
    let disarm_frame = eventually("the caller's disarm frame was never sent", || {
        deadman_frame(&sock, 0)
    })
    .await;
    request_arm(&rig);
    let second_arm = eventually("the second arm frame was never sent", || {
        deadman_frames(&sock, 60).get(1).cloned()
    })
    .await;

    sock.emit_text(confirmation_with(
        &second_arm["req_id"],
        "2026-01-01T00:02:00Z",
    ));
    sock.emit_text(confirmation(&disarm_frame["req_id"]));
    disarm
        .await
        .expect("the disarm task finished")
        .expect("the caller's disarm is confirmed");
    let closed = rig.close();
    eventually("close never disarmed the newer arm", || {
        (deadman_frames(&sock, 0).len() == 2).then_some(())
    })
    .await;
    drained(closed).await;
}

#[tokio::test]
async fn a_late_arm_reply_re_arms_after_a_disarm_that_overtook_it() {
    let mut rig = rig(false, false).await;
    let sock = arm_futures_deadman(&rig).await;
    let arm = deadman_frame(&sock, 60).expect("the arm frame");
    let disarm = rig.client().trade().futures().cancel_all_orders_after(0);
    let disarm = tokio::spawn(async move { disarm.await });
    let disarm_frame = eventually("the caller's disarm frame was never sent", || {
        deadman_frame(&sock, 0)
    })
    .await;

    sock.emit_text(confirmation(&disarm_frame["req_id"]));
    disarm
        .await
        .expect("the disarm task finished")
        .expect("the caller's disarm is confirmed");
    sock.emit_text(confirmation_with(&arm["req_id"], "2026-01-01T00:01:00Z"));
    for _ in 0..SETTLE_POLLS {
        tokio::task::yield_now().await;
    }
    let closed = rig.close();
    eventually("close never disarmed the late-confirmed arm", || {
        (deadman_frames(&sock, 0).len() == 2).then_some(())
    })
    .await;
    drained(closed).await;
}

#[tokio::test]
async fn a_spot_arm_requested_after_the_close_began_is_refused_unsent() {
    use crate::error::ApiError;
    for transport in [Transport::Rest, Transport::WsV2Auth] {
        let mut rig = rig(false, false).await;
        let arm = rig
            .client()
            .trade()
            .cancel_all_orders_after(60)
            .via(transport);

        let closed = rig.close();
        let error = tokio::time::timeout(BUDGET, arm)
            .await
            .expect("timeout: the arm never resolved")
            .expect_err("an arm after the close began is refused");
        drained(closed).await;

        assert_eq!(error.code(), "CLIENT_CLOSED", "{transport:?}");
        assert_eq!(spot_deadman_posts(&rig.rest, 60), 0, "{transport:?}");
        assert!(
            rig.factory.handle_for(WsUrl::Auth).is_none(),
            "{transport:?}: nothing was sent"
        );
    }
}

#[tokio::test]
async fn a_failed_spot_disarm_is_reported_on_the_auth_connection() {
    let mut rig = rig(true, false).await;
    arm_spot_deadman(&rig, 60).await;

    let closed = rig.close();

    assert_eq!(
        reported(&rig, 1).await,
        [(WsUrl::Auth, DeadmanDisarmCause::Timeout)]
    );
    drained(closed).await;
}

#[tokio::test]
async fn a_futures_disarm_on_a_backing_off_socket_is_reported_on_the_unified_private_connection() {
    let mut rig = rig(false, false).await;
    let sock = arm_futures_deadman(&rig).await;
    rig.factory.set_next_upgrade_failure(TransportError {
        kind: TransportErrorKind::TcpRefused,
        transient: true,
    });
    sock.drop_with(TransportError {
        kind: TransportErrorKind::SocketReset,
        transient: true,
    });
    wait_for_state(
        &rig,
        ConnectionState::BackingOff,
        "the private socket never settled in backoff",
    )
    .await;

    let closed = rig.close();

    assert_eq!(
        reported(&rig, 1).await,
        [(WsUrl::UnifiedPrivate, DeadmanDisarmCause::NetworkError)]
    );
    drained(closed).await;
}

#[tokio::test]
async fn a_futures_disarm_before_the_socket_has_a_token_cannot_be_sent_and_is_reported() {
    let mut rig = rig(false, true).await;
    request_arm(&rig);
    let sock = eventually("the arm never opened the private socket", || {
        rig.factory.handle_for(WsUrl::UnifiedPrivate)
    })
    .await;
    wait_for_state(
        &rig,
        ConnectionState::Authenticating,
        "the private socket never reached authenticating",
    )
    .await;

    let closed = rig.close();

    assert_eq!(
        reported(&rig, 1).await,
        [(WsUrl::UnifiedPrivate, DeadmanDisarmCause::NetworkError)]
    );
    assert!(
        sock.sent_text().is_empty(),
        "nothing reaches a socket that has no token"
    );
    drained(closed).await;
}

#[tokio::test]
async fn a_futures_disarm_with_no_cached_token_at_close_is_reported_unsent_not_as_session_expired()
{
    let mut rig = rig(false, false).await;
    let sock = arm_futures_deadman(&rig).await;
    let arm = deadman_frame(&sock, 60).expect("the arm frame");
    sock.emit_text(confirmation_with(&arm["req_id"], "2026-01-01T00:01:00Z"));
    wait_for_state(
        &rig,
        ConnectionState::Open,
        "the arm ack never opened the private socket",
    )
    .await;
    rig.client()
        .auth
        .token_lifecycle()
        .clear_cached_token_for_test();

    let closed = rig.close();

    assert_eq!(
        reported(&rig, 1).await,
        [(WsUrl::UnifiedPrivate, DeadmanDisarmCause::NetworkError)]
    );
    assert!(
        deadman_frame(&sock, 0).is_none(),
        "no disarm reaches the socket without a token"
    );
    drained(closed).await;
}

#[tokio::test]
async fn what_close_reports_of_a_futures_disarm_depends_on_the_connection_and_the_reply() {
    type Reply = Option<fn(&Value) -> String>;
    let rate_limited = vec![(WsUrl::UnifiedPrivate, DeadmanDisarmCause::RateLimitExceeded)];
    // (label, connection at close, venue reply to the disarm, reported)
    let cases: [(&str, ConnectionState, Reply, Reported); 6] = [
        (
            "authenticating, refused",
            ConnectionState::Authenticating,
            Some(refusal),
            rate_limited,
        ),
        (
            "authenticating, confirmed",
            ConnectionState::Authenticating,
            Some(confirmation),
            vec![],
        ),
        (
            "authenticating, unanswered",
            ConnectionState::Authenticating,
            None,
            vec![],
        ),
        (
            "open, refused",
            ConnectionState::Open,
            Some(refusal),
            vec![],
        ),
        (
            "open, confirmed",
            ConnectionState::Open,
            Some(confirmation),
            vec![],
        ),
        ("open, unanswered", ConnectionState::Open, None, vec![]),
    ];
    for (label, connection, reply, expected) in cases {
        let mut rig = rig(false, false).await;
        let sock = arm_futures_deadman(&rig).await;
        if connection == ConnectionState::Open {
            let arm = deadman_frame(&sock, 60).expect("the arm frame");
            sock.emit_text(confirmation_with(&arm["req_id"], "2026-01-01T00:01:00Z"));
        }
        wait_for_state(
            &rig,
            connection,
            "the connection never reached the wanted state",
        )
        .await;

        let closed = rig.close();
        let frame = eventually("close never wrote the Futures disarm", || {
            deadman_frame(&sock, 0)
        })
        .await;
        if let Some(reply) = reply {
            sock.emit_text(reply(&frame["req_id"]));
        }
        drained(closed).await;

        assert_eq!(reported(&rig, expected.len()).await, expected, "{label}");
    }
}
