//! WS streaming handler methods (`on_executions`, `on_balances`, and their
//! `_for` combiners) for [`AccountNamespace`].

use crate::api::subscription_guard::SubscriptionGuard;
use crate::api::subscription_types::SubscriptionError;
use crate::conn::SubscribeParams;
use crate::dispatch::HandlerHandle;

use crate::api::ws_decode::wrap_typed;
use crate::types::ChannelName;

use super::AccountNamespace;
use super::ws_types::{BalanceUpdate, ExecutionUpdate, decode_balances, decode_executions};

impl AccountNamespace {
    /// Register a streaming `executions` handler (all `exec_type` transitions);
    /// the returned `HandlerHandle` deregisters on `Drop`. Does not subscribe —
    /// call `client.subscription().subscribe_executions()`. Invoked once per entry.
    pub fn on_executions<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&ExecutionUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_executions);
        self.ws.register_handler(ChannelName::Executions, wrapped)
    }

    /// Register a streaming `balances` handler (snapshot on subscribe, then
    /// updates on change); the returned `HandlerHandle` deregisters on `Drop`.
    /// Does not subscribe — call `client.subscription().subscribe_balances()`.
    pub fn on_balances<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&BalanceUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_balances);
        self.ws.register_handler(ChannelName::Balances, wrapped)
    }

    /// Register an `executions` handler and subscribe the channel-wide `executions`
    /// stream in one call, returning a [`SubscriptionGuard`] whose `Drop` tears both
    /// down. Delivery is channel-wide (all `exec_type` transitions, no pair filter).
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_executions_for<F>(&self, cb: F) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&ExecutionUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_executions);
        self.ws.register_and_subscribe_guarded(
            ChannelName::Executions,
            Vec::new(),
            SubscribeParams::Executions,
            wrapped,
        )
    }

    /// Register a `balances` handler and subscribe the channel-wide `balances`
    /// stream in one call (snapshot on subscribe, then updates on change), returning
    /// a [`SubscriptionGuard`] whose `Drop` tears both down. Delivery is channel-wide.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_balances_for<F>(&self, cb: F) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&BalanceUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_balances);
        self.ws.register_and_subscribe_guarded(
            ChannelName::Balances,
            Vec::new(),
            SubscribeParams::Balances,
            wrapped,
        )
    }
}
