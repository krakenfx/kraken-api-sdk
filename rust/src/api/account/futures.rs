//! Unified Futures private account handlers.

use std::sync::Arc;

use crate::api::subscription_guard::SubscriptionGuard;
use crate::api::subscription_types::SubscriptionError;
use crate::api::ws_decode::wrap_typed;
use crate::api::ws_surface::WsSurface;
use crate::conn::SubscribeParams;
use crate::dispatch::HandlerHandle;
use crate::types::{ChannelName, WsUrl};

use super::futures_ws_types::{
    FuturesBalanceUpdate, FuturesExecutionUpdate, FuturesMarginUpdate, FuturesPositionsUpdate,
    FuturesPreferencesUpdate, decode_futures_balances, decode_futures_executions,
    decode_futures_margin, decode_futures_positions, decode_futures_preferences,
};

/// Unified Futures private account namespace, accessed via `client.account().futures()`.
pub struct FuturesAccountNamespace {
    ws: Arc<WsSurface>,
}

impl std::fmt::Debug for FuturesAccountNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuturesAccountNamespace")
            .finish_non_exhaustive()
    }
}

impl FuturesAccountNamespace {
    pub(crate) fn new(ws: Arc<WsSurface>) -> Self {
        Self { ws }
    }

    /// Register a Unified Futures executions handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_executions<F>(&self, callback: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesExecutionUpdate) + Send + Sync + 'static,
    {
        self.handler(
            ChannelName::Executions,
            wrap_typed(callback, decode_futures_executions),
        )
    }

    /// Register a Unified Futures positions handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_positions<F>(&self, callback: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesPositionsUpdate) + Send + Sync + 'static,
    {
        self.handler(
            ChannelName::Positions,
            wrap_typed(callback, decode_futures_positions),
        )
    }

    /// Register a Unified Futures margin handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_margin<F>(&self, callback: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesMarginUpdate) + Send + Sync + 'static,
    {
        self.handler(
            ChannelName::Margin,
            wrap_typed(callback, decode_futures_margin),
        )
    }

    /// Register a handler for the shared Unified private balances channel.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_balances<F>(&self, callback: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesBalanceUpdate) + Send + Sync + 'static,
    {
        self.handler(
            ChannelName::Balances,
            wrap_typed(callback, decode_futures_balances),
        )
    }

    /// Register a Unified Futures account-preferences handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_preferences<F>(&self, callback: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesPreferencesUpdate) + Send + Sync + 'static,
    {
        self.handler(
            ChannelName::Preferences,
            wrap_typed(callback, decode_futures_preferences),
        )
    }

    /// Register an executions handler and subscribe in one call.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_executions_for<F>(
        &self,
        snap_orders: Option<bool>,
        snap_trades: Option<bool>,
        callback: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesExecutionUpdate) + Send + Sync + 'static,
    {
        self.guarded(
            ChannelName::Executions,
            SubscribeParams::Executions {
                snap_orders,
                snap_trades,
            },
            wrap_typed(callback, decode_futures_executions),
        )
    }

    /// Register a positions handler and subscribe in one call.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_positions_for<F>(
        &self,
        snapshot: Option<bool>,
        callback: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesPositionsUpdate) + Send + Sync + 'static,
    {
        self.guarded(
            ChannelName::Positions,
            SubscribeParams::Positions { snapshot },
            wrap_typed(callback, decode_futures_positions),
        )
    }

    /// Register a margin handler and subscribe in one call.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_margin_for<F>(&self, callback: F) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesMarginUpdate) + Send + Sync + 'static,
    {
        self.guarded(
            ChannelName::Margin,
            SubscribeParams::Margin,
            wrap_typed(callback, decode_futures_margin),
        )
    }

    /// Register a balances handler and subscribe in one call.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_balances_for<F>(
        &self,
        snapshot: Option<bool>,
        callback: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesBalanceUpdate) + Send + Sync + 'static,
    {
        self.guarded(
            ChannelName::Balances,
            SubscribeParams::Balances { snapshot },
            wrap_typed(callback, decode_futures_balances),
        )
    }

    /// Register a preferences handler and subscribe in one call.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_preferences_for<F>(&self, callback: F) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesPreferencesUpdate) + Send + Sync + 'static,
    {
        self.guarded(
            ChannelName::Preferences,
            SubscribeParams::Preferences,
            wrap_typed(callback, decode_futures_preferences),
        )
    }

    fn handler(
        &self,
        channel: ChannelName,
        callback: crate::dispatch::HandlerCallback,
    ) -> Result<HandlerHandle, SubscriptionError> {
        self.ws
            .register_handler_for_checked(WsUrl::UnifiedPrivate, channel, callback)
    }

    fn guarded(
        &self,
        channel: ChannelName,
        params: SubscribeParams,
        callback: crate::dispatch::HandlerCallback,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        self.ws.register_and_subscribe_guarded_for(
            WsUrl::UnifiedPrivate,
            channel,
            Vec::new(),
            params,
            callback,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{CallerInbound, RegistryMutationOp};

    #[test]
    fn guarded_registrations_post_handler_and_registry_mutations_on_unified_private() {
        type Case = (
            ChannelName,
            SubscribeParams,
            fn(&FuturesAccountNamespace) -> Result<SubscriptionGuard, SubscriptionError>,
        );
        let cases: [Case; 5] = [
            (
                ChannelName::Executions,
                SubscribeParams::Executions {
                    snap_orders: Some(false),
                    snap_trades: Some(true),
                },
                |a| a.on_executions_for(Some(false), Some(true), |_| {}),
            ),
            (
                ChannelName::Positions,
                SubscribeParams::Positions {
                    snapshot: Some(false),
                },
                |a| a.on_positions_for(Some(false), |_| {}),
            ),
            (ChannelName::Margin, SubscribeParams::Margin, |a| {
                a.on_margin_for(|_| {})
            }),
            (
                ChannelName::Balances,
                SubscribeParams::Balances { snapshot: None },
                |a| a.on_balances_for(None, |_| {}),
            ),
            (
                ChannelName::Preferences,
                SubscribeParams::Preferences,
                |a| a.on_preferences_for(|_| {}),
            ),
        ];

        for (channel, params, register) in cases {
            let (bus, ws) = WsSurface::on_default_bus();
            let account = FuturesAccountNamespace::new(ws);
            let mut rx = bus.take_caller_to_io_rx().expect("receiver");

            let _guard = register(&account).expect("guarded registration");

            assert!(
                matches!(
                    rx.try_recv().expect("handler mutation"),
                    CallerInbound::HandlerMutation { url: WsUrl::UnifiedPrivate, channel: got, .. }
                        if got == channel
                ),
                "{channel:?}: handler registers on Unified private"
            );
            assert!(
                matches!(
                    rx.try_recv().expect("registry mutation"),
                    CallerInbound::RegistryMutation {
                        url: WsUrl::UnifiedPrivate,
                        mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                    } if entries.len() == 1
                        && entries[0].channel == channel
                        && entries[0].params == params
                ),
                "{channel:?}: subscription registers on Unified private with its params"
            );
        }
    }
}
