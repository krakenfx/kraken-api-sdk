//! REST methods for [`MarketNamespace`] — Spot public market-data endpoints.

use serde_json::Value;

use crate::api::WireRequest;
use crate::dispatch::dispatch_table::Op;
use crate::types::Symbol;

use super::requests::{OhlcRequest, TradesRequest};
use super::types::{
    AssetMeta, AssetPairMeta, AssetPairs, Assets, OhlcCandle, OhlcResult, OrderBookSnapshot,
    RawCandle, RawOrderBook, RawServerTime, RawSpread, RawSystemStatus, RawTrade, RecentTrade,
    ServerTime, SpreadEntry, SpreadsResult, SystemStatus, Ticker, TickerResult, TradesResult,
};
use super::{MarketError, MarketNamespace};

impl MarketNamespace {
    /// Snapshot tickers for `pairs`, or every pair when `None`, via `GET /0/public/Ticker`;
    /// keyed by the modern pair string (see [`TickerResult::get`]). The all-pairs snapshot
    /// arrives keyed by legacy wire codes, so the first such call also fetches (and caches) `AssetPairs` to re-key.
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken rejected a queried pair as unknown; only produced when `pairs` is given.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter.
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or ticker map failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn ticker(&self, pairs: Option<&[Symbol]>) -> Result<TickerResult, MarketError> {
        // `csv` doubles as symbol context for SymbolNotFound; `None` queries all pairs.
        let csv: Option<String> = pairs.map(symbols_csv);
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(ref c) = csv {
            query.push(("pair", c.as_str()));
        }
        let path = self.endpoint(Op::MarketTicker)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                match csv {
                    Some(ref c) => MarketError::from_rest_for_symbol(e, c),
                    None => MarketError::from(e),
                }
                .with_request_id(&request_id)
            })?;

        let mut tickers =
            crate::api::decode_object_map(result, "ticker", MarketError::malformed, |key, r| {
                Ok((key, Ticker::from_raw(r)?))
            })
            .map_err(|e| e.with_request_id(&request_id))?;

        if pairs.is_none() {
            // First-stamp-wins: the nested fetch's own id survives; this call's id
            // fills only pre-mint failures so no id-less error escapes ticker().
            let map = self
                .pair_key_map()
                .await
                .map_err(|e| e.with_request_id(&request_id))?;
            tickers = rekey_ticker_map(tickers, map);
        }
        Ok(TickerResult { tickers })
    }

    /// Wire-result-key → modern `Symbol` index, lazily built from `AssetPairs`
    /// and cached for the client lifetime.
    async fn pair_key_map(
        &self,
    ) -> Result<&std::collections::HashMap<String, Symbol>, MarketError> {
        self.pairs_key_map
            .get_or_try_init(|| async {
                let path = self.endpoint(Op::MarketAssetPairs)?;
                let request_id = crate::rest::mint_request_id();
                let result = self
                    .rest
                    .public_get(
                        path,
                        &[],
                        crate::rest::RetryPolicy::idempotent(),
                        &request_id,
                    )
                    .await
                    .map_err(|e| MarketError::from(e).with_request_id(&request_id))?;

                let raw: std::collections::HashMap<String, super::types::RawAssetPair> =
                    serde_json::from_value(result).map_err(|e| {
                        MarketError::malformed(format!("pairs decode: {e}"))
                            .with_request_id(&request_id)
                    })?;
                // A row whose rebuilt key fails Symbol validation is skipped.
                let mut map = std::collections::HashMap::with_capacity(raw.len());
                for (wire_key, r) in raw {
                    if let Ok(sym) = Symbol::new(r.modern_pair_key(wire_key.clone())) {
                        map.insert(wire_key, sym);
                    }
                }
                Ok(map)
            })
            .await
    }

    /// Snapshot the current order book for a symbol.
    /// `GET /0/public/Depth`; `count` defaults to 100 (Kraken's documented default).
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken does not know `symbol`.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter (e.g. an out-of-range `count`).
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or order-book payload failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn orderbook(
        &self,
        symbol: &Symbol,
        count: Option<u32>,
    ) -> Result<OrderBookSnapshot, MarketError> {
        let count_str = count.unwrap_or(100).to_string();
        let path = self.endpoint(Op::MarketOrderBook)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &[("pair", symbol.as_str()), ("count", count_str.as_str())],
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                MarketError::from_rest_for_symbol(e, symbol.as_str()).with_request_id(&request_id)
            })?;

        let pair_value = take_first_entry(result).map_err(|e| e.with_request_id(&request_id))?;
        let raw: RawOrderBook = serde_json::from_value(pair_value).map_err(|e| {
            MarketError::malformed(format!("orderbook decode: {}", e)).with_request_id(&request_id)
        })?;

        OrderBookSnapshot::from_raw(raw).map_err(|e| e.with_request_id(&request_id))
    }

    /// Fetch recent trades for a pair via `GET /0/public/Trades`. Pagination and
    /// batch size come from [`TradesRequest`].
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken does not know `pair`.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter (e.g. a bad `since` cursor).
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope, trade rows, or `last` cursor failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn trades(&self, req: TradesRequest) -> Result<TradesResult, MarketError> {
        let pair = req.pair().clone();
        let params = req.wire_params();
        let query: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = self.endpoint(Op::MarketTrades)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                MarketError::from_rest_for_symbol(e, pair.as_str()).with_request_id(&request_id)
            })?;

        // `last` is a decimal STRING on the wire (nanoseconds) — pass through verbatim.
        let (obj, pair_value) =
            take_last_entry(result, "trades").map_err(|e| e.with_request_id(&request_id))?;

        let last = obj
            .get("last")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                MarketError::malformed("trades: missing `last`".into()).with_request_id(&request_id)
            })?
            .to_owned();

        let raw_trades: Vec<RawTrade> = serde_json::from_value(pair_value).map_err(|e| {
            MarketError::malformed(format!("trades decode: {}", e)).with_request_id(&request_id)
        })?;

        let trades = raw_trades
            .into_iter()
            .map(RecentTrade::from_raw)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.with_request_id(&request_id))?;

        Ok(TradesResult { pair, trades, last })
    }

    /// Fetch Kraken's server time via `GET /0/public/Time`.
    ///
    /// # Errors
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or time payload failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn server_time(&self) -> Result<ServerTime, MarketError> {
        let path = self.endpoint(Op::MarketServerTime)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &[],
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| MarketError::from(e).with_request_id(&request_id))?;

        let raw: RawServerTime = serde_json::from_value(result).map_err(|e| {
            MarketError::malformed(format!("server_time decode: {}", e))
                .with_request_id(&request_id)
        })?;

        Ok(ServerTime {
            unixtime: raw.unixtime,
            rfc1123: raw.rfc1123,
        })
    }

    /// Fetch asset metadata via `GET /0/public/Assets`; `None` returns all. Keys are
    /// normalised to modern form; the legacy short form remains on `AssetMeta.altname`.
    ///
    /// # Errors
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter.
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or asset map failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings degrade here, including a rejected asset code (no `SymbolNotFound` lift on this path).
    pub async fn assets(&self, assets: Option<&[&str]>) -> Result<Assets, MarketError> {
        let csv;
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(list) = assets {
            csv = list.join(",");
            query.push(("asset", csv.as_str()));
        }
        let path = self.endpoint(Op::MarketAssets)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| MarketError::from(e).with_request_id(&request_id))?;

        let assets =
            crate::api::decode_object_map(result, "assets", MarketError::malformed, |code, r| {
                Ok((
                    crate::types::AssetCode::from_wire(&code),
                    AssetMeta::from_raw(r)?,
                ))
            })
            .map_err(|e| e.with_request_id(&request_id))?;
        Ok(Assets { assets })
    }

    /// Fetch tradeable-pair metadata via `GET /0/public/AssetPairs`. Kraken's legacy
    /// fields (`altname`/`wsname`/`base`/`quote`) are stripped at decode.
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken rejected a queried pair as unknown; only produced when `pairs` is given.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter.
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or pair-metadata map failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn pairs(&self, pairs: Option<&[Symbol]>) -> Result<AssetPairs, MarketError> {
        // `csv` doubles as symbol context for SymbolNotFound; `None` queries all pairs.
        let csv: Option<String> = pairs.map(symbols_csv);
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(ref c) = csv {
            query.push(("pair", c.as_str()));
        }
        let path = self.endpoint(Op::MarketAssetPairs)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                match csv {
                    Some(ref c) => MarketError::from_rest_for_symbol(e, c),
                    None => MarketError::from(e),
                }
                .with_request_id(&request_id)
            })?;

        let pairs = crate::api::decode_object_map(
            result,
            "pairs",
            MarketError::malformed,
            |key, r: super::types::RawAssetPair| {
                let key = r.modern_pair_key(key);
                Ok((key, AssetPairMeta::from_raw(r)?))
            },
        )
        .map_err(|e| e.with_request_id(&request_id))?;
        Ok(AssetPairs { pairs })
    }

    /// Fetch OHLCV candles via `GET /0/public/OHLC`. Build with
    /// [`OhlcRequest::new`]; optional `.since(secs)` paginates from a prior
    /// response's `last` (`None` → most recent ~720 candles).
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken does not know the request pair.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter (e.g. a bad `since`).
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope, candle rows, or `last` cursor failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn ohlc(&self, req: OhlcRequest) -> Result<OhlcResult, MarketError> {
        let since_str;
        let pair = req.pair().clone();
        let mut query: Vec<(&str, &str)> = vec![
            ("pair", pair.as_str()),
            ("interval", <&str>::from(req.interval())),
        ];
        if let Some(s) = req.since_opt() {
            since_str = s.to_string();
            query.push(("since", since_str.as_str()));
        }
        let path = self.endpoint(Op::MarketOhlc)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                MarketError::from_rest_for_symbol(e, pair.as_str()).with_request_id(&request_id)
            })?;

        let (obj, pair_value) =
            take_last_entry(result, "ohlc").map_err(|e| e.with_request_id(&request_id))?;
        let last = obj.get("last").and_then(Value::as_u64).ok_or_else(|| {
            MarketError::malformed("ohlc: missing `last`".into()).with_request_id(&request_id)
        })?;

        let raw_candles: Vec<RawCandle> = serde_json::from_value(pair_value).map_err(|e| {
            MarketError::malformed(format!("ohlc decode: {}", e)).with_request_id(&request_id)
        })?;
        let candles = raw_candles
            .into_iter()
            .map(OhlcCandle::from_raw)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.with_request_id(&request_id))?;
        Ok(OhlcResult {
            pair,
            candles,
            last,
        })
    }

    /// Fetch recent best-bid/best-ask spreads via `GET /0/public/Spread`. `since` is
    /// the pagination cursor (a prior response's `last`); `None` for the latest ~5000.
    ///
    /// # Errors
    /// - [`MarketError::SymbolNotFound`] — Kraken does not know `pair`.
    /// - [`MarketError::InvalidArguments`] — the exchange rejected a request parameter (e.g. a bad `since` cursor).
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope, spread rows, or `last` cursor failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn spreads(
        &self,
        pair: Symbol,
        since: Option<String>,
    ) -> Result<SpreadsResult, MarketError> {
        let mut query: Vec<(&str, &str)> = vec![("pair", pair.as_str())];
        if let Some(ref s) = since {
            query.push(("since", s.as_str()));
        }
        let path = self.endpoint(Op::MarketSpreads)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &query,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| {
                MarketError::from_rest_for_symbol(e, pair.as_str()).with_request_id(&request_id)
            })?;

        let (obj, pair_value) =
            take_last_entry(result, "spreads").map_err(|e| e.with_request_id(&request_id))?;
        // Wire `result.last` is a JSON integer — stringified for cross-binding cursor parity.
        let last = obj
            .get("last")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                MarketError::malformed("spreads: missing `last`".into())
                    .with_request_id(&request_id)
            })?
            .to_string();

        let raw: Vec<RawSpread> = serde_json::from_value(pair_value).map_err(|e| {
            MarketError::malformed(format!("spreads decode: {}", e)).with_request_id(&request_id)
        })?;
        let spreads = raw
            .into_iter()
            .map(SpreadEntry::from_raw)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.with_request_id(&request_id))?;
        Ok(SpreadsResult {
            pair,
            spreads,
            last,
        })
    }

    /// Fetch the exchange system status via `GET /0/public/SystemStatus`.
    ///
    /// # Errors
    /// - [`MarketError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`MarketError::Transport`] — network failure before a well-formed exchange response; retryable when transient.
    /// - [`MarketError::MalformedResponse`] — the envelope or status payload failed to decode.
    /// - [`MarketError::Unknown`] — catch-all: unmapped Kraken error strings (and internal dispatch misses) degrade here.
    pub async fn status(&self) -> Result<SystemStatus, MarketError> {
        let path = self.endpoint(Op::MarketSystemStatus)?;
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .public_get(
                path,
                &[],
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| MarketError::from(e).with_request_id(&request_id))?;

        let raw: RawSystemStatus = serde_json::from_value(result).map_err(|e| {
            MarketError::malformed(format!("system status decode: {}", e))
                .with_request_id(&request_id)
        })?;

        Ok(SystemStatus {
            status: raw.status,
            timestamp: raw.timestamp,
        })
    }
}

fn symbols_csv(symbols: &[Symbol]) -> String {
    let mut csv = String::new();
    for (i, symbol) in symbols.iter().enumerate() {
        if i > 0 {
            csv.push(',');
        }
        csv.push_str(symbol.as_str());
    }
    csv
}

/// Re-key an all-pairs ticker map from legacy wire codes to modern `Symbol` form.
/// An unmapped key passes through verbatim; on a re-key collision the earlier row
/// wins deterministically (native rows, then wire-key order) with a warning.
fn rekey_ticker_map(
    tickers: std::collections::HashMap<String, Ticker>,
    map: &std::collections::HashMap<String, Symbol>,
) -> std::collections::HashMap<String, Ticker> {
    let mut out = std::collections::HashMap::with_capacity(tickers.len());
    let mut rekeyed: Vec<(String, String, Ticker)> = Vec::new();
    for (key, t) in tickers {
        match map.get(&key) {
            Some(sym) if sym.as_str() != key => rekeyed.push((key, sym.as_str().to_string(), t)),
            Some(_) => {
                out.insert(key, t);
            }
            None => {
                tracing::debug!(pair = %key, "ticker all-pairs key absent from AssetPairs index; passing through verbatim");
                out.insert(key, t);
            }
        }
    }
    rekeyed.sort_by(|a, b| a.0.cmp(&b.0));
    for (wire_key, new_key, t) in rekeyed {
        match out.entry(new_key) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(t);
            }
            std::collections::hash_map::Entry::Occupied(o) => {
                tracing::warn!(pair = %o.key(), dropped_wire_key = %wire_key, "ticker re-key collision; keeping the earlier row");
            }
        }
    }
    out
}

/// Split a Kraken `result` object holding one pair-data entry plus a `last` cursor.
fn take_last_entry(
    result: serde_json::Value,
    ctx: &str,
) -> Result<
    (
        serde_json::map::Map<String, serde_json::Value>,
        serde_json::Value,
    ),
    MarketError,
> {
    let Value::Object(obj) = result else {
        return Err(MarketError::malformed(format!(
            "{ctx}: result not an object"
        )));
    };
    let mut rest = serde_json::map::Map::new();
    let mut pair_value = None;
    for (k, v) in obj {
        if pair_value.is_none() && k != "last" {
            pair_value = Some(v);
        } else {
            rest.insert(k, v);
        }
    }
    let pair_value =
        pair_value.ok_or_else(|| MarketError::malformed(format!("{ctx}: missing pair entry")))?;
    Ok((rest, pair_value))
}

fn take_first_entry(result: Value) -> Result<Value, MarketError> {
    let Value::Object(obj) = result else {
        return Err(MarketError::malformed("result not an object".into()));
    };
    obj.into_iter()
        .next()
        .map(|(_k, v)| v)
        .ok_or_else(|| MarketError::malformed("empty `result` object".into()))
}
