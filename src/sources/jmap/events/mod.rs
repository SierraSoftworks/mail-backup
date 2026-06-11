//! Pluggable strategies for receiving change notifications from a JMAP
//! server.
//!
//! Three strategies exist — websocket push (RFC 8887), EventSource/SSE
//! (RFC 8620 §7.3), and state polling — tried in order of preference until
//! one produces a working subscription. Strategies the server does not
//! support are skipped outright; strategies which fail repeatedly are
//! deprioritized for a while so a flapping transport cannot starve the
//! ones below it. Polling is always supported, so a reachable server
//! always ends up with *some* notification stream.
//!
//! Streams here are hints, not the source of truth: every disconnect is
//! followed by a catch-up sync in the engine, and a periodic safety poll
//! covers missed events, so a strategy ending early is an inconvenience
//! rather than a correctness problem.

mod polling;
mod sse;
mod websocket;

pub use polling::PollingStrategy;
pub use sse::SseStrategy;
pub use websocket::WebSocketStrategy;

use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_stream::{Stream, StreamExt};
use tracing_batteries::prelude::*;

use crate::entities::mail::SourceNotification;
use crate::helpers::jmap::MailClient;

/// How long a strategy may take to establish its subscription before the
/// chain gives up on it and tries the next one.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// Streams which die sooner than this after subscribing count towards a
/// strategy's failure streak; streams which live longer clear it.
const HEALTHY_STREAM_MIN: Duration = Duration::from_secs(60);

/// How many consecutive failures a strategy may accumulate before it is
/// deprioritized for [`COOLDOWN`].
const DEMOTION_THRESHOLD: u32 = 3;

/// How long a deprioritized strategy is skipped in favour of the ones
/// below it.
const COOLDOWN: Duration = Duration::from_secs(15 * 60);

/// The notification stream a strategy produces once subscribed.
pub type EventStream<'a> =
    Pin<Box<dyn Stream<Item = Result<SourceNotification, human_errors::Error>> + 'a>>;

/// A transport for receiving change notifications from a JMAP server.
pub trait EventStrategy {
    /// A short name for log output: "websocket", "sse", "polling".
    fn name(&self) -> &'static str;

    /// Whether the connected server supports this strategy. This is a
    /// cheap, local check against the session document; it must not issue
    /// any requests.
    fn supported(&self, client: &MailClient) -> bool;

    /// Establishes the subscription. An error here means "try the next
    /// strategy"; errors yielded by the returned stream instead end the
    /// streaming phase and bubble to the engine's reconnect loop.
    async fn subscribe<'a>(
        &'a self,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error>;
}

/// All event strategies, dispatched by enum (mirroring `stores::AnyStore`).
pub enum AnyEventStrategy {
    WebSocket(WebSocketStrategy),
    Sse(SseStrategy),
    Polling(PollingStrategy),
}

impl EventStrategy for AnyEventStrategy {
    fn name(&self) -> &'static str {
        match self {
            AnyEventStrategy::WebSocket(strategy) => strategy.name(),
            AnyEventStrategy::Sse(strategy) => strategy.name(),
            AnyEventStrategy::Polling(strategy) => strategy.name(),
        }
    }

    fn supported(&self, client: &MailClient) -> bool {
        match self {
            AnyEventStrategy::WebSocket(strategy) => strategy.supported(client),
            AnyEventStrategy::Sse(strategy) => strategy.supported(client),
            AnyEventStrategy::Polling(strategy) => strategy.supported(client),
        }
    }

    async fn subscribe<'a>(
        &'a self,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error> {
        match self {
            AnyEventStrategy::WebSocket(strategy) => strategy.subscribe(client).await,
            AnyEventStrategy::Sse(strategy) => strategy.subscribe(client).await,
            AnyEventStrategy::Polling(strategy) => strategy.subscribe(client).await,
        }
    }
}

/// Per-strategy failure tracking. The streak counts subscribe failures and
/// quick stream deaths alike; reaching [`DEMOTION_THRESHOLD`] puts the
/// strategy in cooldown.
#[derive(Default)]
struct StrategyHealth {
    consecutive_failures: u32,
    cooldown_until: Option<tokio::time::Instant>,
    subscribed_at: Option<tokio::time::Instant>,
}

/// Tries strategies in preference order on every (re)subscription, skipping
/// unsupported and recently-failing ones, and forwards the selected
/// stream's notifications.
pub struct StrategyChain<S = AnyEventStrategy> {
    strategies: Vec<S>,
    health: Mutex<Vec<StrategyHealth>>,
}

impl Default for StrategyChain {
    fn default() -> Self {
        Self::new(vec![
            AnyEventStrategy::WebSocket(WebSocketStrategy),
            AnyEventStrategy::Sse(SseStrategy),
            AnyEventStrategy::Polling(PollingStrategy::default()),
        ])
    }
}

impl<S: EventStrategy> StrategyChain<S> {
    pub fn new(strategies: Vec<S>) -> Self {
        let health = strategies.iter().map(|_| StrategyHealth::default()).collect();
        Self {
            strategies,
            health: Mutex::new(health),
        }
    }

    /// A long-lived stream of change notifications from the first strategy
    /// which produces a working subscription. The stream ends when the
    /// underlying connection drops (callers reconnect, re-entering the
    /// selection) or when `cancel` is set.
    pub fn events<'a>(
        &'a self,
        client: &'a MailClient,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<SourceNotification, human_errors::Error>> + 'a {
        async_stream::stream! {
            let mut selected = None;
            let mut last_error = None;
            let mut cooling_down = Vec::new();

            for (index, strategy) in self.strategies.iter().enumerate() {
                if cancelled(cancel) {
                    return;
                }
                if !strategy.supported(client) {
                    debug!(
                        "The {} event strategy is not supported by this mail server; skipping it.",
                        strategy.name()
                    );
                    continue;
                }
                if let Some(remaining) = self.cooldown_remaining(index) {
                    debug!(
                        "The {} event strategy is cooling down after repeated failures ({:?} remaining); skipping it.",
                        strategy.name(),
                        remaining
                    );
                    cooling_down.push(index);
                    continue;
                }
                match self.try_subscribe(index, client).await {
                    Ok(stream) => {
                        selected = Some((index, stream));
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }

            // Every remaining candidate is cooling down; rather than leaving
            // the daemon without notifications, retry them anyway in
            // preference order.
            if selected.is_none() {
                for index in cooling_down {
                    if cancelled(cancel) {
                        return;
                    }
                    match self.try_subscribe(index, client).await {
                        Ok(stream) => {
                            selected = Some((index, stream));
                            break;
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
            }

            let Some((index, mut stream)) = selected else {
                yield Err(last_error.unwrap_or_else(|| {
                    human_errors::system(
                        "The mail server does not support any of the available event notification strategies.",
                        &["This is a bug in mail-backup; please report it to us on GitHub."],
                    )
                }));
                return;
            };

            loop {
                let Some(item) = stream.next().await else {
                    // The subscription ended cleanly; account for its
                    // lifetime so a quickly-dying transport gets demoted.
                    self.stream_ended(index);
                    break;
                };

                if cancelled(cancel) {
                    // A shutdown is not a strategy failure.
                    return;
                }

                let failed = item.is_err();
                if failed {
                    // Recorded before the yield: the consumer may drop the
                    // stream without polling it again after an error.
                    self.stream_ended(index);
                }
                yield item;
                if failed {
                    break;
                }
            }
        }
    }

    /// Attempts a subscription with [`SUBSCRIBE_TIMEOUT`], recording the
    /// outcome in the strategy's health.
    async fn try_subscribe<'a>(
        &'a self,
        index: usize,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error> {
        let strategy = &self.strategies[index];
        let result = tokio::time::timeout(SUBSCRIBE_TIMEOUT, strategy.subscribe(client))
            .await
            .unwrap_or_else(|_| {
                Err(human_errors::system(
                    format!(
                        "Subscribing to {} change notifications timed out after {:?}.",
                        strategy.name(),
                        SUBSCRIBE_TIMEOUT
                    ),
                    &[
                        "Check that the mail server is reachable from this machine and that no firewall or proxy is dropping its connections.",
                    ],
                ))
            });

        match result {
            Ok(stream) => {
                info!(
                    strategy = strategy.name(),
                    "Subscribed to mailbox change notifications via {}.",
                    strategy.name()
                );
                self.health.lock().unwrap()[index].subscribed_at =
                    Some(tokio::time::Instant::now());
                Ok(stream)
            }
            Err(error) => {
                warn!(
                    "Subscribing to {} change notifications failed: {}",
                    strategy.name(),
                    error
                );
                self.record_failure(index);
                Err(error)
            }
        }
    }

    /// The time left before a deprioritized strategy becomes eligible
    /// again, if it is currently cooling down.
    fn cooldown_remaining(&self, index: usize) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        self.health.lock().unwrap()[index]
            .cooldown_until
            .filter(|&until| until > now)
            .map(|until| until - now)
    }

    fn record_failure(&self, index: usize) {
        let mut health = self.health.lock().unwrap();
        let entry = &mut health[index];
        entry.subscribed_at = None;
        Self::strike(entry, self.strategies[index].name());
    }

    /// Accounts for a subscription ending: long-lived streams clear the
    /// strategy's failure streak, short-lived ones extend it.
    fn stream_ended(&self, index: usize) {
        let mut health = self.health.lock().unwrap();
        let entry = &mut health[index];
        let Some(subscribed_at) = entry.subscribed_at.take() else {
            return;
        };
        if subscribed_at.elapsed() >= HEALTHY_STREAM_MIN {
            entry.consecutive_failures = 0;
            entry.cooldown_until = None;
        } else {
            Self::strike(entry, self.strategies[index].name());
        }
    }

    fn strike(entry: &mut StrategyHealth, name: &str) {
        entry.consecutive_failures += 1;
        if entry.consecutive_failures >= DEMOTION_THRESHOLD {
            entry.consecutive_failures = 0;
            entry.cooldown_until = Some(tokio::time::Instant::now() + COOLDOWN);
            warn!(
                "The {} event strategy failed {} times in a row; deprioritizing it for {:?}.",
                name, DEMOTION_THRESHOLD, COOLDOWN
            );
        }
    }
}

fn cancelled(cancel: &AtomicBool) -> bool {
    cancel.load(std::sync::atomic::Ordering::Relaxed)
}
