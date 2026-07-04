//! A thin wrapper around [`jmap_client::Client`] handling connection setup,
//! account selection, and resilience: every request runs through the shared
//! retry/circuit-breaker machinery in [`crate::helpers::resilience`].

use std::future::Future;

use crate::errors::HumanizableError;
use crate::helpers::resilience::{self, CircuitBreaker, RetryError, RetryPolicy};

/// The JMAP capabilities advertised in every request's `using` array.
///
/// jmap-client 0.4.2 began advertising eleven capabilities by default
/// (submission, vacationresponse, contacts, calendars, websocket, sieve,
/// blob, quota, principals on top of core and mail). RFC 8620 §3.3 requires
/// the server to recognise every capability named in `using`, and Fastmail
/// rejects any it does not support — but with a bare `400 Bad Request` rather
/// than the `urn:ietf:params:jmap:error:unknownCapability` problem document
/// the spec prescribes, so the failure surfaces only as an opaque
/// "Server failed: 400 Bad Request". Backup and restore touch nothing beyond
/// the core and mail capabilities, so we pin the set to those two — restoring
/// jmap-client 0.4.1's behaviour.
const USING: &[jmap_client::URI] = &[jmap_client::URI::Core, jmap_client::URI::Mail];

pub struct MailClient {
    client: jmap_client::client::Client,
    max_objects_in_get: usize,
    retry_policy: RetryPolicy,
    /// Shared by every request this client makes (API calls, blob
    /// downloads and uploads, the initial connect), so repeated transient
    /// failures anywhere pause all traffic to this server for a while
    /// rather than hammering it from many independent retry loops.
    breaker: CircuitBreaker,
}

impl MailClient {
    /// Connects to a JMAP session resource using a bearer token, optionally
    /// selecting a specific account by id or name (e-mail address).
    /// Transient connection failures (TCP resets, 5xx responses, rate
    /// limiting) are retried with backoff like any other request.
    pub async fn connect(
        session_url: &str,
        token: &str,
        account: Option<&str>,
    ) -> Result<Self, human_errors::Error> {
        let retry_policy = RetryPolicy::default();
        let breaker = CircuitBreaker::default();

        // The client only follows redirects to explicitly trusted hosts, and
        // providers commonly redirect /.well-known/jmap to their session
        // resource (Fastmail does), so the server's own host must be trusted.
        let trusted_host = url::Url::parse(session_url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string));

        let client = resilience::retry(
            &retry_policy,
            &breaker,
            "Connecting to the mail server",
            is_transient,
            || async {
                let mut builder = jmap_client::client::Client::new()
                    .credentials(jmap_client::client::Credentials::bearer(token));
                if let Some(host) = &trusted_host {
                    builder = builder.follow_redirects([host.clone()]);
                }
                builder.connect(session_url).await
            },
        )
        .await
        .map_err(|e| e.to_human_error())?;

        let mut client = Self {
            max_objects_in_get: client
                .session()
                .core_capabilities()
                .map(|c| c.max_objects_in_get())
                .unwrap_or(500)
                .clamp(10, 4096),
            client,
            retry_policy,
            breaker,
        };

        client.select_account(account)?;
        Ok(client)
    }

    /// Runs a request against this client's server, retrying transient
    /// failures with exponential backoff and coordinating with the client's
    /// shared circuit breaker (see [`crate::helpers::resilience`]).
    pub async fn retry<T, F, Fut>(
        &self,
        description: &str,
        operation: F,
    ) -> Result<T, RetryError<jmap_client::Error>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, jmap_client::Error>>,
    {
        resilience::retry(
            &self.retry_policy,
            &self.breaker,
            description,
            is_transient,
            operation,
        )
        .await
    }

    fn select_account(&mut self, account: Option<&str>) -> Result<(), human_errors::Error> {
        let session = self.client.session();

        match account {
            Some(wanted) => {
                let resolved = session
                    .accounts()
                    .find(|id| {
                        id.as_str() == wanted
                            || session
                                .account(id)
                                .is_some_and(|a| a.name().eq_ignore_ascii_case(wanted))
                    })
                    .cloned();

                match resolved {
                    Some(id) => {
                        self.client.set_default_account_id(id);
                        Ok(())
                    }
                    None => Err(human_errors::user(
                        format!("The account '{}' was not found on the mail server.", wanted),
                        &[
                            "Make sure the configured account matches one of the accounts your API token has access to, or remove the account setting to use the primary account.",
                        ],
                    )),
                }
            }
            None => {
                if self.client.default_account_id().is_empty() {
                    let primary = session
                        .primary_accounts()
                        .find(|(capability, _)| capability.as_str() == "urn:ietf:params:jmap:mail")
                        .map(|(_, id)| id.clone())
                        .or_else(|| session.accounts().next().cloned());

                    match primary {
                        Some(id) => {
                            self.client.set_default_account_id(id);
                        }
                        None => {
                            return Err(human_errors::user(
                                "The mail server did not report any accounts for this API token.",
                                &[
                                    "Make sure the API token has been granted access to at least one mail account.",
                                ],
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    pub fn inner(&self) -> &jmap_client::client::Client {
        &self.client
    }

    /// Builds a JMAP request whose `using` array is constrained to the
    /// capabilities we actually rely on (see [`USING`]).
    ///
    /// Always prefer this over `self.inner().build()`: the latter advertises
    /// every capability jmap-client knows about, which Fastmail rejects.
    pub fn build(&self) -> jmap_client::core::request::Request<'_> {
        let mut request = self.client.build();
        request.using = USING.to_vec();
        request
    }

    /// The server's current Email state string (an Email/get with no ids).
    pub async fn email_state(&self) -> Result<String, human_errors::Error> {
        self.retry("Fetching the mail state", || async {
            let mut request = self.build();
            request.get_email().ids(Vec::<String>::new());
            request
                .send_single::<jmap_client::core::response::EmailGetResponse>()
                .await
                .map(|mut r| r.take_state())
        })
        .await
        .map_err(|e| e.to_human_error())
    }

    /// The server's current Mailbox state string.
    pub async fn mailbox_state(&self) -> Result<String, human_errors::Error> {
        self.retry("Fetching the mailbox state", || async {
            let mut request = self.build();
            request.get_mailbox().ids(Vec::<String>::new());
            request
                .send_single::<jmap_client::core::response::MailboxGetResponse>()
                .await
                .map(|mut r| r.take_state())
        })
        .await
        .map_err(|e| e.to_human_error())
    }

    pub fn account_id(&self) -> &str {
        self.client.default_account_id()
    }

    pub fn max_objects_in_get(&self) -> usize {
        self.max_objects_in_get
    }
}

/// Whether an error is worth retrying: connection problems, timeouts, server
/// errors, and rate limiting.
fn is_transient(error: &jmap_client::Error) -> bool {
    match error {
        jmap_client::Error::Transport(e) => {
            e.is_connect()
                || e.is_timeout()
                || e.is_request()
                || e.status()
                    .is_some_and(|s| s.as_u16() == 429 || s.is_server_error())
        }
        jmap_client::Error::Problem(problem) => {
            problem.status().is_none_or(|s| s == 429 || s >= 500)
        }
        // Server errors carry "<status> <reason>" messages; only retry rate
        // limiting and server-side failures, never auth or client errors.
        jmap_client::Error::Server(message) => {
            message.starts_with("429") || message.starts_with('5')
        }
        jmap_client::Error::Method(e) => matches!(
            e.error(),
            jmap_client::core::error::MethodErrorType::ServerUnavailable
        ),
        _ => false,
    }
}

pub fn is_cannot_calculate_changes(error: &RetryError<jmap_client::Error>) -> bool {
    matches!(
        error.inner(),
        Some(jmap_client::Error::Method(e)) if matches!(
            e.error(),
            jmap_client::core::error::MethodErrorType::CannotCalculateChanges
        )
    )
}

/// Whether an event-stream error is jmap-client's keep-alive parsing
/// artifact rather than a real failure.
///
/// The SSE specification says events with an empty data buffer must be
/// ignored, but jmap-client's parser dispatches them (comment keep-alives
/// and blank lines produce one) and then fails to JSON-parse the zero-byte
/// payload — an EOF at the very first byte. Fastmail sends comment
/// keep-alives on its event stream, so these arrive regularly on a
/// perfectly healthy connection.
pub fn is_keepalive_artifact(error: &jmap_client::Error) -> bool {
    matches!(
        error,
        jmap_client::Error::Parse(e)
            if e.classify() == serde_json::error::Category::Eof
                && e.line() == 1
                && e.column() == 0
    )
}

pub fn is_anchor_not_found(error: &RetryError<jmap_client::Error>) -> bool {
    matches!(
        error.inner(),
        Some(jmap_client::Error::Method(e)) if matches!(
            e.error(),
            jmap_client::core::error::MethodErrorType::AnchorNotFound
        )
    )
}
