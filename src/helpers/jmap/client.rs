//! A thin wrapper around [`jmap_client::Client`] handling connection setup,
//! account selection, and retries with exponential backoff.

use std::future::Future;
use std::time::Duration;

use tracing_batteries::prelude::*;

use crate::errors::HumanizableError;

/// The maximum number of attempts for a transient-failing request.
const MAX_ATTEMPTS: u32 = 5;

pub struct MailClient {
    client: jmap_client::client::Client,
    max_objects_in_get: usize,
}

impl MailClient {
    /// Connects to a JMAP session resource using a bearer token, optionally
    /// selecting a specific account by id or name (e-mail address).
    pub async fn connect(
        session_url: &str,
        token: &str,
        account: Option<&str>,
    ) -> Result<Self, human_errors::Error> {
        let mut builder = jmap_client::client::Client::new()
            .credentials(jmap_client::client::Credentials::bearer(token));

        // The client only follows redirects to explicitly trusted hosts, and
        // providers commonly redirect /.well-known/jmap to their session
        // resource (Fastmail does), so the server's own host must be trusted.
        if let Some(host) = url::Url::parse(session_url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
        {
            builder = builder.follow_redirects([host]);
        }

        let client = builder
            .connect(session_url)
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
        };

        client.select_account(account)?;
        Ok(client)
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

    /// The server's current Email state string (an Email/get with no ids).
    pub async fn email_state(&self) -> Result<String, human_errors::Error> {
        retry("Fetching the mail state", || async {
            let mut request = self.client.build();
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
        retry("Fetching the mailbox state", || async {
            let mut request = self.client.build();
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

pub fn is_cannot_calculate_changes(error: &jmap_client::Error) -> bool {
    matches!(
        error,
        jmap_client::Error::Method(e) if matches!(
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

pub fn is_anchor_not_found(error: &jmap_client::Error) -> bool {
    matches!(
        error,
        jmap_client::Error::Method(e) if matches!(
            e.error(),
            jmap_client::core::error::MethodErrorType::AnchorNotFound
        )
    )
}

/// Runs an operation, retrying transient failures with exponential backoff
/// and jitterless doubling (0.5s, 1s, 2s, 4s).
pub async fn retry<T, F, Fut>(description: &str, mut operation: F) -> Result<T, jmap_client::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, jmap_client::Error>>,
{
    let mut delay = Duration::from_millis(500);
    let mut attempt = 1;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < MAX_ATTEMPTS && is_transient(&error) => {
                warn!(
                    "{} failed (attempt {}/{}): {}; retrying in {:?}",
                    description, attempt, MAX_ATTEMPTS, error, delay
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}
