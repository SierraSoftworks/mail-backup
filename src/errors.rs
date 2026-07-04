use reqwest::StatusCode;

pub trait HumanizableError {
    fn to_human_error(self) -> human_errors::Error;
}

impl HumanizableError for reqwest::Error {
    fn to_human_error(self) -> human_errors::Error {
        if self.is_connect() {
            human_errors::wrap_user(
                self,
                "We could not connect to the remote server to make a web request.",
                &[
                    "Make sure that your internet connection is working correctly and the service is not blocked by your firewall.",
                ],
            )
        } else if self.is_decode() {
            human_errors::wrap_system(
                self,
                "We could not decode the response from the remote server.",
                &[
                    "This is likely due to a problem with the remote server, please try again later and report the problem to us on GitHub if the issue persists.",
                ],
            )
        } else if self.is_redirect() {
            human_errors::wrap_user(
                self,
                "We could not complete a web request to due to a redirect loop.",
                &[
                    "This is likely due to a problem with the remote server, please try again later and report the problem to us on GitHub if the issue persists.",
                ],
            )
        } else if self.is_timeout() {
            human_errors::wrap_system(
                self,
                "We timed out making a web request.",
                &[
                    "This is likely due to a problem with the remote server or your internet connection, please try again later and report the problem to us on GitHub if the issue persists.",
                ],
            )
        } else {
            human_errors::wrap_system(
                self,
                "An internal error occurred which we could not recover from.",
                &[
                    "Please read the error above and decide if there is something you can do to fix the problem, or report it to us on GitHub.",
                ],
            )
        }
    }
}

impl HumanizableError for reqwest::Response {
    fn to_human_error(self) -> human_errors::Error {
        match self.status() {
            StatusCode::NOT_FOUND => human_errors::user(
                "We received a 404 Not Found response when sending a web request.",
                &[
                    "Please check that you're using the correct options and try again. If the problem persists, please open an issue with us on GitHub.",
                ],
            ),
            StatusCode::UNAUTHORIZED => human_errors::user(
                "We received a 401 Unauthorized response when sending a web request.",
                &[
                    "This probably means that you have not configured your access tokens correctly, please check your configuration and try again.",
                ],
            ),
            StatusCode::FORBIDDEN => human_errors::user(
                "We received a 403 Forbidden response when sending a web request.",
                &[
                    "This probably means that you do not have permission to access this resource, please check that you do have permission and try again.",
                ],
            ),
            status => human_errors::wrap_system(
                ResponseError::from(self),
                format!(
                    "We received a {} status code when making a web request.",
                    status
                ),
                &[
                    "This is likely due to a problem with the remote server, please try again later and report the problem to us on GitHub if the issue persists.",
                ],
            ),
        }
    }
}

impl HumanizableError for jmap_client::Error {
    fn to_human_error(self) -> human_errors::Error {
        match &self {
            jmap_client::Error::Transport(_) => human_errors::wrap_user(
                self,
                "We could not communicate with the mail server.",
                &[
                    "Make sure that your internet connection is working correctly and that the configured session URL is reachable.",
                ],
            ),
            jmap_client::Error::Problem(problem) => match problem.status() {
                Some(401) | Some(403) => human_errors::wrap_user(
                    self,
                    "The mail server rejected our credentials.",
                    &[
                        "Make sure that the configured API token is valid and has not expired or been revoked.",
                    ],
                ),
                _ => human_errors::wrap_system(
                    self,
                    "The mail server reported a problem with our request.",
                    &[
                        "This is likely a temporary problem with the mail server; please try again later and report the problem to us on GitHub if it persists.",
                    ],
                ),
            },
            jmap_client::Error::Server(message)
                if message.starts_with("401") || message.starts_with("403") =>
            {
                human_errors::wrap_user(
                    self,
                    "The mail server rejected our credentials.",
                    &[
                        "Make sure that the configured API token is valid and has not expired or been revoked.",
                    ],
                )
            }
            jmap_client::Error::Parse(_) => human_errors::wrap_system(
                self,
                "We could not understand the mail server's response.",
                &[
                    "Make sure the configured URL points at a JMAP server (for Fastmail this is https://api.fastmail.com).",
                ],
            ),
            _ => human_errors::wrap_system(
                self,
                "An error occurred while talking to the mail server.",
                &[
                    "Please read the error above and decide if there is something you can do to fix the problem, or report it to us on GitHub.",
                ],
            ),
        }
    }
}

impl<E: HumanizableError> HumanizableError for crate::helpers::resilience::RetryError<E> {
    fn to_human_error(self) -> human_errors::Error {
        use crate::helpers::resilience::RetryError;
        match self {
            RetryError::Operation(error) => error.to_human_error(),
            RetryError::CircuitOpen { retry_after } => human_errors::system(
                format!(
                    "The remote server has been failing repeatedly, so requests to it are paused for the next {}s to give it a chance to recover.",
                    retry_after.as_secs().max(1)
                ),
                &[
                    "This usually indicates an outage or degradation on the remote server; the daemon will retry automatically, or you can re-run the command once the server has recovered.",
                ],
            ),
        }
    }
}

impl HumanizableError for filt_rs::Error {
    fn to_human_error(self) -> human_errors::Error {
        human_errors::wrap_user(
            self,
            "We could not understand the filter expression.",
            &[
                "Check that the filter in your configuration (or the --filter option) is a valid filter expression and try again.",
            ],
        )
    }
}

impl HumanizableError for reqwest::header::InvalidHeaderValue {
    fn to_human_error(self) -> human_errors::Error {
        human_errors::wrap_system(
            self,
            "Could not parse header value due to an invalid value.",
            &["Please check your configuration and try again."],
        )
    }
}

#[derive(Debug)]
pub struct ResponseError {
    pub status_code: StatusCode,
    pub body: Option<String>,
}

impl ResponseError {
    #[allow(dead_code)] // kept in parity with github-backup's error helpers
    pub async fn with_body(resp: reqwest::Response) -> Self {
        Self {
            status_code: resp.status(),
            body: resp.text().await.ok(),
        }
    }
}

impl From<reqwest::Response> for ResponseError {
    fn from(resp: reqwest::Response) -> Self {
        Self {
            status_code: resp.status(),
            body: None,
        }
    }
}

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(body) = self.body.clone() {
            write!(
                f,
                "HTTP {} {}\n{}",
                self.status_code.as_u16(),
                self.status_code.canonical_reason().unwrap_or_default(),
                body
            )
        } else {
            write!(
                f,
                "HTTP {} {}",
                self.status_code.as_u16(),
                self.status_code.canonical_reason().unwrap_or_default()
            )
        }
    }
}

impl std::error::Error for ResponseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_parse_error_is_humanized_as_user_error() {
        // An unterminated string literal is a lexer error, giving us a
        // `filt_rs::Error` to humanize.
        let err = crate::Filter::new("subject == \"unterminated")
            .expect_err("an unterminated string should fail to parse");

        let humanized = err.to_human_error();
        let rendered = format!("{}", human_errors::pretty(&humanized));
        assert!(
            rendered.contains("filter expression"),
            "unexpected rendering: {rendered}"
        );
    }
}
