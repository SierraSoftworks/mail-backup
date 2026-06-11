//! Wire-level probe for the JMAP EventSource (SSE) push channel.
//!
//! Captures exactly what a JMAP server sends on its event-source endpoint —
//! independently of any client library — and analyses each event against the
//! Server-Sent Events specification
//! (https://html.spec.whatwg.org/multipage/server-sent-events.html). In
//! particular it reports, for every `state` event, whether the JSON payload
//! carries the `@type` member which RFC 8620 §7.1 defines on the StateChange
//! object.
//!
//! It can also consume the same endpoint through `jmap-client` (as the main
//! application does) so the library's interpretation can be compared with
//! the raw wire data.
//!
//! ```text
//! # Ground truth: dump and analyse the raw wire bytes for 60 seconds
//! cargo run --example sse_probe -- --token fmu1-... --mode raw
//!
//! # What jmap-client makes of the same endpoint
//! cargo run --example sse_probe -- --token fmu1-... --mode client
//!
//! # Both, sequentially; keep a byte-exact capture for evidence
//! cargo run --example sse_probe -- --token fmu1-... --mode both --capture sse-capture.bin
//! ```
//!
//! Send yourself an email while the probe runs to trigger a `state` event.

use std::time::Duration;

use clap::Parser;
use futures::StreamExt;

#[derive(Parser, Debug)]
#[command(about = "Wire-level probe for the JMAP EventSource (SSE) push channel.")]
struct Args {
    /// The JMAP server's base URL (the session resource is resolved at
    /// /.well-known/jmap).
    #[arg(long, default_value = "https://api.fastmail.com")]
    url: String,

    /// Bearer token; falls back to the JMAP_TOKEN environment variable.
    #[arg(long)]
    token: Option<String>,

    /// Comma-separated type names for the {types} template variable, or "*".
    #[arg(long, default_value = "Email,Mailbox")]
    types: String,

    /// Value for the {closeafter} template variable ("no" or "state").
    #[arg(long, default_value = "no")]
    closeafter: String,

    /// Value for the {ping} template variable, in seconds.
    #[arg(long, default_value_t = 30)]
    ping: u32,

    /// How long to listen, in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// raw: capture and analyse the wire bytes directly (ground truth);
    /// client: consume the endpoint through jmap-client; both: raw, then client.
    #[arg(long, default_value = "raw")]
    mode: String,

    /// Write the exact raw bytes received to this file (raw mode only).
    #[arg(long)]
    capture: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let token = args
        .token
        .clone()
        .or_else(|| std::env::var("JMAP_TOKEN").ok())
        .ok_or("provide a token via --token or the JMAP_TOKEN environment variable")?;

    println!(
        "Fetching the session resource from {}/.well-known/jmap ...",
        args.url
    );
    let http = reqwest::Client::new();
    let session: serde_json::Value = http
        .get(format!(
            "{}/.well-known/jmap",
            args.url.trim_end_matches('/')
        ))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let template = session["eventSourceUrl"]
        .as_str()
        .ok_or("the session resource has no eventSourceUrl")?;
    let event_source_url = template
        .replace("{types}", &args.types)
        .replace("{closeafter}", &args.closeafter)
        .replace("{ping}", &args.ping.to_string());

    println!("eventSourceUrl template: {template}");
    println!("Connecting to:           {event_source_url}");
    println!(
        "Listening for {}s — send yourself an email to trigger a 'state' event.",
        args.duration
    );
    println!();

    match args.mode.as_str() {
        "raw" => raw_mode(&args, &http, &token, &event_source_url).await?,
        "client" => client_mode(&args, &token).await?,
        "both" => {
            raw_mode(&args, &http, &token, &event_source_url).await?;
            println!();
            client_mode(&args, &token).await?;
        }
        other => return Err(format!("unknown --mode '{other}' (raw|client|both)").into()),
    }

    Ok(())
}

/// Streams the endpoint directly with reqwest, prints every chunk
/// byte-exactly (escaped), assembles events per the SSE specification, and
/// analyses each one.
async fn raw_mode(
    args: &Args,
    http: &reqwest::Client,
    token: &str,
    event_source_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RAW MODE: exact wire bytes, assembled per the SSE specification ===");

    let mut capture_file = match &args.capture {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };

    let response = http
        .get(event_source_url)
        .bearer_auth(token)
        .header("Accept", "text/event-stream")
        .send()
        .await?
        .error_for_status()?;
    println!(
        "HTTP {} — content-type: {}",
        response.status(),
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("(none)")
    );

    let mut stream = response.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.duration);
    let mut analyzer = SseAnalyzer::default();
    let mut chunk_no = 0usize;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n[probe] duration elapsed");
                break;
            }
            chunk = stream.next() => match chunk {
                Some(Ok(bytes)) => {
                    chunk_no += 1;
                    println!(
                        "--- chunk {chunk_no} ({} bytes): {:?}",
                        bytes.len(),
                        String::from_utf8_lossy(&bytes)
                    );
                    if let Some(file) = capture_file.as_mut() {
                        use std::io::Write;
                        file.write_all(&bytes)?;
                    }
                    analyzer.feed(&bytes);
                }
                Some(Err(e)) => {
                    println!("[probe] transport error: {e}");
                    break;
                }
                None => {
                    println!("[probe] the server closed the stream");
                    break;
                }
            }
        }
    }

    analyzer.summary();
    if let Some(path) = &args.capture {
        println!("Byte-exact capture written to {}", path.display());
    }
    Ok(())
}

/// Incremental SSE event-stream processor implementing the specification's
/// interpretation rules, with diagnostics relevant to JMAP push.
#[derive(Default)]
struct SseAnalyzer {
    line: Vec<u8>,
    pending_cr: bool,

    event_type: String,
    data: Vec<String>,
    last_id: String,

    crlf_lines: usize,
    lf_lines: usize,
    cr_lines: usize,
    comment_lines: usize,
    empty_dispatches: usize,
    dispatched_events: usize,
    multi_data_events: usize,
    state_with_type: usize,
    state_without_type: usize,
}

impl SseAnalyzer {
    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            // A CR terminates the line; whether it was CRLF or a bare CR is
            // only known once the next byte arrives (it may sit in the next
            // network chunk).
            if self.pending_cr {
                self.pending_cr = false;
                let line = std::mem::take(&mut self.line);
                if byte == b'\n' {
                    self.crlf_lines += 1;
                    self.process_line(&line);
                    continue;
                }
                self.cr_lines += 1;
                self.process_line(&line);
            }

            match byte {
                b'\r' => self.pending_cr = true,
                b'\n' => {
                    self.lf_lines += 1;
                    let line = std::mem::take(&mut self.line);
                    self.process_line(&line);
                }
                _ => self.line.push(byte),
            }
        }
    }

    fn process_line(&mut self, line: &[u8]) {
        if line.is_empty() {
            self.dispatch();
            return;
        }

        if line[0] == b':' {
            self.comment_lines += 1;
            println!("  [comment line] {:?}", String::from_utf8_lossy(line));
            return;
        }

        let (field, value) = match line.iter().position(|&b| b == b':') {
            Some(colon) => {
                let mut value = &line[colon + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (&line[..colon], value)
            }
            None => (line, &line[..0]),
        };

        let field = String::from_utf8_lossy(field).to_string();
        let value = String::from_utf8_lossy(value).to_string();
        match field.as_str() {
            "event" => self.event_type = value,
            "data" => self.data.push(value),
            "id" if !value.contains('\u{0}') => self.last_id = value,
            "id" | "retry" => {}
            other => println!("  [unknown field] {other:?} = {value:?}"),
        }
    }

    fn dispatch(&mut self) {
        let event_type = std::mem::take(&mut self.event_type);
        let data_lines = std::mem::take(&mut self.data);

        if data_lines.is_empty() {
            self.empty_dispatches += 1;
            println!(
                "  [event boundary] empty data buffer (event type buffer was {:?}) => per the SSE spec this is NOT dispatched as an event",
                event_type
            );
            return;
        }

        self.dispatched_events += 1;
        if data_lines.len() > 1 {
            self.multi_data_events += 1;
        }
        let payload = data_lines.join("\n");
        let name = if event_type.is_empty() {
            "message"
        } else {
            &event_type
        };

        println!(
            "  ==[ EVENT: {name:?}, last-event-id {:?}, {} data line(s) ]==",
            self.last_id,
            data_lines.len()
        );
        println!("    data: {payload}");

        match serde_json::from_str::<serde_json::Value>(&payload) {
            Ok(serde_json::Value::Object(object)) => {
                let keys: Vec<&String> = object.keys().collect();
                println!("    JSON object with top-level keys: {keys:?}");
                match object.get("@type") {
                    Some(value) => {
                        println!("    \"@type\" member: PRESENT = {value}");
                        if name == "state" {
                            self.state_with_type += 1;
                        }
                    }
                    None => {
                        println!("    \"@type\" member: ABSENT");
                        if name == "state" {
                            self.state_without_type += 1;
                        }
                    }
                }
            }
            Ok(other) => println!("    JSON, but not an object: {other}"),
            Err(e) => println!("    not valid JSON ({e})"),
        }
    }

    fn summary(&self) {
        println!();
        println!("=== RAW MODE SUMMARY ===");
        println!(
            "line terminators: {} CRLF, {} bare LF, {} bare CR",
            self.crlf_lines, self.lf_lines, self.cr_lines
        );
        println!(
            "comment lines (keep-alives):               {}",
            self.comment_lines
        );
        println!(
            "event boundaries with an EMPTY data buffer: {} (not dispatchable per spec)",
            self.empty_dispatches
        );
        println!(
            "dispatched events:                          {}",
            self.dispatched_events
        );
        println!(
            "  of which spanned multiple data lines:     {}",
            self.multi_data_events
        );
        println!(
            "'state' events WITH    an \"@type\" member:   {}",
            self.state_with_type
        );
        println!(
            "'state' events WITHOUT an \"@type\" member:   {}",
            self.state_without_type
        );
    }
}

/// Consumes the same endpoint through jmap-client (mirroring the main
/// application's configuration) and prints every stream item it yields.
async fn client_mode(args: &Args, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CLIENT MODE: the same endpoint as interpreted by jmap-client ===");

    let host = url::Url::parse(&args.url)?
        .host_str()
        .map(str::to_string)
        .ok_or("the --url has no host")?;
    let client = jmap_client::client::Client::new()
        .credentials(jmap_client::client::Credentials::bearer(token))
        .follow_redirects([host])
        .connect(&args.url)
        .await?;

    let types = if args.types == "*" {
        None
    } else {
        Some(
            args.types
                .split(',')
                .map(parse_data_type)
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    let mut stream = client
        .event_source(types, args.closeafter == "state", Some(args.ping), None)
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.duration);
    let mut item_no = 0usize;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("\n[probe] duration elapsed");
                break;
            }
            item = stream.next() => match item {
                Some(Ok(notification)) => {
                    item_no += 1;
                    println!("  item {item_no}: Ok({notification:?})");
                }
                Some(Err(e)) => {
                    item_no += 1;
                    println!("  item {item_no}: Err({e}) [debug: {e:?}]");
                }
                None => {
                    println!("[probe] the stream ended");
                    break;
                }
            }
        }
    }

    Ok(())
}

fn parse_data_type(name: &str) -> Result<jmap_client::DataType, String> {
    use jmap_client::DataType;
    match name.trim() {
        "Email" => Ok(DataType::Email),
        "EmailDelivery" => Ok(DataType::EmailDelivery),
        "EmailSubmission" => Ok(DataType::EmailSubmission),
        "Mailbox" => Ok(DataType::Mailbox),
        "Thread" => Ok(DataType::Thread),
        "Identity" => Ok(DataType::Identity),
        other => Err(format!(
            "unsupported type name '{other}' (use --types '*' for everything)"
        )),
    }
}
