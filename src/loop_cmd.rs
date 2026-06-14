use std::error::Error;
use std::fmt;
use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
use dialoguer::console::style;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{ACCEPT, ACCEPT_ENCODING};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::http::{build_http_client, build_http_client_from_builder, DEFAULT_HTTP_TIMEOUT};
use crate::project_context::{resolve_project_command_context_with_auth_mode, ProjectContext};
use crate::ui::{
    animations_enabled, apply_column_padding, header, is_interactive, is_quiet, print_with_pager,
    styled_table, truncate, LinePrompt,
};

const DEFAULT_AGENT_SLUG: &str = "loop-chat";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt loop
  bt loop --list
  bt loop \"Find the most expensive traces from the last day\"
  bt loop --conversation daily-debug \"What changed since yesterday?\"
  bt loop --harness codex --model gpt-5.4 \"Investigate this project\"
")]
pub struct LoopArgs {
    /// Message to send. Omit to start an interactive session.
    #[arg(value_name = "MESSAGE")]
    message: Vec<String>,

    /// Loop Runtime base URL. Defaults to the Braintrust API URL plus /loop-runtime.
    #[arg(
        long = "runtime-url",
        env = "BT_LOOP_RUNTIME_URL",
        hide_env_values = true
    )]
    runtime_url: Option<String>,

    /// Loop agent slug to use or create
    #[arg(long, env = "BT_LOOP_AGENT", default_value = DEFAULT_AGENT_SLUG)]
    agent: String,

    /// List recent Loop conversations instead of starting a chat
    #[arg(long = "list", default_value_t = false)]
    list: bool,

    /// Number of conversations to list
    #[arg(long, env = "BT_LOOP_LIMIT", default_value_t = 20)]
    limit: usize,

    /// Conversation slug or id to resume. Creates a slug if it does not exist.
    #[arg(long, short = 'c', env = "BT_LOOP_CONVERSATION")]
    conversation: Option<String>,

    /// Name for a newly created conversation
    #[arg(long = "conversation-name", env = "BT_LOOP_CONVERSATION_NAME")]
    conversation_name: Option<String>,

    /// Backend harness to use
    #[arg(long, env = "BT_LOOP_HARNESS", value_enum, default_value_t = HarnessArg::Default)]
    harness: HarnessArg,

    /// Model override for this turn
    #[arg(long, env = "BT_LOOP_MODEL")]
    model: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HarnessArg {
    Default,
    Codex,
    #[value(name = "claude-code")]
    ClaudeCode,
}

impl std::fmt::Display for HarnessArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_wire())
    }
}

impl HarnessArg {
    fn as_wire(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Codex => "codex",
            Self::ClaudeCode => "claude-code",
        }
    }
}

pub async fn run(base: BaseArgs, args: LoopArgs) -> Result<()> {
    let message = args.message.join(" ").trim().to_string();

    if args.list && !message.is_empty() {
        bail!("MESSAGE cannot be used with --list");
    }

    if base.json {
        if message.is_empty() && !args.list {
            bail!("MESSAGE is required with --json. Example: bt loop --json \"Summarize this project\"");
        }
    } else if message.is_empty() && !args.list && !is_interactive() {
        bail!(
            "MESSAGE is required in non-interactive mode. Example: bt loop \"Summarize this project\""
        );
    }

    let ctx = resolve_project_command_context_with_auth_mode(&base, false).await?;
    let runtime_url = resolve_loop_runtime_url(ctx.client.base_url(), args.runtime_url.as_deref())?;
    let client = LoopRuntimeClient::new(runtime_url.as_str(), ctx.client.api_key())?;

    if args.list {
        let conversations = client.list_conversations(&ctx.project.id, &args).await?;
        if base.json {
            println!("{}", serde_json::to_string(&conversations)?);
        } else {
            print_conversation_list(&ctx, &conversations)?;
        }
        return Ok(());
    }

    if base.json {
        let conversation = client.create_conversation(&ctx, &args).await?;
        let report = send_and_collect(&client, &ctx, &conversation, &args, &message, true).await?;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    if !message.is_empty() {
        let conversation = client.create_conversation(&ctx, &args).await?;
        print_chat_header(&ctx, &conversation, &args);
        send_and_print(&client, &ctx, &conversation, &args, &message).await?;
        return Ok(());
    }

    print_project_header(&ctx);
    run_interactive_chat(&client, &ctx, &args).await
}

fn resolve_loop_runtime_url(api_url: &str, explicit_runtime_url: Option<&str>) -> Result<String> {
    if let Some(runtime_url) = explicit_runtime_url {
        let runtime_url = runtime_url.trim();
        if runtime_url.is_empty() {
            bail!("--runtime-url must not be empty");
        }
        return Ok(runtime_url.trim_end_matches('/').to_string());
    }

    Ok(format!("{}/loop-runtime", api_url.trim_end_matches('/')))
}

async fn run_interactive_chat(
    client: &LoopRuntimeClient,
    ctx: &ProjectContext,
    args: &LoopArgs,
) -> Result<()> {
    let mut initial_history = Vec::new();
    let mut conversation = if args.conversation.is_some() {
        let created = client.create_conversation(ctx, args).await?;
        print_conversation_header(&created, args);
        let events = client
            .get_conversation_events(&ctx.project.id, &created.agent.id, &created.conversation.id)
            .await?;
        print_history(&events.events)?;
        initial_history = user_message_history(&events.events);
        Some(created)
    } else {
        None
    };
    let mut editor = LinePrompt::new(initial_history);
    let prompt = style("You: ").bold().to_string();
    loop {
        let Some(input) = editor.read_line(&prompt, "You: ".len())? else {
            return Ok(());
        };
        let message = input.trim();
        if message.is_empty() {
            continue;
        }
        if matches!(message, "/exit" | "/quit" | "exit" | "quit") {
            return Ok(());
        }
        if conversation.is_none() {
            let created = client.create_conversation(ctx, args).await?;
            print_conversation_header(&created, args);
            conversation = Some(created);
        }
        let conversation = conversation
            .as_ref()
            .expect("conversation is created before sending a Loop message");
        send_and_print(client, ctx, conversation, args, message).await?;
        editor.add_history(message);
    }
}

fn print_history(events: &[RuntimeEvent]) -> Result<()> {
    for event in events {
        match event.event_type() {
            Some("messages") => print_history_messages(event)?,
            Some("error") => {
                let message = event
                    .data
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Loop turn failed");
                eprintln!("{} {}", style("error").red().bold(), message);
            }
            _ => {}
        }
    }
    Ok(())
}

fn print_history_messages(event: &RuntimeEvent) -> Result<()> {
    let Some(messages) = event.data.get("messages").and_then(Value::as_array) else {
        return Ok(());
    };
    for message in messages {
        let text = message_content_text(message.get("content"));
        if text.trim().is_empty() {
            continue;
        }
        match message.get("role").and_then(Value::as_str) {
            Some("user") => println!("{} {text}", style("You:").bold()),
            Some("assistant") => println!("{} {text}", style("Loop:").bold()),
            _ => {}
        }
    }
    io::stdout().flush()?;
    Ok(())
}

fn user_message_history(events: &[RuntimeEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| event.data.get("messages").and_then(Value::as_array))
        .flat_map(|messages| messages.iter())
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .map(|message| message_content_text(message.get("content")))
        .filter(|text| !text.trim().is_empty())
        .collect()
}

async fn send_and_print(
    client: &LoopRuntimeClient,
    ctx: &ProjectContext,
    conversation: &CreateConversationResponse,
    args: &LoopArgs,
    message: &str,
) -> Result<()> {
    let mut renderer = TranscriptRenderer::default();
    let report =
        send_and_collect_with_callback(client, ctx, conversation, args, message, false, |event| {
            renderer.render_event(event)
        })
        .await?;
    renderer.finish_assistant_line()?;
    if report.ended_with_error {
        bail!("Loop turn failed");
    }
    Ok(())
}

async fn send_and_collect(
    client: &LoopRuntimeClient,
    ctx: &ProjectContext,
    conversation: &CreateConversationResponse,
    args: &LoopArgs,
    message: &str,
    include_events: bool,
) -> Result<LoopChatReport> {
    send_and_collect_with_callback(
        client,
        ctx,
        conversation,
        args,
        message,
        include_events,
        |_| Ok(()),
    )
    .await
}

async fn send_and_collect_with_callback<F>(
    client: &LoopRuntimeClient,
    ctx: &ProjectContext,
    conversation: &CreateConversationResponse,
    args: &LoopArgs,
    message: &str,
    include_events: bool,
    mut on_event: F,
) -> Result<LoopChatReport>
where
    F: FnMut(&RuntimeEvent) -> Result<()>,
{
    let submission = client
        .submit_turn(
            &ctx.project.id,
            &conversation.agent.id,
            &conversation.conversation.id,
            SubmitTurnBody {
                input: vec![json!({
                    "role": "user",
                    "content": message,
                })],
                harness: args.harness.as_wire(),
                model: args.model.as_deref(),
            },
        )
        .await?;

    let mut status = TurnStatus::new(!include_events, "Waiting for Loop...");
    let mut collected = Vec::new();
    let mut ended_with_error = false;
    client
        .watch_events(
            &ctx.project.id,
            &submission.agent.id,
            &submission.conversation.id,
            conversation.conversation.latest_event_id.as_deref(),
            |event| {
                if event.turn_id.as_deref() != Some(submission.turn.id.as_str()) {
                    return Ok(true);
                }

                if let Some(message) = runtime_status_message(&event) {
                    status.set_message(message);
                }
                if event_starts_visible_output(&event) || event.is_turn_ended() {
                    status.clear();
                }
                if event.is_error() {
                    ended_with_error = true;
                }
                if event.is_turn_ended() {
                    if include_events {
                        collected.push(event);
                    }
                    return Ok(false);
                }
                if runtime_status_message(&event).is_none() {
                    on_event(&event)?;
                }
                if include_events {
                    collected.push(event);
                }
                Ok(true)
            },
        )
        .await?;
    status.clear();

    Ok(LoopChatReport {
        submission,
        events: collected,
        ended_with_error,
    })
}

fn print_chat_header(
    ctx: &ProjectContext,
    conversation: &CreateConversationResponse,
    args: &LoopArgs,
) {
    print_project_header(ctx);
    print_conversation_header(conversation, args);
}

fn print_project_header(ctx: &ProjectContext) {
    if is_quiet() {
        return;
    }
    eprintln!(
        "{} {} {} {}",
        style("Loop").bold(),
        style("->").dim(),
        style(ctx.project.name.as_str()).bold(),
        style(format!("({})", ctx.project.id)).dim()
    );
}

fn print_conversation_header(conversation: &CreateConversationResponse, args: &LoopArgs) {
    if is_quiet() {
        return;
    }
    eprintln!(
        "{} {} {}",
        style("Conversation").dim(),
        style(conversation.conversation.slug.as_str()).bold(),
        style(format!("[{}]", args.harness)).dim()
    );
}

fn print_conversation_list(
    ctx: &ProjectContext,
    response: &ListConversationsResponse,
) -> Result<()> {
    let mut output = String::new();
    if response.conversations.is_empty() {
        output.push_str("No Loop conversations found.\n");
        print_with_pager(&output)?;
        return Ok(());
    }

    output.push_str(
        format!(
            "{} {} {} {} {}\n\n",
            style(response.conversations.len()).bold(),
            style("Loop conversations in").dim(),
            style(ctx.project.name.as_str()).bold(),
            style("for agent").dim(),
            style(response.agent.slug.as_str()).bold()
        )
        .as_str(),
    );

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Slug"),
        header("ID"),
        header("Latest event"),
    ]);
    apply_column_padding(&mut table, (0, 4));
    for conversation in &response.conversations {
        let latest_event = conversation
            .latest_event_id
            .as_deref()
            .map(|id| truncate(id, 12))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            truncate(&conversation.name, 28),
            truncate(&conversation.slug, 28),
            truncate(&conversation.id, 12),
            latest_event,
        ]);
    }
    output.push_str(&table.to_string());
    print_with_pager(&output)?;
    Ok(())
}

#[derive(Clone)]
struct LoopRuntimeClient {
    http: reqwest::Client,
    watch_http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl LoopRuntimeClient {
    fn new(base_url: &str, api_key: &str) -> Result<Self> {
        if base_url.trim().is_empty() {
            bail!("--runtime-url must not be empty");
        }
        Ok(Self {
            http: build_http_client(DEFAULT_HTTP_TIMEOUT)?,
            watch_http: build_http_client_from_builder(
                reqwest::Client::builder().connect_timeout(DEFAULT_HTTP_TIMEOUT),
            )?,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
        })
    }

    async fn create_conversation(
        &self,
        ctx: &ProjectContext,
        args: &LoopArgs,
    ) -> Result<CreateConversationResponse> {
        let conversation_id = args.conversation.as_deref().filter(|value| is_uuid(value));
        let conversation_slug = args.conversation.as_deref().filter(|value| !is_uuid(value));
        self.post(
            &format!(
                "/project/{}/runtime/agents/{}/conversations",
                encode(&ctx.project.id),
                encode(&args.agent)
            ),
            &json!({
                "conversation_id": conversation_id,
                "conversation_slug": conversation_slug,
                "conversation_name": args.conversation_name.as_deref(),
                "harness": args.harness.as_wire(),
            }),
        )
        .await
        .map_err(|err| {
            LoopCommandError::new(
                format!(
                    "failed to create or resolve Loop conversation for project '{}'",
                    ctx.project.name
                ),
                err,
            )
            .into()
        })
    }

    async fn list_conversations(
        &self,
        project_id: &str,
        args: &LoopArgs,
    ) -> Result<ListConversationsResponse> {
        let agent = self.agent_by_slug(project_id, &args.agent).await?;
        let response = self
            .exo_request(
                project_id,
                ExoRequest::ListConversations {
                    agent_id: agent.id.clone(),
                    request: ListConversationsRequest {
                        cursor: None,
                        limit: Some(args.limit),
                    },
                },
            )
            .await
            .map_err(|err| LoopCommandError::new("failed to list Loop conversations", err))?;
        match response {
            ExoResponse::Conversations { result } => Ok(ListConversationsResponse {
                agent,
                conversations: result
                    .conversations
                    .into_iter()
                    .map(|conversation| conversation.record)
                    .collect(),
                next_cursor: result.next_cursor,
            }),
            response => Err(LoopCommandError::new(
                "failed to list Loop conversations",
                LoopRuntimeRequestError::new(format!(
                    "Loop Runtime returned unexpected Exo response: {}",
                    response.response_type()
                ))
                .into(),
            )
            .into()),
        }
    }

    async fn get_conversation_events(
        &self,
        project_id: &str,
        agent_id: &str,
        conversation_id: &str,
    ) -> Result<GetConversationEventsResponse> {
        let response = self
            .exo_request(
                project_id,
                ExoRequest::ConversationGetEvents {
                    agent_id: agent_id.to_string(),
                    conversation_id: conversation_id.to_string(),
                    query: Some(EventQuery {
                        cursor: None,
                        direction: Some(EventQueryDirection::Asc),
                        limit: None,
                        session_id: None,
                        turn_id: None,
                        types: None,
                    }),
                },
            )
            .await
            .map_err(|err| LoopCommandError::new("failed to load Loop conversation", err))?;
        match response {
            ExoResponse::Events { result } => Ok(GetConversationEventsResponse {
                events: result.events,
                cursor: result.cursor,
            }),
            response => Err(LoopCommandError::new(
                "failed to load Loop conversation",
                LoopRuntimeRequestError::new(format!(
                    "Loop Runtime returned unexpected Exo response: {}",
                    response.response_type()
                ))
                .into(),
            )
            .into()),
        }
    }

    async fn agent_by_slug(&self, project_id: &str, slug: &str) -> Result<AgentRecord> {
        let response = self
            .exo_request(project_id, ExoRequest::ListAgents)
            .await
            .map_err(|err| LoopCommandError::new("failed to list Loop agents", err))?;
        let ExoResponse::Agents { agents } = response else {
            return Err(LoopCommandError::new(
                "failed to list Loop agents",
                LoopRuntimeRequestError::new("Loop Runtime returned unexpected Exo response")
                    .into(),
            )
            .into());
        };
        agents
            .into_iter()
            .find(|agent| agent.slug == slug)
            .ok_or_else(|| {
                LoopCommandError::new(
                    "failed to list Loop conversations",
                    LoopRuntimeRequestError::new(format!("Loop agent not found: {slug}")).into(),
                )
                .into()
            })
    }

    async fn submit_turn(
        &self,
        project_id: &str,
        agent_id: &str,
        conversation_id: &str,
        body: SubmitTurnBody<'_>,
    ) -> Result<SubmitTurnResponse> {
        self.post(
            &format!(
                "/project/{}/runtime/agents/{}/conversations/{}/turns",
                encode(project_id),
                encode(agent_id),
                encode(conversation_id)
            ),
            &body,
        )
        .await
        .map_err(|err| LoopCommandError::new("failed to submit Loop turn", err).into())
    }

    async fn exo_request(&self, project_id: &str, request: ExoRequest) -> Result<ExoResponse> {
        let message = ExoClientMessage::Request { id: 1, request };
        let response: ExoServerMessage = self
            .post(
                &format!("/project/{}/exo/request", encode(project_id)),
                &message,
            )
            .await?;
        let ExoServerMessage::Response {
            ok,
            response,
            error,
            ..
        } = response;
        if !ok {
            return Err(LoopRuntimeRequestError::new(format!(
                "Loop Runtime Exo request failed: {}",
                error.unwrap_or_else(|| "unknown error".to_string())
            ))
            .into());
        }
        response.ok_or_else(|| {
            LoopRuntimeRequestError::new("Loop Runtime Exo response was empty").into()
        })
    }

    async fn watch_events<F>(
        &self,
        project_id: &str,
        agent_id: &str,
        conversation_id: &str,
        after: Option<&str>,
        mut on_event: F,
    ) -> Result<()>
    where
        F: FnMut(RuntimeEvent) -> Result<bool>,
    {
        let mut url = reqwest::Url::parse(&self.url(&format!(
            "/project/{}/runtime/agents/{}/conversations/{}/events/watch",
            encode(project_id),
            encode(agent_id),
            encode(conversation_id)
        )))?;
        if let Some(after) = after {
            url.query_pairs_mut().append_pair("after", after);
        }
        let url_string = url.to_string();
        let response = self
            .watch_http
            .get(url)
            .bearer_auth(&self.api_key)
            .header(ACCEPT, "text/event-stream")
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(|err| {
                LoopCommandError::new(
                    "failed to watch Loop events",
                    LoopRuntimeRequestError::with_source(
                        format!("Loop Runtime request failed: GET {url_string}"),
                        err,
                    )
                    .into(),
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.map_err(|err| {
                LoopCommandError::new(
                    "failed to watch Loop events",
                    LoopRuntimeRequestError::with_source(
                        format!("failed to read Loop Runtime response body from GET {url_string}"),
                        err,
                    )
                    .into(),
                )
            })?;
            let body_text = String::from_utf8_lossy(&body);
            return Err(LoopCommandError::new(
                "failed to watch Loop events",
                LoopRuntimeRequestError::new(format!(
                    "Loop Runtime request failed: GET {url_string} returned {status}: {body_text}"
                ))
                .into(),
            )
            .into());
        }
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|err| {
                LoopCommandError::new(
                    "failed to watch Loop events",
                    LoopRuntimeRequestError::with_source(
                        format!("Loop Runtime event stream failed: GET {url_string}"),
                        err,
                    )
                    .into(),
                )
            })?;
            buffer.extend_from_slice(&chunk);
            while let Some((boundary, separator_len)) = sse_event_boundary(&buffer) {
                let raw_event = buffer.drain(..boundary).collect::<Vec<_>>();
                buffer.drain(..separator_len);
                if let Some(event) = parse_sse_event(&raw_event)? {
                    match event.name.as_str() {
                        "exo_event" => {
                            let runtime_event = serde_json::from_str::<RuntimeEvent>(&event.data)
                                .with_context(|| {
                                format!("failed to parse Loop Runtime event from {url_string}")
                            })?;
                            if !on_event(runtime_event)? {
                                return Ok(());
                            }
                        }
                        "error" => {
                            bail!("Loop Runtime event stream failed: {}", event.data);
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn post<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize,
    {
        let url = self.url(path);
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(|err| {
                LoopRuntimeRequestError::with_source(
                    format!("Loop Runtime request failed: POST {url}"),
                    err,
                )
            })?;
        parse_loop_runtime_response(response, "POST", &url).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }
}

#[derive(Debug)]
struct LoopCommandError {
    message: String,
    source: anyhow::Error,
}

impl LoopCommandError {
    fn new(message: impl Into<String>, source: anyhow::Error) -> Self {
        Self {
            message: message.into(),
            source,
        }
    }
}

impl fmt::Display for LoopCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.message, self.source)
    }
}

impl Error for LoopCommandError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
struct LoopRuntimeRequestError {
    message: String,
    source: Option<reqwest::Error>,
}

impl LoopRuntimeRequestError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    fn with_source(message: impl Into<String>, source: reqwest::Error) -> Self {
        Self {
            message: message.into(),
            source: Some(source),
        }
    }
}

impl fmt::Display for LoopRuntimeRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(f, "{}: {source}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl Error for LoopRuntimeRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|err| err as &(dyn Error + 'static))
    }
}

async fn parse_loop_runtime_response<T>(
    response: reqwest::Response,
    method: &str,
    url: &str,
) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response.bytes().await.map_err(|err| {
        LoopRuntimeRequestError::with_source(
            format!("failed to read Loop Runtime response body from {method} {url}"),
            err,
        )
    })?;
    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(LoopRuntimeRequestError::new(format!(
            "Loop Runtime request failed: {method} {url} returned {status}: {body_text}"
        ))
        .into());
    }
    serde_json::from_slice(&body)
        .with_context(|| format!("failed to parse Loop Runtime response from {method} {url}"))
}

struct SseEvent {
    name: String,
    data: String,
}

fn sse_event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
        return Some((index, 4));
    }
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2))
}

fn parse_sse_event(raw_event: &[u8]) -> Result<Option<SseEvent>> {
    if raw_event.is_empty() {
        return Ok(None);
    }
    let text = std::str::from_utf8(raw_event).context("failed to parse Loop Runtime SSE frame")?;
    let mut name = "message".to_string();
    let mut data = Vec::new();
    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if let Some(value) = line.strip_prefix("event:") {
            name = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start().to_string());
        }
    }
    Ok(Some(SseEvent {
        name,
        data: data.join("\n"),
    }))
}

struct TurnStatus {
    spinner: Option<ProgressBar>,
}

impl TurnStatus {
    fn new(enabled: bool, message: &str) -> Self {
        if !enabled || !io::stderr().is_terminal() || !animations_enabled() || is_quiet() {
            return Self { spinner: None };
        }
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
                .template("{spinner:.cyan} {msg}")
                .expect("spinner template should be valid"),
        );
        spinner.set_message(message.to_string());
        spinner.enable_steady_tick(Duration::from_millis(80));
        Self {
            spinner: Some(spinner),
        }
    }

    fn set_message(&self, message: &str) {
        if let Some(spinner) = &self.spinner {
            spinner.set_message(message.to_string());
        }
    }

    fn clear(&mut self) {
        if let Some(spinner) = self.spinner.take() {
            spinner.finish_and_clear();
        }
    }
}

impl Drop for TurnStatus {
    fn drop(&mut self) {
        self.clear();
    }
}

#[derive(Debug, Serialize)]
struct LoopChatReport {
    submission: SubmitTurnResponse,
    events: Vec<RuntimeEvent>,
    ended_with_error: bool,
}

#[derive(Debug, Serialize)]
struct ListConversationsResponse {
    agent: AgentRecord,
    conversations: Vec<ConversationRecord>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct GetConversationEventsResponse {
    events: Vec<RuntimeEvent>,
    cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExoClientMessage {
    Request { id: u64, request: ExoRequest },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExoRequest {
    ListAgents,
    ListConversations {
        agent_id: String,
        request: ListConversationsRequest,
    },
    ConversationGetEvents {
        agent_id: String,
        conversation_id: String,
        query: Option<EventQuery>,
    },
}

#[derive(Debug, Serialize)]
struct ListConversationsRequest {
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct EventQuery {
    cursor: Option<String>,
    direction: Option<EventQueryDirection>,
    limit: Option<u32>,
    session_id: Option<String>,
    turn_id: Option<String>,
    types: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventQueryDirection {
    Asc,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExoServerMessage {
    Response {
        #[serde(rename = "id")]
        _id: u64,
        ok: bool,
        response: Option<ExoResponse>,
        error: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExoResponse {
    Agents {
        agents: Vec<AgentRecord>,
    },
    Conversations {
        result: ExoListConversationsResult,
    },
    Events {
        result: ExoEventsResult,
    },
    #[serde(other)]
    Unknown,
}

impl ExoResponse {
    fn response_type(&self) -> &'static str {
        match self {
            Self::Agents { .. } => "agents",
            Self::Conversations { .. } => "conversations",
            Self::Events { .. } => "events",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExoListConversationsResult {
    conversations: Vec<ConversationHandleInfo>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConversationHandleInfo {
    record: ConversationRecord,
}

#[derive(Debug, Deserialize)]
struct ExoEventsResult {
    events: Vec<RuntimeEvent>,
    cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentRecord {
    id: String,
    slug: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConversationRecord {
    id: String,
    slug: String,
    name: String,
    #[serde(default)]
    latest_event_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TurnRecord {
    id: String,
    session_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateConversationResponse {
    agent: AgentRecord,
    conversation: ConversationRecord,
    harness: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SubmitTurnResponse {
    agent: AgentRecord,
    conversation: ConversationRecord,
    turn: TurnRecord,
    harness: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeEvent {
    id: String,
    #[serde(default)]
    turn_id: Option<String>,
    data: Value,
}

impl RuntimeEvent {
    fn event_type(&self) -> Option<&str> {
        self.data.get("type").and_then(Value::as_str)
    }

    fn is_turn_ended(&self) -> bool {
        self.event_type() == Some("turn_ended")
    }

    fn is_error(&self) -> bool {
        self.event_type() == Some("error")
    }
}

fn runtime_status_message(event: &RuntimeEvent) -> Option<&str> {
    if event.event_type() != Some("custom") {
        return None;
    }
    if event.data.get("event_type").and_then(Value::as_str) != Some("agent_runtime.status") {
        return None;
    }
    event
        .data
        .pointer("/payload/message")
        .and_then(Value::as_str)
}

fn event_starts_visible_output(event: &RuntimeEvent) -> bool {
    match event.event_type() {
        Some("lingua_stream_chunk") => !stream_chunk_text(event).is_empty(),
        Some("messages") => event
            .data
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message.get("role").and_then(Value::as_str) == Some("assistant")
                        && !message_content_text(message.get("content"))
                            .trim()
                            .is_empty()
                })
            }),
        Some("tool_requested" | "tool_result" | "error") => true,
        _ => false,
    }
}

#[derive(Debug, Serialize)]
struct SubmitTurnBody<'a> {
    input: Vec<Value>,
    harness: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
}

#[derive(Default)]
struct TranscriptRenderer {
    assistant_line_open: bool,
    streamed_assistant_text: bool,
    rendered_assistant_message: bool,
}

impl TranscriptRenderer {
    fn render_event(&mut self, event: &RuntimeEvent) -> Result<()> {
        match event.event_type() {
            Some("lingua_stream_chunk") => self.render_stream_chunk(event),
            Some("messages") => self.render_messages(event),
            Some("tool_requested") => self.render_tool_request(event),
            Some("tool_result") => self.render_tool_result(event),
            Some("custom") => self.render_custom(event),
            Some("error") => self.render_error(event),
            _ => Ok(()),
        }
    }

    fn render_stream_chunk(&mut self, event: &RuntimeEvent) -> Result<()> {
        let text = stream_chunk_text(event);
        if text.is_empty() {
            return Ok(());
        }
        self.open_assistant_line()?;
        print!("{text}");
        io::stdout().flush()?;
        self.streamed_assistant_text = true;
        Ok(())
    }

    fn render_messages(&mut self, event: &RuntimeEvent) -> Result<()> {
        if self.streamed_assistant_text || self.rendered_assistant_message {
            return Ok(());
        }
        let Some(messages) = event.data.get("messages").and_then(Value::as_array) else {
            return Ok(());
        };
        for message in messages {
            if message.get("role").and_then(Value::as_str) != Some("assistant") {
                continue;
            }
            let text = message_content_text(message.get("content"));
            if text.trim().is_empty() {
                continue;
            }
            self.open_assistant_line()?;
            print!("{text}");
            self.rendered_assistant_message = true;
        }
        io::stdout().flush()?;
        Ok(())
    }

    fn render_tool_request(&mut self, event: &RuntimeEvent) -> Result<()> {
        self.finish_assistant_line()?;
        let function_name = event
            .data
            .pointer("/request/function_name")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        eprintln!("{} {}", style("tool").dim(), style(function_name).cyan());
        Ok(())
    }

    fn render_tool_result(&mut self, event: &RuntimeEvent) -> Result<()> {
        self.finish_assistant_line()?;
        let tool_call_id = event
            .data
            .get("tool_call_id")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        eprintln!(
            "{} {}",
            style("tool result").dim(),
            style(tool_call_id).dim()
        );
        Ok(())
    }

    fn render_custom(&mut self, event: &RuntimeEvent) -> Result<()> {
        if event.data.get("event_type").and_then(Value::as_str) != Some("agent_runtime.status") {
            return Ok(());
        }
        let Some(message) = event
            .data
            .pointer("/payload/message")
            .and_then(Value::as_str)
        else {
            return Ok(());
        };
        self.finish_assistant_line()?;
        eprintln!("{}", style(message).dim());
        Ok(())
    }

    fn render_error(&mut self, event: &RuntimeEvent) -> Result<()> {
        self.finish_assistant_line()?;
        let message = event
            .data
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Loop turn failed");
        eprintln!("{} {}", style("error").red().bold(), message);
        Ok(())
    }

    fn open_assistant_line(&mut self) -> Result<()> {
        if self.assistant_line_open {
            return Ok(());
        }
        print!("{}", style("Loop: ").bold());
        io::stdout().flush()?;
        self.assistant_line_open = true;
        Ok(())
    }

    fn finish_assistant_line(&mut self) -> Result<()> {
        if self.assistant_line_open {
            println!();
            io::stdout().flush()?;
            self.assistant_line_open = false;
        }
        Ok(())
    }
}

fn stream_chunk_text(event: &RuntimeEvent) -> String {
    let Some(choices) = event
        .data
        .pointer("/chunk/choices")
        .and_then(Value::as_array)
    else {
        return String::new();
    };
    choices
        .iter()
        .filter_map(|choice| {
            choice
                .pointer("/delta/content")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>()
        .join("")
}

fn message_content_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return false;
            }
        } else if !byte.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_detection_accepts_canonical_ids() {
        assert!(is_uuid("123e4567-e89b-12d3-a456-426614174000"));
        assert!(!is_uuid("daily-debug"));
    }

    #[test]
    fn default_agent_slug_matches_loop_chat_runtime_agent() {
        assert_eq!(DEFAULT_AGENT_SLUG, "loop-chat");
    }

    #[test]
    fn loop_runtime_url_defaults_to_api_proxy_path() {
        assert_eq!(
            resolve_loop_runtime_url("http://localhost:8000/", None).expect("runtime URL"),
            "http://localhost:8000/loop-runtime"
        );
    }

    #[test]
    fn loop_runtime_url_uses_explicit_override() {
        assert_eq!(
            resolve_loop_runtime_url(
                "http://localhost:8000",
                Some(" https://loop-runtime.example.test/ "),
            )
            .expect("runtime URL"),
            "https://loop-runtime.example.test"
        );
    }

    #[test]
    fn stream_chunk_text_reads_openai_style_delta_content() {
        let event = RuntimeEvent {
            id: "event-id".to_string(),
            turn_id: Some("turn-id".to_string()),
            data: json!({
                "type": "lingua_stream_chunk",
                "chunk": {
                    "choices": [
                        {"index": 0, "delta": {"content": "hello "}},
                        {"index": 0, "delta": {"content": "world"}}
                    ]
                }
            }),
        };
        assert_eq!(stream_chunk_text(&event), "hello world");
    }

    #[test]
    fn sse_boundary_reads_lf_and_crlf_frames() {
        assert_eq!(sse_event_boundary(b"event: exo_event\n\n"), Some((16, 2)));
        assert_eq!(
            sse_event_boundary(b"event: exo_event\r\n\r\n"),
            Some((16, 4))
        );
    }

    #[test]
    fn parse_sse_event_reads_runtime_frame() {
        let event = parse_sse_event(b"event: exo_event\r\ndata: {\"id\":\"event-id\"}\r\n")
            .expect("valid frame")
            .expect("event");
        assert_eq!(event.name, "exo_event");
        assert_eq!(event.data, "{\"id\":\"event-id\"}");
    }

    #[test]
    fn message_content_text_reads_string_and_text_parts() {
        assert_eq!(
            message_content_text(Some(&json!("plain response"))),
            "plain response"
        );
        assert_eq!(
            message_content_text(Some(&json!([
                {"type": "text", "text": "part one"},
                {"type": "text", "text": " part two"}
            ]))),
            "part one part two"
        );
    }

    #[test]
    fn user_message_history_reads_only_conversation_user_messages() {
        let events = vec![
            RuntimeEvent {
                id: "event-1".to_string(),
                turn_id: Some("turn-1".to_string()),
                data: json!({
                    "type": "messages",
                    "messages": [
                        {"role": "user", "content": "first question"},
                        {"role": "assistant", "content": "first answer"}
                    ]
                }),
            },
            RuntimeEvent {
                id: "event-2".to_string(),
                turn_id: Some("turn-2".to_string()),
                data: json!({
                    "type": "messages",
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": "second question"}]}
                    ]
                }),
            },
        ];

        assert_eq!(
            user_message_history(&events),
            vec!["first question".to_string(), "second question".to_string()]
        );
    }
}
