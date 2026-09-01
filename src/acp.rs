use std::{
    ffi::{OsStr, OsString},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderName, HeaderValue, StatusCode},
        Message,
    },
};

use crate::{
    args::BaseArgs,
    ui::{print_command_status, CommandStatus},
};

const ACP_PATH: &str = "/acp";
const ACP_CONNECTION_ID_HEADER: &str = "acp-connection-id";
const CODEX_ACP_PACKAGE: &str = "@agentclientprotocol/codex-acp";
const INTERNAL_CODEX_SHIM_ENV: &str = "BT_INTERNAL_ACP_CODEX_SHIM";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt acp codex --no-auth
  bt acp codex --no-auth --listen 127.0.0.1:5001
  bt acp codex --no-auth --trace --project test-project
  bt acp codex --no-auth --adapter ./node_modules/@agentclientprotocol/codex-acp/dist/index.js
")]
pub struct AcpArgs {
    #[command(subcommand)]
    command: AcpCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum AcpCommand {
    /// Serve Codex through the ACP WebSocket transport.
    Codex(CodexArgs),
}

#[derive(Debug, Clone, Args)]
struct CodexArgs {
    /// Explicitly allow unauthenticated local access. Only valid with a loopback listener.
    #[arg(long, env = "BT_ACP_NO_AUTH", default_value_t = false)]
    no_auth: bool,

    /// Address for the ACP endpoint. The prototype only permits loopback.
    #[arg(long, env = "BT_ACP_LISTEN", default_value = "127.0.0.1:5001")]
    listen: SocketAddr,

    /// Node.js executable used to run codex-acp's bundled JavaScript entry point.
    #[arg(long, env = "BT_ACP_NODE", default_value = "node")]
    node: OsString,

    /// Path to codex-acp's bundled dist/index.js. By default it is resolved from the current project.
    #[arg(long, env = "BT_ACP_CODEX_ADAPTER")]
    adapter: Option<PathBuf>,

    /// Codex executable used by codex-acp.
    #[arg(long, env = "CODEX_PATH", default_value = "codex")]
    codex: OsString,

    /// Capture the Codex run with Braintrust's existing managed tracing hooks.
    #[arg(long, default_value_t = false)]
    trace: bool,
}

#[derive(Debug, Clone, Args)]
#[command(trailing_var_arg = true)]
pub struct CodexAppServerArgs {
    #[arg(
        long,
        env = INTERNAL_CODEX_SHIM_ENV,
        hide = true,
        hide_env = true,
        default_value_t = false
    )]
    internal_codex_shim: bool,

    #[arg(allow_hyphen_values = true)]
    args: Vec<OsString>,
}

#[derive(Clone)]
struct AgentLaunch {
    program: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    remove_braintrust_api_key: bool,
}

impl AgentLaunch {
    fn spawn(&self) -> Result<Child> {
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .envs(self.env.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if self.remove_braintrust_api_key {
            command.env_remove("BRAINTRUST_API_KEY");
        }
        command.spawn().with_context(|| {
            format!(
                "failed to launch ACP agent with {}",
                self.program.to_string_lossy()
            )
        })
    }
}

pub async fn run(base: BaseArgs, args: AcpArgs) -> Result<()> {
    match args.command {
        AcpCommand::Codex(args) => run_codex(base, args).await,
    }
}

async fn run_codex(base: BaseArgs, args: CodexArgs) -> Result<()> {
    if !args.no_auth {
        bail!(
            "an ACP access mode is required; for local development pass --no-auth (loopback only)"
        );
    }
    if !args.listen.ip().is_loopback() {
        bail!("--no-auth only permits a loopback listener; use 127.0.0.1 or ::1");
    }

    let adapter = resolve_codex_adapter(&args.node, args.adapter.as_deref()).await?;
    let launch = codex_launch(&base, &args, adapter)?;
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to listen on {}", args.listen))?;
    let address = listener.local_addr()?;
    print_command_status(
        CommandStatus::Success,
        &format!("ACP WebSocket endpoint listening at ws://{address}{ACP_PATH}"),
    );

    let launch = Arc::new(launch);
    loop {
        let (stream, peer) = listener.accept().await?;
        let launch = Arc::clone(&launch);
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, launch).await {
                print_command_status(
                    CommandStatus::Warning,
                    &format!("ACP connection from {peer} ended: {error:#}"),
                );
            }
        });
    }
}

async fn resolve_codex_adapter(node: &OsStr, configured: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = configured {
        if !path.is_file() {
            bail!("codex-acp adapter does not exist: {}", path.display());
        }
        return Ok(path.to_path_buf());
    }

    let script = format!("require.resolve({CODEX_ACP_PACKAGE:?})");
    let output = Command::new(node)
        .args([OsStr::new("-p"), OsStr::new(&script)])
        .output()
        .await
        .with_context(|| {
            format!(
                "failed to run Node.js executable {}",
                node.to_string_lossy()
            )
        })?;
    if !output.status.success() {
        bail!(
            "could not resolve {CODEX_ACP_PACKAGE} from the current project; install it locally or pass --adapter <PATH>"
        );
    }
    let path =
        String::from_utf8(output.stdout).context("Node.js returned a non-UTF-8 codex-acp path")?;
    let path = PathBuf::from(path.trim());
    if !path.is_file() {
        bail!(
            "resolved codex-acp adapter does not exist: {}",
            path.display()
        );
    }
    Ok(path)
}

fn codex_launch(base: &BaseArgs, args: &CodexArgs, adapter: PathBuf) -> Result<AgentLaunch> {
    let mut env = vec![(
        OsString::from("CODEX_PATH"),
        if args.trace {
            std::env::current_exe()
                .context("failed to resolve the bt executable for Codex tracing")?
                .into_os_string()
        } else {
            args.codex.clone()
        },
    )];
    let mut remove_braintrust_api_key = false;
    if args.trace {
        env.push((
            OsString::from(INTERNAL_CODEX_SHIM_ENV),
            OsString::from("true"),
        ));
        env.push((OsString::from("CODEX_BIN"), args.codex.clone()));
        append_trace_environment(&mut env, base);
        remove_braintrust_api_key = base.profile.is_some();
    }
    Ok(AgentLaunch {
        program: args.node.clone(),
        args: vec![adapter.into_os_string()],
        env,
        remove_braintrust_api_key,
    })
}

fn append_trace_environment(env: &mut Vec<(OsString, OsString)>, base: &BaseArgs) {
    for (name, value) in [
        ("BRAINTRUST_PROFILE", base.profile.as_deref()),
        ("BRAINTRUST_ORG_NAME", base.org_name.as_deref()),
        ("BRAINTRUST_DEFAULT_PROJECT", base.project.as_deref()),
        ("BRAINTRUST_API_URL", base.api_url.as_deref()),
        ("BRAINTRUST_APP_URL", base.app_url.as_deref()),
    ] {
        if let Some(value) = value {
            env.push((OsString::from(name), OsString::from(value)));
        }
    }
    if let Some(path) = base.ca_cert() {
        env.push((
            OsString::from("BRAINTRUST_CA_CERT"),
            path.as_os_str().to_os_string(),
        ));
    }
    if base.no_input {
        env.push((
            OsString::from("BRAINTRUST_NO_INPUT"),
            OsString::from("true"),
        ));
    }
}

pub async fn run_codex_app_server(base: BaseArgs, args: CodexAppServerArgs) -> Result<()> {
    if !args.internal_codex_shim {
        bail!("the internal app-server command may only be launched by `bt acp codex --trace`");
    }
    let mut agent_args = vec![OsString::from("app-server")];
    agent_args.extend(args.args);
    bt_daemon::run_trace(
        bt_daemon::TraceArgs {
            command: bt_daemon::TraceCommand::Run(bt_daemon::RunArgs {
                source: bt_daemon::RunSource::Codex,
                agent_args,
            }),
        },
        crate::trace_host::context(base),
    )
    .await
}

async fn serve_connection(stream: TcpStream, launch: Arc<AgentLaunch>) -> Result<()> {
    let connection_id = uuid::Uuid::new_v4().to_string();
    let response_connection_id = connection_id.clone();
    let socket = accept_hdr_async(stream, move |request: &Request, mut response: Response| {
        if request.uri().path() != ACP_PATH {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                format!("ACP WebSocket endpoint is {ACP_PATH}"),
            ));
        }
        response.headers_mut().insert(
            HeaderName::from_static(ACP_CONNECTION_ID_HEADER),
            HeaderValue::from_str(&response_connection_id)
                .expect("UUID connection id is a valid header value"),
        );
        Ok(response)
    })
    .await
    .context("ACP WebSocket handshake failed")?;

    let mut child = launch.spawn()?;
    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("ACP agent stdin was not piped"))?;
    let child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("ACP agent stdout was not piped"))?;
    let mut lines = BufReader::new(child_stdout).lines();
    let (mut writer, mut reader) = socket.split();
    let mut initialized = false;

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line.context("failed to read ACP agent output")? {
                    Some(line) if line.trim().is_empty() => {}
                    Some(line) => {
                        serde_json::from_str::<Value>(&line)
                            .context("ACP agent emitted invalid JSON-RPC")?;
                        writer.send(Message::Text(line.into())).await?;
                    }
                    None => break,
                }
            }
            message = reader.next() => {
                let Some(message) = message else {
                    break;
                };
                match message? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text)
                            .context("ACP client sent invalid JSON-RPC")?;
                        if !initialized {
                            if value.get("method").and_then(Value::as_str) != Some("initialize") {
                                let id = value.get("id").cloned().unwrap_or(Value::Null);
                                writer.send(Message::Text(json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": {
                                        "code": -32002,
                                        "message": "initialize must be the first ACP message",
                                    },
                                }).to_string().into())).await?;
                                break;
                            }
                            initialized = true;
                        }
                        child_stdin.write_all(text.as_bytes()).await?;
                        child_stdin.write_all(b"\n").await?;
                        child_stdin.flush().await?;
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }

    drop(child_stdin);
    let _ = child.start_kill();
    let _ = child.wait().await;
    let _ = connection_id;
    Ok(())
}

fn error_response(status: StatusCode, body: String) -> ErrorResponse {
    tokio_tungstenite::tungstenite::http::Response::builder()
        .status(status)
        .body(Some(body))
        .expect("static ACP error response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configured_adapter_must_exist() {
        let error = resolve_codex_adapter(
            OsStr::new("node"),
            Some(Path::new("/definitely/missing/codex-acp.js")),
        )
        .await
        .expect_err("missing adapter should fail");
        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn codex_trace_launch_uses_internal_bt_shim() -> Result<()> {
        let adapter = PathBuf::from("/tmp/codex-acp.js");
        let args = CodexArgs {
            no_auth: true,
            listen: "127.0.0.1:5001".parse()?,
            node: OsString::from("node"),
            adapter: Some(adapter.clone()),
            codex: OsString::from("codex"),
            trace: true,
        };
        let launch = codex_launch(&BaseArgs::default(), &args, adapter.clone())?;
        assert_eq!(launch.program, OsString::from("node"));
        assert_eq!(launch.args, vec![adapter.into_os_string()]);
        assert!(launch
            .env
            .iter()
            .any(|(name, value)| name == INTERNAL_CODEX_SHIM_ENV && value == "true"));
        assert!(launch
            .env
            .iter()
            .any(|(name, value)| name == "CODEX_BIN" && value == "codex"));
        Ok(())
    }
}
