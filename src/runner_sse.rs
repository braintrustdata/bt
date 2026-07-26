use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use std::future::Future;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::atomic::AtomicU64;
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(not(unix))]
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

#[cfg(unix)]
const SOCKET_BIND_MAX_ATTEMPTS: u8 = 16;
#[cfg(unix)]
static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) enum SseListener {
    #[cfg(unix)]
    Unix(UnixListener),
    #[cfg(not(unix))]
    Tcp(TcpListener),
}

pub(crate) struct SseListenerGuard {
    endpoint: SseEndpoint,
    #[cfg(unix)]
    _socket_cleanup_guard: SocketCleanupGuard,
}

enum SseEndpoint {
    #[cfg(unix)]
    Unix(PathBuf),
    #[cfg(not(unix))]
    Tcp(std::net::SocketAddr),
}

#[cfg(unix)]
struct SocketCleanupGuard {
    path: PathBuf,
}

#[cfg(unix)]
impl SocketCleanupGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[cfg(unix)]
impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl SseListenerGuard {
    pub(crate) fn env<'a>(&self, socket_var: &'a str, addr_var: &'a str) -> (&'a str, String) {
        #[cfg(unix)]
        let _ = addr_var;
        #[cfg(not(unix))]
        let _ = socket_var;
        match &self.endpoint {
            #[cfg(unix)]
            SseEndpoint::Unix(path) => (socket_var, path.to_string_lossy().to_string()),
            #[cfg(not(unix))]
            SseEndpoint::Tcp(addr) => (addr_var, addr.to_string()),
        }
    }
}

pub(crate) fn bind_sse_listener(prefix: &str) -> Result<(SseListener, SseListenerGuard)> {
    #[cfg(unix)]
    {
        bind_unix_sse_listener(prefix)
    }

    #[cfg(not(unix))]
    {
        let _ = prefix;
        bind_tcp_sse_listener()
    }
}

#[cfg(unix)]
fn bind_unix_sse_listener(prefix: &str) -> Result<(SseListener, SseListenerGuard)> {
    let mut last_bind_err: Option<std::io::Error> = None;
    for _ in 0..SOCKET_BIND_MAX_ATTEMPTS {
        let socket_path = build_sse_socket_path(prefix)?;
        let socket_cleanup_guard = SocketCleanupGuard::new(socket_path.clone());
        let _ = std::fs::remove_file(&socket_path);
        match UnixListener::bind(&socket_path) {
            Ok(listener) => {
                return Ok((
                    SseListener::Unix(listener),
                    SseListenerGuard {
                        endpoint: SseEndpoint::Unix(socket_path),
                        _socket_cleanup_guard: socket_cleanup_guard,
                    },
                ))
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::AddrInUse
                ) =>
            {
                last_bind_err = Some(err);
                continue;
            }
            Err(err) => {
                return Err(err).context("failed to bind SSE unix socket");
            }
        }
    }
    let err = last_bind_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "failed to allocate a unique SSE socket path",
        )
    });
    Err(err).context(format!(
        "failed to bind SSE unix socket after {SOCKET_BIND_MAX_ATTEMPTS} attempts"
    ))
}

#[cfg(not(unix))]
fn bind_tcp_sse_listener() -> Result<(SseListener, SseListenerGuard)> {
    let std_listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).context("failed to bind SSE TCP listener")?;
    std_listener
        .set_nonblocking(true)
        .context("failed to configure SSE TCP listener")?;
    let addr = std_listener
        .local_addr()
        .context("failed to read SSE TCP listener address")?;
    let listener =
        TcpListener::from_std(std_listener).context("failed to create SSE TCP listener")?;
    Ok((
        SseListener::Tcp(listener),
        SseListenerGuard {
            endpoint: SseEndpoint::Tcp(addr),
        },
    ))
}

#[cfg(unix)]
pub(crate) fn build_sse_socket_path(prefix: &str) -> Result<PathBuf> {
    let pid = std::process::id();
    let serial = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system time")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!("{prefix}-{pid}-{now}-{serial}.sock")))
}

pub(crate) async fn accept_and_read_sse_stream<C, F>(
    listener: SseListener,
    on_connected: C,
    on_event: F,
) -> Result<()>
where
    C: FnOnce(),
    F: FnMut(Option<String>, String),
{
    match listener {
        #[cfg(unix)]
        SseListener::Unix(listener) => {
            let (stream, _) = listener
                .accept()
                .await
                .context("failed to accept SSE unix socket connection")?;
            on_connected();
            read_sse_stream(stream, on_event).await
        }
        #[cfg(not(unix))]
        SseListener::Tcp(listener) => {
            let (stream, _) = listener
                .accept()
                .await
                .context("failed to accept SSE TCP connection")?;
            on_connected();
            read_sse_stream(stream, on_event).await
        }
    }
}

pub(crate) async fn forward_stream<T, F>(
    stream: T,
    name: &'static str,
    mut on_line: F,
) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
    F: FnMut(&'static str, String),
{
    let mut lines = BufReader::new(stream).lines();
    while let Some(line) = lines.next_line().await? {
        on_line(name, line);
    }
    Ok(())
}

pub(crate) async fn read_sse_stream<T, F>(stream: T, mut on_event: F) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
    F: FnMut(Option<String>, String),
{
    let mut lines = BufReader::new(stream).lines();
    let mut event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            if event.is_some() || !data_lines.is_empty() {
                let data = data_lines.join("\n");
                on_event(event.take(), data);
                data_lines.clear();
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        }
    }

    if event.is_some() || !data_lines.is_empty() {
        let data = data_lines.join("\n");
        on_event(event.take(), data);
    }

    Ok(())
}

pub(crate) async fn drive_runner_events<E, F>(
    mut rx: mpsc::UnboundedReceiver<E>,
    mut wait: Pin<Box<dyn Future<Output = Result<ExitStatus>> + Send + '_>>,
    sse_task: &mut tokio::task::JoinHandle<()>,
    sse_connected: &AtomicBool,
    missing_status_message: &'static str,
    mut on_event: F,
) -> Result<ExitStatus>
where
    F: FnMut(E),
{
    let mut status: Option<ExitStatus> = None;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => on_event(event),
                    None => {
                        if status.is_none() {
                            status = Some(wait.as_mut().await?);
                            abort_unconnected_sse(sse_task, sse_connected);
                        }
                        break;
                    }
                }
            }
            wait_result = wait.as_mut(), if status.is_none() => {
                status = Some(wait_result?);
                abort_unconnected_sse(sse_task, sse_connected);
            }
        }

        if status.is_some() && rx.is_closed() {
            break;
        }
    }

    let _ = sse_task.await;
    status.context(missing_status_message)
}

fn abort_unconnected_sse(sse_task: &mut tokio::task::JoinHandle<()>, sse_connected: &AtomicBool) {
    if !sse_connected.load(Ordering::Relaxed) {
        sse_task.abort();
    }
}
