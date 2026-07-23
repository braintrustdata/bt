//! Detect terminals that can display inline images and encode a PNG for them.
//!
//! Supports the kitty graphics protocol (kitty, Ghostty, WezTerm) and the
//! iTerm2 inline-image protocol (iTerm2, WezTerm). When neither is available
//! (or stdout is not a terminal) the caller falls back to the ASCII chart.

use std::io::IsTerminal;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::ValueEnum;

/// Whether to upgrade `--plot` to an inline image when the terminal supports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(super) enum ImageMode {
    /// Use inline images when the terminal supports them, else the ASCII chart.
    Auto,
    /// Always draw the ASCII chart.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Kitty,
    ITerm,
}

/// Detect inline-image support from the host terminal.
///
/// This reads terminal capability env vars (`TERM`, `TERM_PROGRAM`,
/// `KITTY_WINDOW_ID`, `LC_TERMINAL`). These describe the terminal emulator, not
/// this tool's configuration, so they cannot be CLI flags — the user-facing
/// toggle is `--image`.
fn detect_protocol() -> Option<Protocol> {
    if !std::io::stdout().is_terminal() {
        return None;
    }
    let term = std::env::var("TERM").unwrap_or_default();
    if std::env::var_os("KITTY_WINDOW_ID").is_some()
        || term.contains("kitty")
        || term.contains("ghostty")
    {
        return Some(Protocol::Kitty);
    }
    let term_program = std::env::var("TERM_PROGRAM").unwrap_or_default();
    let lc_terminal = std::env::var("LC_TERMINAL").unwrap_or_default();
    if term_program == "iTerm.app"
        || term_program == "WezTerm"
        || lc_terminal.eq_ignore_ascii_case("iTerm2")
    {
        return Some(Protocol::ITerm);
    }
    None
}

/// Whether the current terminal supports inline images.
pub(super) fn is_supported() -> bool {
    detect_protocol().is_some()
}

/// Encode `png` as an inline-image escape sequence for the current terminal,
/// scaled to `cols` character columns wide. Returns `None` when no supported
/// terminal is detected.
pub(super) fn inline_image(png: &[u8], cols: u16) -> Option<String> {
    match detect_protocol()? {
        Protocol::ITerm => Some(iterm_escape(png, cols)),
        Protocol::Kitty => Some(kitty_escape(png, cols)),
    }
}

fn iterm_escape(png: &[u8], cols: u16) -> String {
    let data = STANDARD.encode(png);
    format!(
        "\x1b]1337;File=inline=1;size={};width={};preserveAspectRatio=1:{}\x07\n",
        png.len(),
        cols,
        data,
    )
}

fn kitty_escape(png: &[u8], cols: u16) -> String {
    let data = STANDARD.encode(png);
    let bytes = data.as_bytes();
    let chunks: Vec<&[u8]> = bytes.chunks(4096).collect();
    let mut out = String::new();
    for (index, chunk) in chunks.iter().enumerate() {
        let more = u8::from(index + 1 < chunks.len());
        if index == 0 {
            // f=100: PNG payload, a=T: transmit and display, c: fit to columns.
            out.push_str(&format!("\x1b_Gf=100,a=T,c={cols},m={more};"));
        } else {
            out.push_str(&format!("\x1b_Gm={more};"));
        }
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        out.push_str("\x1b\\");
    }
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterm_escape_wraps_base64_png() {
        let escape = iterm_escape(b"\x89PNG\r\n\x1a\n", 80);
        assert!(escape.starts_with("\x1b]1337;File=inline=1;"), "{escape:?}");
        assert!(escape.contains("width=80"), "{escape:?}");
        assert!(escape.contains("size=8"), "{escape:?}");
        assert!(escape.trim_end().ends_with('\x07'), "{escape:?}");
    }

    #[test]
    fn kitty_escape_chunks_and_sets_columns() {
        let escape = kitty_escape(&vec![0u8; 8000], 100);
        assert!(escape.contains("\x1b_Gf=100,a=T,c=100,"), "{escape:?}");
        // Large payloads produce multiple chunks with a final m=0.
        assert!(escape.contains("m=1;"), "{escape:?}");
        assert!(escape.contains("m=0;"), "{escape:?}");
        assert!(escape.matches("\x1b\\").count() >= 2, "{escape:?}");
    }
}
