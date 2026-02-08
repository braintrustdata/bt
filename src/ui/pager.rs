use std::io::{self, IsTerminal, Write};
use std::process::{Command, Stdio};

pub fn print_with_pager(output: &str) -> io::Result<()> {
    let stdout = io::stdout();
    if !stdout.is_terminal() {
        println!("{output}");
        return Ok(());
    }

    let (_, term_height) = crossterm::terminal::size().unwrap_or((80, 24));
    let line_count = output.lines().count();
    if line_count <= term_height as usize {
        println!("{output}");
        return Ok(());
    }

    let pager = std::env::var("PAGER")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "less -R".into());

    let mut parts = pager.split_whitespace();
    let cmd = parts.next().unwrap_or("less");
    let args: Vec<&str> = parts.collect();

    let mut child = match Command::new(cmd).args(args).stdin(Stdio::piped()).spawn() {
        Ok(c) => c,
        Err(_) => {
            println!("{output}");
            return Ok(());
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = writeln!(stdin, "{output}");
    }

    let _ = child.wait();
    Ok(())
}
