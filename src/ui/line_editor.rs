use std::io::{self, IsTerminal, Write};

use anyhow::{Context, Result};
use crossterm::{
    cursor::MoveToColumn,
    event::{
        read as read_terminal_event, Event as TerminalEvent, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    queue,
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use unicode_width::UnicodeWidthStr;

struct LineEditor {
    input: String,
    cursor: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    draft: Option<String>,
}

impl LineEditor {
    fn new(history: Vec<String>) -> Self {
        Self {
            input: String::new(),
            cursor: 0,
            history,
            history_index: None,
            draft: None,
        }
    }

    fn input(&self) -> &str {
        &self.input
    }

    fn cursor_prefix(&self) -> &str {
        &self.input[..self.cursor]
    }

    fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
        self.history_index = None;
        self.draft = None;
    }

    fn add_history(&mut self, value: &str) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        if self.history.last().map(String::as_str) != Some(value) {
            self.history.push(value.to_string());
        }
        self.history_index = None;
        self.draft = None;
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Left => self.move_left(),
            KeyCode::Right => self.move_right(),
            KeyCode::Home => self.move_home(),
            KeyCode::End => self.move_end(),
            KeyCode::Up => self.history_prev(),
            KeyCode::Down => self.history_next(),
            KeyCode::Char(ch)
                if !key.modifiers.contains(KeyModifiers::CONTROL)
                    && !key.modifiers.contains(KeyModifiers::ALT) =>
            {
                self.insert_char(ch);
            }
            _ => return false,
        }
        true
    }

    fn insert_char(&mut self, ch: char) {
        self.clear_history_selection();
        self.input.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.clear_history_selection();
        let new_cursor = prev_char_boundary(&self.input, self.cursor);
        self.input.replace_range(new_cursor..self.cursor, "");
        self.cursor = new_cursor;
    }

    fn delete(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        self.clear_history_selection();
        let next_cursor = next_char_boundary(&self.input, self.cursor);
        self.input.replace_range(self.cursor..next_cursor, "");
    }

    fn move_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor = prev_char_boundary(&self.input, self.cursor);
    }

    fn move_right(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        self.cursor = next_char_boundary(&self.input, self.cursor);
    }

    fn move_home(&mut self) {
        self.cursor = 0;
    }

    fn move_end(&mut self) {
        self.cursor = self.input.len();
    }

    fn history_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next_index = match self.history_index {
            None => {
                self.draft = Some(self.input.clone());
                self.history.len().saturating_sub(1)
            }
            Some(0) => 0,
            Some(idx) => idx - 1,
        };
        self.history_index = Some(next_index);
        self.input = self.history[next_index].clone();
        self.cursor = self.input.len();
    }

    fn history_next(&mut self) {
        let Some(idx) = self.history_index else {
            return;
        };
        let next_index = idx + 1;
        if next_index >= self.history.len() {
            self.history_index = None;
            self.input = self.draft.take().unwrap_or_default();
            self.cursor = self.input.len();
            return;
        }
        self.history_index = Some(next_index);
        self.input = self.history[next_index].clone();
        self.cursor = self.input.len();
    }

    fn clear_history_selection(&mut self) {
        if self.history_index.take().is_some() {
            self.draft = None;
        }
    }
}

pub struct LinePrompt {
    editor: LineEditor,
}

impl LinePrompt {
    pub fn new(history: Vec<String>) -> Self {
        Self {
            editor: LineEditor::new(history),
        }
    }

    pub fn add_history(&mut self, value: &str) {
        self.editor.add_history(value);
    }

    pub fn read_line(&mut self, prompt: &str, prompt_width: usize) -> Result<Option<String>> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            print!("{prompt}");
            io::stdout().flush()?;
            let mut input = String::new();
            if io::stdin().read_line(&mut input)? == 0 {
                println!();
                return Ok(None);
            }
            return Ok(Some(input.trim_end_matches(['\r', '\n']).to_string()));
        }

        let _raw_mode = RawModeGuard::enable()?;
        render_prompt_line(prompt, prompt_width, &self.editor)?;

        loop {
            let TerminalEvent::Key(key) = read_terminal_event()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    finish_prompt_line()?;
                    return Ok(None);
                }
                KeyCode::Char('d')
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && self.editor.input().is_empty() =>
                {
                    finish_prompt_line()?;
                    return Ok(None);
                }
                KeyCode::Char('j') | KeyCode::Char('m')
                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    return finish_with_input(&mut self.editor);
                }
                KeyCode::Enter => return finish_with_input(&mut self.editor),
                _ => {
                    self.editor.handle_key(key);
                }
            }
            render_prompt_line(prompt, prompt_width, &self.editor)?;
        }
    }
}

struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> Result<Self> {
        enable_raw_mode().context("failed to enable terminal raw mode")?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

fn render_prompt_line(prompt: &str, prompt_width: usize, editor: &LineEditor) -> Result<()> {
    let cursor_col = prompt_width + UnicodeWidthStr::width(editor.cursor_prefix());
    let mut stdout = io::stdout();
    queue!(stdout, MoveToColumn(0), Clear(ClearType::CurrentLine))?;
    write!(stdout, "{}{}", prompt, editor.input())?;
    queue!(
        stdout,
        MoveToColumn(cursor_col.min(u16::MAX as usize) as u16)
    )?;
    stdout.flush()?;
    Ok(())
}

fn finish_with_input(editor: &mut LineEditor) -> Result<Option<String>> {
    let input = editor.input().to_string();
    editor.clear_input();
    finish_prompt_line()?;
    Ok(Some(input))
}

fn finish_prompt_line() -> Result<()> {
    let mut stdout = io::stdout();
    write!(stdout, "\r\n")?;
    stdout.flush()?;
    Ok(())
}

fn prev_char_boundary(s: &str, idx: usize) -> usize {
    s[..idx].char_indices().last().map(|(i, _)| i).unwrap_or(0)
}

fn next_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut iter = s[idx..].char_indices();
    iter.next();
    iter.next().map(|(i, _)| idx + i).unwrap_or_else(|| s.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_editor_history_preserves_draft() {
        let mut editor = LineEditor::new(vec!["first".to_string(), "second".to_string()]);
        editor.insert_char('d');
        editor.insert_char('r');
        editor.insert_char('a');
        editor.insert_char('f');
        editor.insert_char('t');

        editor.history_prev();
        assert_eq!(editor.input(), "second");

        editor.history_prev();
        assert_eq!(editor.input(), "first");

        editor.history_next();
        assert_eq!(editor.input(), "second");

        editor.history_next();
        assert_eq!(editor.input(), "draft");
    }

    #[test]
    fn line_editor_handles_utf8_cursor_movement() {
        let mut editor = LineEditor::new(Vec::new());
        editor.insert_char('a');
        editor.insert_char('é');
        editor.insert_char('b');
        editor.move_left();
        editor.backspace();

        assert_eq!(editor.input(), "ab");
    }
}
