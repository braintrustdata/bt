use std::str::FromStr;
use std::sync::LazyLock;

use dialoguer::console;
use syntect::easy::ScopeRegionIterator;
use syntect::highlighting::ScopeSelector;
use syntect::parsing::{ParseState, ScopeStack, SyntaxSet};

static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);

static COMMENT: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("comment").unwrap());
static STRING: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("string").unwrap());
static CONSTANT_NUMERIC: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("constant.numeric").unwrap());
static CONSTANT_LANGUAGE: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("constant.language").unwrap());
static ENTITY_NAME_FUNCTION: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("entity.name.function").unwrap());
static SUPPORT_FUNCTION: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("support.function").unwrap());
static KEYWORD_OPERATOR: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("keyword.operator").unwrap());
static KEYWORD: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("keyword").unwrap());
static STORAGE: LazyLock<ScopeSelector> =
    LazyLock::new(|| ScopeSelector::from_str("storage").unwrap());

fn runtime_to_extension(runtime: &str) -> &str {
    match runtime {
        "python" => "py",
        "node" => "js",
        "typescript" => "ts",
        _ => runtime,
    }
}

fn style_token(token: &str, scope_stack: &ScopeStack) -> String {
    if token.is_empty() {
        return String::new();
    }

    let scopes = scope_stack.as_slice();

    if COMMENT.does_match(scopes).is_some() {
        return format!("{}", console::style(token).dim());
    }
    if STRING.does_match(scopes).is_some() {
        return format!("{}", console::style(token).green());
    }
    if CONSTANT_NUMERIC.does_match(scopes).is_some() {
        return format!("{}", console::style(token).magenta());
    }
    if CONSTANT_LANGUAGE.does_match(scopes).is_some() {
        return format!("{}", console::style(token).cyan().bold());
    }
    if ENTITY_NAME_FUNCTION.does_match(scopes).is_some() {
        return format!("{}", console::style(token).yellow());
    }
    if SUPPORT_FUNCTION.does_match(scopes).is_some() {
        return format!("{}", console::style(token).yellow());
    }
    if KEYWORD_OPERATOR.does_match(scopes).is_some() {
        return format!("{}", console::style(token).red());
    }
    if KEYWORD.does_match(scopes).is_some() {
        return format!("{}", console::style(token).cyan().bold());
    }
    if STORAGE.does_match(scopes).is_some() {
        return format!("{}", console::style(token).cyan());
    }

    token.to_string()
}

pub fn highlight_code(code: &str, language_hint: &str) -> Option<Vec<String>> {
    let ps = &*SYNTAX_SET;
    let ext = runtime_to_extension(language_hint);
    let syntax = ps.find_syntax_by_extension(ext)?;
    let mut state = ParseState::new(syntax);

    let mut result = Vec::new();
    let mut scope_stack = ScopeStack::new();
    for line in code.lines() {
        let line_nl = format!("{line}\n");
        let ops = state.parse_line(&line_nl, ps).ok()?;

        let mut highlighted = String::new();
        for (token, op) in ScopeRegionIterator::new(&ops, &line_nl) {
            scope_stack.apply(op).ok()?;
            let token = token.trim_end_matches('\n');
            highlighted.push_str(&style_token(token, &scope_stack));
        }
        result.push(highlighted);
    }
    Some(result)
}
