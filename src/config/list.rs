use anyhow::Result;
use serde_json::{Map, Value};

use crate::args::BaseArgs;

pub fn run(base: BaseArgs, global: bool, local: bool, verbose: bool) -> Result<()> {
    if verbose {
        run_verbose(base, global, local)
    } else {
        run_resolved(base, global, local)
    }
}

fn run_resolved(base: BaseArgs, global: bool, local: bool) -> Result<()> {
    let config = if global {
        super::load_global()?
    } else if local {
        super::local_path()
            .map(|p| super::load_file(&p))
            .unwrap_or_default()
    } else {
        super::load()?
    };

    let output = format_resolved(&config, base.json)?;
    if !output.is_empty() {
        if base.json {
            // send json to stdout instead of stderr like we other commands
            // so it can be piped to other tools
            println!("{output}");
        } else {
            eprintln!("{output}");
        }
    }

    Ok(())
}

fn format_resolved(config: &super::Config, json: bool) -> Result<String> {
    let fields = config.non_empty_fields();

    if json {
        let map: Map<String, Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect();
        Ok(serde_json::to_string(&map)?)
    } else {
        Ok(fields
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

fn run_verbose(base: BaseArgs, global: bool, local: bool) -> Result<()> {
    let global_path = super::global_path().ok();
    let local_path = super::local_path();

    let global_cfg = if !local {
        global_path
            .as_ref()
            .map(|p| (p.display().to_string(), super::load_file(p)))
    } else {
        None
    };

    let local_cfg = if !global {
        local_path.as_ref().map(|p| {
            let display_path = std::env::current_dir()
                .ok()
                .and_then(|cwd| pathdiff::diff_paths(p, &cwd))
                .unwrap_or_else(|| p.clone())
                .display()
                .to_string();
            (display_path, super::load_file(p))
        })
    } else {
        None
    };

    let mut sources: Vec<(String, Vec<(&str, &str)>)> = Vec::new();

    if let Some((path, ref cfg)) = global_cfg {
        let fields = cfg.non_empty_fields();
        if !fields.is_empty() {
            sources.push((path, fields));
        }
    }

    if let Some((path, ref cfg)) = local_cfg {
        let fields = cfg.non_empty_fields();
        if !fields.is_empty() {
            sources.push((path, fields));
        }
    }

    let output = format_verbose(&sources, base.json)?;
    if !output.is_empty() {
        println!("{output}");
    }

    Ok(())
}

fn format_verbose(sources: &[(String, Vec<(&str, &str)>)], json: bool) -> Result<String> {
    if json {
        let mut map = Map::new();
        for (path, fields) in sources {
            let o: Map<String, Value> = fields
                .iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
                .collect();
            map.insert(path.clone(), Value::Object(o));
        }
        Ok(serde_json::to_string(&map)?)
    } else {
        let mut parts = Vec::new();
        for (path, fields) in sources {
            let mut group = String::from(path.as_str());
            for (key, value) in fields {
                group.push_str(&format!("\n  {key}: {value}"));
            }
            parts.push(group);
        }
        Ok(parts.join("\n\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn config_with(org: &str, project: &str) -> Config {
        Config {
            org: Some(org.into()),
            project: Some(project.into()),
            ..Default::default()
        }
    }

    // --- format_resolved tests ---

    #[test]
    fn resolved_text_shows_merged() {
        let config = config_with("acme", "widgets");
        let out = format_resolved(&config, false).unwrap();
        assert_eq!(out, "org: acme\nproject: widgets");
    }

    #[test]
    fn resolved_text_empty_config() {
        let out = format_resolved(&Config::default(), false).unwrap();
        assert_eq!(out, "");
    }

    #[test]
    fn resolved_json_flat_object() {
        let config = config_with("acme", "widgets");
        let out = format_resolved(&config, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["org"], "acme");
        assert_eq!(parsed["project"], "widgets");
    }

    #[test]
    fn resolved_json_empty() {
        let out = format_resolved(&Config::default(), true).unwrap();
        assert_eq!(out, "{}");
    }

    // --- format_verbose tests ---

    #[test]
    fn verbose_text_two_sources() {
        let sources: Vec<(String, Vec<(&str, &str)>)> = vec![
            ("~/.bt/config.json".into(), vec![("org", "global-org")]),
            (".bt/config.json".into(), vec![("project", "local-proj")]),
        ];
        let out = format_verbose(&sources, false).unwrap();
        assert_eq!(
            out,
            "~/.bt/config.json\n  org: global-org\n\n.bt/config.json\n  project: local-proj"
        );
    }

    #[test]
    fn verbose_text_single_sources() {
        let sources: Vec<(String, Vec<(&str, &str)>)> = vec![(
            ".bt/config.json".into(),
            vec![("org", "global-org"), ("project", "local-proj")],
        )];
        let out = format_verbose(&sources, false).unwrap();
        assert_eq!(
            out,
            ".bt/config.json\n  org: global-org\n  project: local-proj"
        );
    }

    #[test]
    fn verbose_json_nested_by_path() {
        let sources: Vec<(String, Vec<(&str, &str)>)> = vec![
            ("~/.bt/config.json".into(), vec![("org", "global-org")]),
            (".bt/config.json".into(), vec![("project", "local-proj")]),
        ];
        let out = format_verbose(&sources, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["~/.bt/config.json"]["org"], "global-org");
        assert_eq!(parsed[".bt/config.json"]["project"], "local-proj");
    }

    #[test]
    fn verbose_json_empty() {
        let sources: Vec<(String, Vec<(&str, &str)>)> = vec![];
        let out = format_verbose(&sources, true).unwrap();
        assert_eq!(out, "{}");
    }
}
