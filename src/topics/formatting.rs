use chrono::{DateTime, SecondsFormat, Utc};

pub(crate) fn format_project_header(
    project_name: &str,
    project_id: &str,
    org_name: &str,
) -> String {
    format!("Project: {org_name} / {project_name} ({project_id})")
}

pub(crate) fn format_duration_compact(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "n/a".to_string();
    };

    let units = [
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ];
    for (suffix, scale) in units {
        if seconds >= scale && seconds % scale == 0 {
            return format!("{}{}", seconds / scale, suffix);
        }
    }
    format!("{seconds}s")
}

pub(crate) fn format_timestamp_with_relative(value: &str) -> String {
    let Ok(parsed) = DateTime::parse_from_rfc3339(value) else {
        return value.to_string();
    };
    format_datetime_with_relative(parsed.with_timezone(&Utc))
}

pub(crate) fn format_datetime_with_relative(timestamp: DateTime<Utc>) -> String {
    format!(
        "{} ({})",
        timestamp.to_rfc3339_opts(SecondsFormat::Secs, true),
        relative_time(timestamp)
    )
}

pub(crate) fn format_relative_duration_seconds(
    delta_seconds: i64,
    include_direction: bool,
) -> String {
    let absolute_seconds = delta_seconds.abs();
    if absolute_seconds < 5 {
        return if include_direction {
            "now".to_string()
        } else {
            "0s".to_string()
        };
    }

    let units = [
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ];
    for (suffix, scale) in units {
        if absolute_seconds < scale {
            continue;
        }
        let rounded = absolute_seconds / scale;
        if !include_direction {
            return format!("{rounded}{suffix}");
        }
        if delta_seconds > 0 {
            return format!("in {rounded}{suffix}");
        }
        return format!("{rounded}{suffix} ago");
    }
    "0s".to_string()
}

pub(crate) fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);

    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }

    out.chars().rev().collect()
}

fn relative_time(timestamp: DateTime<Utc>) -> String {
    let delta = timestamp.signed_duration_since(Utc::now());
    let seconds = delta.num_seconds();

    if seconds.abs() < 5 {
        return "now".to_string();
    }

    let text = human_interval(seconds.abs());
    if seconds >= 0 {
        format!("in {text}")
    } else {
        format!("{text} ago")
    }
}

fn human_interval(seconds: i64) -> String {
    let units = [("d", 24 * 60 * 60), ("h", 60 * 60), ("m", 60), ("s", 1)];
    let mut remaining = seconds;
    let mut parts = Vec::new();

    for (suffix, scale) in units {
        if remaining < scale {
            continue;
        }
        let value = remaining / scale;
        remaining %= scale;
        parts.push(format!("{value}{suffix}"));
        if parts.len() == 2 {
            break;
        }
    }

    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}
