use anyhow::{bail, Context, Result};

pub fn parse_duration_to_seconds(input: &str) -> Result<u64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("duration cannot be empty");
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Ok(seconds);
    }

    let suffix = trimmed.chars().last().filter(|ch| ch.is_ascii_alphabetic());
    let (num_str, unit) = match suffix {
        Some(unit) => (&trimmed[..trimmed.len() - unit.len_utf8()], unit),
        None => (trimmed, 's'),
    };
    let value: u64 = num_str
        .trim()
        .parse()
        .with_context(|| format!("invalid duration '{input}'"))?;
    let multiplier = match unit.to_ascii_lowercase() {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 60 * 60 * 24,
        _ => bail!("invalid duration '{input}'. expected suffix s/m/h/d"),
    };
    Ok(value.saturating_mul(multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_units() {
        assert_eq!(parse_duration_to_seconds("90").expect("seconds"), 90);
        assert_eq!(parse_duration_to_seconds("15m").expect("minutes"), 900);
        assert_eq!(parse_duration_to_seconds("2h").expect("hours"), 7_200);
        assert_eq!(parse_duration_to_seconds("1d").expect("days"), 86_400);
    }

    #[test]
    fn rejects_non_ascii_suffix_without_panicking() {
        for input in ["1–", "1é", "1🙂"] {
            let err = parse_duration_to_seconds(input).expect_err("invalid unicode suffix");
            assert!(err.to_string().contains("invalid duration"));
        }
    }
}
