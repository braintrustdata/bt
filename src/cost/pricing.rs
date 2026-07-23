use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use serde::Deserialize;

const SUPPORTED_PRICING_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct TokenRates {
    pub input_usd_per_1m_tokens: f64,
    pub cached_input_usd_per_1m_tokens: Option<f64>,
    pub cache_write_usd_per_1m_tokens: Option<f64>,
    pub cache_write_5m_usd_per_1m_tokens: Option<f64>,
    pub cache_write_1h_usd_per_1m_tokens: Option<f64>,
    pub output_usd_per_1m_tokens: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct TokenUsage {
    pub spans: u64,
    pub uncached_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub effective_cache_write_tokens: u64,
    pub split_cache_write_5m_tokens: u64,
    pub split_cache_write_1h_tokens: u64,
    pub fallback_cache_write_tokens: u64,
    pub output_tokens: u64,
}

impl TokenUsage {
    pub fn cost(self, rates: &TokenRates) -> f64 {
        let per_million = 1_000_000.0;
        let input_cost =
            self.uncached_input_tokens as f64 * rates.input_usd_per_1m_tokens / per_million;
        let cached_input_cost = self.cached_input_tokens as f64
            * rates
                .cached_input_usd_per_1m_tokens
                .unwrap_or(rates.input_usd_per_1m_tokens)
            / per_million;
        let generic_cache_write_rate = rates
            .cache_write_usd_per_1m_tokens
            .unwrap_or(rates.input_usd_per_1m_tokens);
        let cache_write_cost = match (
            rates.cache_write_5m_usd_per_1m_tokens,
            rates.cache_write_1h_usd_per_1m_tokens,
        ) {
            (Some(rate_5m), Some(rate_1h)) => {
                (self.split_cache_write_5m_tokens as f64 * rate_5m
                    + self.split_cache_write_1h_tokens as f64 * rate_1h
                    + self.fallback_cache_write_tokens as f64 * generic_cache_write_rate)
                    / per_million
            }
            _ => self.effective_cache_write_tokens as f64 * generic_cache_write_rate / per_million,
        };
        let output_cost = self.output_tokens as f64 * rates.output_usd_per_1m_tokens / per_million;

        input_cost + cached_input_cost + cache_write_cost + output_cost
    }
}

#[derive(Debug, Clone)]
struct PriceInterval {
    effective_from: DateTime<Utc>,
    effective_until: Option<DateTime<Utc>>,
    rates: TokenRates,
}

impl PriceInterval {
    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp >= self.effective_from
            && self
                .effective_until
                .is_none_or(|effective_until| timestamp < effective_until)
    }
}

#[derive(Debug, Clone)]
struct ModelPriceHistory {
    intervals: Vec<PriceInterval>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PriceBook {
    histories: Vec<ModelPriceHistory>,
    model_lookup: HashMap<String, usize>,
}

impl PriceBook {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read pricing file {}", path.display()))?;
        let raw: RawPricingFile = toml::from_str(&contents)
            .with_context(|| format!("failed to parse pricing file {}", path.display()))?;
        Self::from_raw(raw).with_context(|| format!("invalid pricing file {}", path.display()))
    }

    fn from_raw(raw: RawPricingFile) -> Result<Self> {
        if raw.version != SUPPORTED_PRICING_VERSION {
            bail!(
                "unsupported pricing file version {}; expected {}",
                raw.version,
                SUPPORTED_PRICING_VERSION
            );
        }
        if raw.models.is_empty() {
            bail!("pricing file must define at least one model under [models]");
        }

        let mut histories = Vec::with_capacity(raw.models.len());
        let mut model_lookup = HashMap::new();

        for (model_name, raw_model) in raw.models {
            let model_name = model_name.trim();
            if model_name.is_empty() {
                bail!("model names cannot be empty");
            }
            if raw_model.rates.is_empty() {
                bail!("models.{model_name}.rates must contain at least one historical rate");
            }

            let mut parsed_rates = raw_model
                .rates
                .into_iter()
                .enumerate()
                .map(|(index, rate)| parse_rate(model_name, index, rate))
                .collect::<Result<Vec<_>>>()?;
            parsed_rates.sort_by_key(|rate| rate.effective_from);

            for rates in parsed_rates.windows(2) {
                let current = &rates[0];
                let next = &rates[1];
                if current.effective_from == next.effective_from {
                    bail!(
                        "models.{model_name}.rates has duplicate effective_from {}",
                        format_timestamp(current.effective_from)
                    );
                }
                if current
                    .effective_until
                    .is_some_and(|effective_until| effective_until > next.effective_from)
                {
                    bail!(
                        "models.{model_name}.rates has overlapping intervals at {}",
                        format_timestamp(next.effective_from)
                    );
                }
            }

            let mut intervals = Vec::with_capacity(parsed_rates.len());
            for index in 0..parsed_rates.len() {
                let next_start = parsed_rates.get(index + 1).map(|rate| rate.effective_from);
                let rate = &parsed_rates[index];
                intervals.push(PriceInterval {
                    effective_from: rate.effective_from,
                    effective_until: rate.effective_until.or(next_start),
                    rates: rate.rates,
                });
            }

            let history_index = histories.len();
            histories.push(ModelPriceHistory { intervals });
            insert_model_lookup(&mut model_lookup, model_name, history_index)?;
            for alias in raw_model.aliases {
                let alias = alias.trim();
                if alias.is_empty() {
                    bail!("models.{model_name}.aliases cannot contain an empty name");
                }
                insert_model_lookup(&mut model_lookup, alias, history_index)?;
            }
        }

        Ok(Self {
            histories,
            model_lookup,
        })
    }

    pub fn rate_at(&self, model: &str, timestamp: DateTime<Utc>) -> Option<&TokenRates> {
        let history_index = self.model_lookup.get(&normalize_model_name(model))?;
        self.histories[*history_index]
            .intervals
            .iter()
            .find(|interval| interval.contains(timestamp))
            .map(|interval| &interval.rates)
    }

    pub fn boundaries_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Vec<DateTime<Utc>> {
        let mut boundaries = Vec::new();
        for history in &self.histories {
            for interval in &history.intervals {
                if interval.effective_from > since && interval.effective_from < until {
                    boundaries.push(interval.effective_from);
                }
                if let Some(effective_until) = interval.effective_until {
                    if effective_until > since && effective_until < until {
                        boundaries.push(effective_until);
                    }
                }
            }
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        boundaries
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPricingFile {
    version: u32,
    models: BTreeMap<String, RawModelPricing>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawModelPricing {
    #[serde(default)]
    aliases: Vec<String>,
    rates: Vec<RawTokenRates>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTokenRates {
    effective_from: String,
    effective_until: Option<String>,
    input_usd_per_1m_tokens: f64,
    cached_input_usd_per_1m_tokens: Option<f64>,
    cache_write_usd_per_1m_tokens: Option<f64>,
    cache_write_5m_usd_per_1m_tokens: Option<f64>,
    cache_write_1h_usd_per_1m_tokens: Option<f64>,
    output_usd_per_1m_tokens: f64,
}

struct ParsedTokenRates {
    effective_from: DateTime<Utc>,
    effective_until: Option<DateTime<Utc>>,
    rates: TokenRates,
}

fn parse_rate(model_name: &str, index: usize, raw: RawTokenRates) -> Result<ParsedTokenRates> {
    let path = format!("models.{model_name}.rates[{index}]");
    let effective_from = parse_timestamp(&raw.effective_from)
        .with_context(|| format!("{path}.effective_from is invalid"))?;
    let effective_until = raw
        .effective_until
        .as_deref()
        .map(parse_timestamp)
        .transpose()
        .with_context(|| format!("{path}.effective_until is invalid"))?;
    if effective_until.is_some_and(|until| until <= effective_from) {
        bail!("{path}.effective_until must be later than effective_from");
    }

    let rates = TokenRates {
        input_usd_per_1m_tokens: raw.input_usd_per_1m_tokens,
        cached_input_usd_per_1m_tokens: raw.cached_input_usd_per_1m_tokens,
        cache_write_usd_per_1m_tokens: raw.cache_write_usd_per_1m_tokens,
        cache_write_5m_usd_per_1m_tokens: raw.cache_write_5m_usd_per_1m_tokens,
        cache_write_1h_usd_per_1m_tokens: raw.cache_write_1h_usd_per_1m_tokens,
        output_usd_per_1m_tokens: raw.output_usd_per_1m_tokens,
    };
    validate_rates(&path, &rates)?;

    Ok(ParsedTokenRates {
        effective_from,
        effective_until,
        rates,
    })
}

fn validate_rates(path: &str, rates: &TokenRates) -> Result<()> {
    let values = [
        (
            "input_usd_per_1m_tokens",
            Some(rates.input_usd_per_1m_tokens),
        ),
        (
            "cached_input_usd_per_1m_tokens",
            rates.cached_input_usd_per_1m_tokens,
        ),
        (
            "cache_write_usd_per_1m_tokens",
            rates.cache_write_usd_per_1m_tokens,
        ),
        (
            "cache_write_5m_usd_per_1m_tokens",
            rates.cache_write_5m_usd_per_1m_tokens,
        ),
        (
            "cache_write_1h_usd_per_1m_tokens",
            rates.cache_write_1h_usd_per_1m_tokens,
        ),
        (
            "output_usd_per_1m_tokens",
            Some(rates.output_usd_per_1m_tokens),
        ),
    ];
    for (name, value) in values {
        if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
            bail!("{path}.{name} must be a finite, non-negative number");
        }
    }

    if rates.cache_write_5m_usd_per_1m_tokens.is_some()
        != rates.cache_write_1h_usd_per_1m_tokens.is_some()
    {
        bail!(
            "{path} must specify both cache_write_5m_usd_per_1m_tokens and cache_write_1h_usd_per_1m_tokens, or neither"
        );
    }
    Ok(())
}

fn insert_model_lookup(
    model_lookup: &mut HashMap<String, usize>,
    model_name: &str,
    history_index: usize,
) -> Result<()> {
    let normalized = normalize_model_name(model_name);
    if model_lookup.insert(normalized, history_index).is_some() {
        bail!("model name or alias '{model_name}' is defined more than once");
    }
    Ok(())
}

fn normalize_model_name(model: &str) -> String {
    model.trim().to_ascii_lowercase()
}

pub(super) fn parse_timestamp(value: &str) -> Result<DateTime<Utc>> {
    parse_timestamp_in_offset(
        value,
        FixedOffset::east_opt(0).expect("UTC is a valid offset"),
    )
}

/// Parse a timestamp. RFC 3339 values keep their explicit offset; a bare
/// `YYYY-MM-DD` is interpreted at midnight in `offset` (so `--since 2026-07-20`
/// with UTC-7 means `2026-07-20T00:00:00-07:00`).
pub(super) fn parse_timestamp_in_offset(value: &str, offset: FixedOffset) -> Result<DateTime<Utc>> {
    let value = value.trim();
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let naive = date.and_hms_opt(0, 0, 0).expect("midnight is a valid time");
        return offset
            .from_local_datetime(&naive)
            .single()
            .map(|local| local.with_timezone(&Utc))
            .context("ambiguous local timestamp for the given time zone");
    }
    bail!("expected RFC 3339 timestamp or YYYY-MM-DD date, got '{value}'")
}

pub(super) fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// Format an instant for display in the given offset, without any time-zone
/// suffix (no `Z` or `+HH:MM`).
pub(super) fn format_timestamp_in_offset(timestamp: DateTime<Utc>, offset: FixedOffset) -> String {
    timestamp
        .with_timezone(&offset)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}

/// Reformat a UTC bucket key (RFC 3339, e.g. `2026-07-22T18:00:00Z`) in
/// `offset`, without a time-zone suffix. Returns `None` if it isn't a timestamp.
pub(super) fn reoffset_local(raw: &str, offset: FixedOffset) -> Option<String> {
    let instant = DateTime::parse_from_rfc3339(raw).ok()?;
    Some(
        instant
            .with_timezone(&offset)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_book(input: &str) -> Result<PriceBook> {
        let raw: RawPricingFile = toml::from_str(input)?;
        PriceBook::from_raw(raw)
    }

    #[test]
    fn historical_rates_apply_at_inclusive_start_and_exclusive_end() {
        let book = parse_book(
            r#"
version = 1

[models."test-model"]
aliases = ["test-deployment"]

[[models."test-model".rates]]
effective_from = "2025-01-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0

[[models."test-model".rates]]
effective_from = "2025-06-01T00:00:00Z"
input_usd_per_1m_tokens = 3.0
output_usd_per_1m_tokens = 4.0
"#,
        )
        .expect("valid pricing file");

        let before_change = parse_timestamp("2025-05-31T23:59:59Z").unwrap();
        let at_change = parse_timestamp("2025-06-01T00:00:00Z").unwrap();
        assert_eq!(
            book.rate_at("test-model", before_change)
                .unwrap()
                .input_usd_per_1m_tokens,
            1.0
        );
        assert_eq!(
            book.rate_at("TEST-DEPLOYMENT", at_change)
                .unwrap()
                .input_usd_per_1m_tokens,
            3.0
        );
    }

    #[test]
    fn explicit_end_can_leave_an_unpriced_gap() {
        let book = parse_book(
            r#"
version = 1

[models."test-model"]
[[models."test-model".rates]]
effective_from = "2025-01-01"
effective_until = "2025-02-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0

[[models."test-model".rates]]
effective_from = "2025-03-01"
input_usd_per_1m_tokens = 3.0
output_usd_per_1m_tokens = 4.0
"#,
        )
        .expect("valid pricing file");

        assert!(book
            .rate_at("test-model", parse_timestamp("2025-02-15").unwrap())
            .is_none());
    }

    #[test]
    fn rejects_overlapping_intervals() {
        let error = parse_book(
            r#"
version = 1

[models."test-model"]
[[models."test-model".rates]]
effective_from = "2025-01-01"
effective_until = "2025-07-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0

[[models."test-model".rates]]
effective_from = "2025-06-01"
input_usd_per_1m_tokens = 3.0
output_usd_per_1m_tokens = 4.0
"#,
        )
        .expect_err("overlap must fail");
        assert!(
            error.to_string().contains("overlapping intervals"),
            "{error}"
        );
    }

    #[test]
    fn rejects_only_one_ttl_cache_write_rate() {
        let error = parse_book(
            r#"
version = 1

[models."test-model"]
[[models."test-model".rates]]
effective_from = "2025-01-01"
input_usd_per_1m_tokens = 1.0
cache_write_5m_usd_per_1m_tokens = 1.25
output_usd_per_1m_tokens = 2.0
"#,
        )
        .expect_err("incomplete TTL rates must fail");
        assert!(error.to_string().contains("must specify both"), "{error}");
    }

    #[test]
    fn computes_generic_and_split_cache_costs() {
        let usage = TokenUsage {
            spans: 1,
            uncached_input_tokens: 1_000_000,
            cached_input_tokens: 1_000_000,
            effective_cache_write_tokens: 3_000_000,
            split_cache_write_5m_tokens: 1_000_000,
            split_cache_write_1h_tokens: 1_000_000,
            fallback_cache_write_tokens: 1_000_000,
            output_tokens: 1_000_000,
        };
        let split_rates = TokenRates {
            input_usd_per_1m_tokens: 1.0,
            cached_input_usd_per_1m_tokens: Some(0.1),
            cache_write_usd_per_1m_tokens: Some(1.25),
            cache_write_5m_usd_per_1m_tokens: Some(1.25),
            cache_write_1h_usd_per_1m_tokens: Some(2.0),
            output_usd_per_1m_tokens: 3.0,
        };
        assert_eq!(usage.cost(&split_rates), 8.6);

        let generic_rates = TokenRates {
            cache_write_5m_usd_per_1m_tokens: None,
            cache_write_1h_usd_per_1m_tokens: None,
            ..split_rates
        };
        assert_eq!(usage.cost(&generic_rates), 7.85);
    }

    #[test]
    fn boundaries_include_historical_changes_and_explicit_ends() {
        let book = parse_book(
            r#"
version = 1

[models."test-model"]
[[models."test-model".rates]]
effective_from = "2025-01-01"
effective_until = "2025-03-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0

[[models."test-model".rates]]
effective_from = "2025-04-01"
input_usd_per_1m_tokens = 3.0
output_usd_per_1m_tokens = 4.0
"#,
        )
        .unwrap();
        let boundaries = book.boundaries_between(
            parse_timestamp("2025-02-01").unwrap(),
            parse_timestamp("2025-05-01").unwrap(),
        );
        assert_eq!(
            boundaries,
            vec![
                parse_timestamp("2025-03-01").unwrap(),
                parse_timestamp("2025-04-01").unwrap()
            ]
        );
    }
}
