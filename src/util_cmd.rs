use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;

use crate::args::BaseArgs;

const TOP_BITS: u64 = 0x0DE1u64 << 48;
const MODULUS: u128 = 1u128 << 64;
const COPRIME: u64 = 205_891_132_094_649;
const COPRIME_INVERSE: u64 = 1_522_336_535_492_693_385;

#[derive(Debug, Clone, Args)]
pub struct UtilArgs {
    #[command(subcommand)]
    command: UtilCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum UtilCommands {
    /// Transaction-id conversion utilities
    Xact(XactArgs),
}

#[derive(Debug, Clone, Args)]
struct XactArgs {
    #[command(subcommand)]
    command: XactCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum XactCommands {
    /// Convert a transaction id to a pretty version id
    ToPretty(ToPrettyArgs),
    /// Convert a pretty version id to a transaction id
    FromPretty(FromPrettyArgs),
    /// Convert a transaction id to a timestamp
    ToTime(ToTimeArgs),
    /// Convert a timestamp to a transaction id
    FromTime(FromTimeArgs),
    /// Decode and display transaction id details
    Inspect(InspectArgs),
}

#[derive(Debug, Clone, Args)]
struct ToPrettyArgs {
    /// Decimal transaction id (for example: 1000192656880881099)
    #[arg(value_name = "XACT_ID")]
    xact_id: String,
}

#[derive(Debug, Clone, Args)]
struct FromPrettyArgs {
    /// 16-char hex version id (for example: 81cd05ee665fdfb3)
    #[arg(value_name = "VERSION_ID")]
    version_id: String,
}

#[derive(Debug, Clone, Args)]
struct ToTimeArgs {
    /// Decimal transaction id
    #[arg(value_name = "XACT_ID")]
    xact_id: String,

    /// Output format for non-JSON mode
    #[arg(long, value_enum, default_value_t = TimeOutputFormat::Iso)]
    format: TimeOutputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum, Eq, PartialEq)]
enum TimeOutputFormat {
    Iso,
    Unix,
}

#[derive(Debug, Clone, Args)]
struct FromTimeArgs {
    /// Timestamp value (defaults to now when omitted)
    /// ISO input accepts RFC3339 (2025-01-01T12:34:56Z) and date-only (2025-01-01).
    #[arg(value_name = "TIMESTAMP")]
    timestamp: Option<String>,

    /// Input format
    #[arg(long, value_enum, default_value_t = TimeInputFormat::Iso)]
    input: TimeInputFormat,

    /// Low 16-bit transaction counter value
    #[arg(long, default_value_t = 0)]
    counter: u16,
}

#[derive(Debug, Clone, Copy, ValueEnum, Eq, PartialEq)]
enum TimeInputFormat {
    Iso,
    Unix,
}

#[derive(Debug, Clone, Args)]
struct InspectArgs {
    /// Decimal transaction id or 16-char pretty version id
    #[arg(value_name = "XACT_OR_VERSION")]
    value: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum InputKind {
    XactId,
    PrettyVersionId,
}

#[derive(Debug, Clone, Serialize)]
struct XactInfo {
    input_kind: InputKind,
    xact_id: String,
    pretty_version: String,
    unix_seconds: u64,
    iso_utc: String,
    counter: u16,
}

pub async fn run(base: BaseArgs, args: UtilArgs) -> Result<()> {
    match args.command {
        UtilCommands::Xact(xact) => run_xact(base, xact),
    }
}

fn run_xact(base: BaseArgs, args: XactArgs) -> Result<()> {
    match args.command {
        XactCommands::ToPretty(args) => run_to_pretty(base.json, args),
        XactCommands::FromPretty(args) => run_from_pretty(base.json, args),
        XactCommands::ToTime(args) => run_to_time(base.json, args),
        XactCommands::FromTime(args) => run_from_time(base.json, args),
        XactCommands::Inspect(args) => run_inspect(base.json, args),
    }
}

fn run_to_pretty(json: bool, args: ToPrettyArgs) -> Result<()> {
    let xact = parse_xact_id(&args.xact_id)?;
    let pretty = prettify_xact(xact);
    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "xact_id": xact.to_string(),
                "pretty_version": pretty,
            }))?
        );
    } else {
        println!("{pretty}");
    }
    Ok(())
}

fn run_from_pretty(json: bool, args: FromPrettyArgs) -> Result<()> {
    let xact = load_pretty_xact(&args.version_id)?;
    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "version_id": args.version_id,
                "xact_id": xact,
            }))?
        );
    } else {
        println!("{xact}");
    }
    Ok(())
}

fn run_to_time(json: bool, args: ToTimeArgs) -> Result<()> {
    let xact = parse_xact_id(&args.xact_id)?;
    let unix_seconds = xact_to_unix_seconds(xact);
    let iso = unix_seconds_to_iso(unix_seconds)?;
    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "xact_id": xact.to_string(),
                "unix_seconds": unix_seconds,
                "iso_utc": iso,
            }))?
        );
    } else {
        match args.format {
            TimeOutputFormat::Iso => println!("{iso}"),
            TimeOutputFormat::Unix => println!("{unix_seconds}"),
        }
    }
    Ok(())
}

fn run_from_time(json: bool, args: FromTimeArgs) -> Result<()> {
    let unix_seconds = parse_timestamp_or_now(args.timestamp.as_deref(), args.input)?;
    let xact = build_xact_id(unix_seconds, args.counter);
    let pretty = prettify_xact(xact);
    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "input_timestamp": args.timestamp,
                "input_format": match args.input {
                    TimeInputFormat::Iso => "iso",
                    TimeInputFormat::Unix => "unix",
                },
                "unix_seconds": unix_seconds,
                "counter": args.counter,
                "xact_id": xact.to_string(),
                "pretty_version": pretty,
            }))?
        );
    } else {
        println!("{xact}");
    }
    Ok(())
}

fn run_inspect(json: bool, args: InspectArgs) -> Result<()> {
    let info = inspect_xact_like_input(&args.value)?;
    if json {
        println!("{}", serde_json::to_string(&info)?);
    } else {
        println!(
            "Input kind: {}\nXact ID: {}\nPretty version: {}\nUnix seconds: {}\nISO UTC: {}\nCounter: {}",
            match info.input_kind {
                InputKind::XactId => "xact_id",
                InputKind::PrettyVersionId => "pretty_version_id",
            },
            info.xact_id,
            info.pretty_version,
            info.unix_seconds,
            info.iso_utc,
            info.counter
        );
    }
    Ok(())
}

fn inspect_xact_like_input(value: &str) -> Result<XactInfo> {
    let (input_kind, xact_id) = if is_pretty_version(value) {
        (InputKind::PrettyVersionId, load_pretty_xact(value)?)
    } else {
        let parsed = parse_xact_id(value)?;
        (InputKind::XactId, parsed.to_string())
    };

    let xact = parse_xact_id(&xact_id)?;
    let unix_seconds = xact_to_unix_seconds(xact);
    let iso_utc = unix_seconds_to_iso(unix_seconds)?;
    Ok(XactInfo {
        input_kind,
        xact_id,
        pretty_version: prettify_xact(xact),
        unix_seconds,
        iso_utc,
        counter: xact_counter(xact),
    })
}

fn parse_timestamp_or_now(value: Option<&str>, input: TimeInputFormat) -> Result<u64> {
    match value {
        Some(v) => parse_timestamp(v, input),
        None => Ok(current_unix_seconds()),
    }
}

fn parse_xact_id(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("invalid transaction id '{value}'"))
}

fn parse_timestamp(value: &str, input: TimeInputFormat) -> Result<u64> {
    match input {
        TimeInputFormat::Unix => value
            .parse::<u64>()
            .with_context(|| format!("invalid unix timestamp '{value}'")),
        TimeInputFormat::Iso => {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
                let seconds = parsed.timestamp();
                if seconds < 0 {
                    bail!("timestamp '{value}' is before Unix epoch");
                }
                return Ok(seconds as u64);
            }

            if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid date value '{value}'"))?;
                let seconds = dt.and_utc().timestamp();
                if seconds < 0 {
                    bail!("timestamp '{value}' is before Unix epoch");
                }
                return Ok(seconds as u64);
            }

            bail!(
                "invalid ISO timestamp '{value}' (expected RFC3339 like 2025-01-01T12:34:56Z or date-only 2025-01-01)"
            )
        }
    }
}

fn modular_multiply(value: u64, prime: u64) -> u64 {
    ((value as u128 * prime as u128) % MODULUS) as u64
}

fn prettify_xact(value: u64) -> String {
    let encoded = modular_multiply(value, COPRIME);
    format!("{encoded:016x}")
}

fn load_pretty_xact(encoded_hex: &str) -> Result<String> {
    if encoded_hex.len() != 16 {
        return Ok(encoded_hex.to_string());
    }
    let value = u64::from_str_radix(encoded_hex, 16).with_context(|| {
        format!("invalid pretty version '{encoded_hex}' (expected 16 hex characters)")
    })?;
    let multiplied_inverse = modular_multiply(value, COPRIME_INVERSE);
    let with_top_bits = TOP_BITS | multiplied_inverse;
    Ok(with_top_bits.to_string())
}

fn xact_to_unix_seconds(xact_id: u64) -> u64 {
    (xact_id >> 16) & 0xffff_ffff
}

fn xact_counter(xact_id: u64) -> u16 {
    (xact_id & 0xffff) as u16
}

fn build_xact_id(unix_seconds: u64, counter: u16) -> u64 {
    TOP_BITS | ((unix_seconds & 0xffff_ffff_ffff) << 16) | u64::from(counter)
}

fn unix_seconds_to_iso(unix_seconds: u64) -> Result<String> {
    let dt = DateTime::<Utc>::from_timestamp(unix_seconds as i64, 0).ok_or_else(|| {
        anyhow!("cannot represent unix timestamp as UTC datetime: {unix_seconds}")
    })?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn is_pretty_version(value: &str) -> bool {
    value.len() == 16 && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn current_unix_seconds() -> u64 {
    Utc::now().timestamp().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pretty_roundtrip_sanity() {
        let cases = [
            ("1000192671193355184", "7a670c06c4787a30"),
            ("1000192656880881099", "81cd05ee665fdfb3"),
            ("1000192649085046089", "39a12b7b05fb91c1"),
            ("1000192689925586944", "0c05c446e60d0000"),
        ];

        for (original, pretty) in cases {
            let original_u64 = original.parse::<u64>().unwrap();
            assert_eq!(prettify_xact(original_u64), pretty);
            assert_eq!(load_pretty_xact(pretty).unwrap(), original);
        }
    }

    #[test]
    fn from_time_to_xact_and_back() {
        let unix_seconds = 1_710_209_616u64;
        let counter = 42u16;
        let xact = build_xact_id(unix_seconds, counter);
        assert_eq!(xact_to_unix_seconds(xact), unix_seconds);
        assert_eq!(xact_counter(xact), counter);
    }

    #[test]
    fn load_pretty_passthrough_for_non_pretty_input() {
        assert_eq!(load_pretty_xact("123").unwrap(), "123");
        assert_eq!(
            load_pretty_xact("1000192656880881099").unwrap(),
            "1000192656880881099"
        );
    }

    #[test]
    fn inspect_detects_pretty_versions() {
        let info = inspect_xact_like_input("81cd05ee665fdfb3").unwrap();
        assert!(matches!(info.input_kind, InputKind::PrettyVersionId));
        assert_eq!(info.xact_id, "1000192656880881099");
    }

    #[test]
    fn parse_iso_date_without_time() {
        assert_eq!(
            parse_timestamp("2025-01-01", TimeInputFormat::Iso).unwrap(),
            1_735_689_600
        );
    }

    #[test]
    fn missing_timestamp_defaults_to_now() {
        let before = current_unix_seconds();
        let parsed = parse_timestamp_or_now(None, TimeInputFormat::Iso).unwrap();
        let after = current_unix_seconds();
        assert!(parsed >= before && parsed <= after);
    }
}
