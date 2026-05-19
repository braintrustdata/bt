use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Local, NaiveDate, SecondsFormat, Utc};
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
    /// Version and pagination-key conversion utilities
    #[command(name = "version")]
    Version(VersionArgs),
}

#[derive(Debug, Clone, Args)]
struct VersionArgs {
    #[command(subcommand)]
    command: VersionCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum VersionCommands {
    /// Convert a transaction id to a pretty version id
    ToPretty(ToPrettyArgs),
    /// Convert a pretty version id to a transaction id
    FromPretty(FromPrettyArgs),
    /// Convert a transaction id to a timestamp
    ToTime(ToTimeArgs),
    /// Convert a timestamp to a transaction id or pagination key
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
    /// Decimal transaction id, 16-char pretty version id, or pagination key
    #[arg(value_name = "XACT_OR_PAGINATION")]
    value: String,

    /// Output format for non-JSON mode
    #[arg(long, value_enum, default_value_t = TimeOutputFormat::Iso)]
    format: TimeOutputFormat,

    /// Display ISO timestamps in UTC instead of the local timezone
    #[arg(long)]
    utc: bool,
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

    /// Output a pagination key instead of a transaction id
    #[arg(long)]
    pagination_key: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, Eq, PartialEq)]
enum TimeInputFormat {
    Iso,
    Unix,
}

#[derive(Debug, Clone, Args)]
struct InspectArgs {
    /// Decimal transaction id, 16-char pretty version id, or pagination key
    #[arg(value_name = "XACT_OR_PAGINATION")]
    value: String,

    /// Display ISO timestamps in UTC instead of the local timezone
    #[arg(long)]
    utc: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum InputKind {
    XactId,
    PrettyVersionId,
    PaginationKey,
}

#[derive(Debug, Clone, Serialize)]
struct XactInfo {
    input_kind: InputKind,
    xact_id: String,
    pretty_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination_row_num: Option<u16>,
    unix_seconds: u64,
    iso_utc: String,
    iso_local: String,
    counter: u16,
}

pub async fn run(base: BaseArgs, args: UtilArgs) -> Result<()> {
    match args.command {
        UtilCommands::Version(version) => run_version(base, version),
    }
}

fn run_version(base: BaseArgs, args: VersionArgs) -> Result<()> {
    match args.command {
        VersionCommands::ToPretty(args) => run_to_pretty(base.json, args),
        VersionCommands::FromPretty(args) => run_from_pretty(base.json, args),
        VersionCommands::ToTime(args) => run_to_time(base.json, args),
        VersionCommands::FromTime(args) => run_from_time(base.json, args),
        VersionCommands::Inspect(args) => run_inspect(base.json, args),
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
    let info = inspect_xact_like_input(&args.value)?;
    if json {
        let iso = display_iso(&info, args.utc).to_string();
        let mut payload = serde_json::json!({
            "input_kind": input_kind_label(info.input_kind),
            "xact_id": &info.xact_id,
            "unix_seconds": info.unix_seconds,
            "iso": iso,
            "iso_utc": &info.iso_utc,
            "iso_local": &info.iso_local,
            "timezone": timezone_label(args.utc),
        });
        if let Some(pagination_key) = info.pagination_key.as_deref() {
            payload["pagination_key"] = serde_json::json!(pagination_key);
        }
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        match args.format {
            TimeOutputFormat::Iso => println!("{}", display_iso(&info, args.utc)),
            TimeOutputFormat::Unix => println!("{}", info.unix_seconds),
        }
    }
    Ok(())
}

fn run_from_time(json: bool, args: FromTimeArgs) -> Result<()> {
    let unix_seconds = parse_timestamp_or_now(args.timestamp.as_deref(), args.input)?;
    let xact = build_xact_id(unix_seconds, args.counter);
    let pretty = prettify_xact(xact);
    let pagination_key = build_pagination_key(unix_seconds, args.counter, 0);
    let output_kind = if args.pagination_key {
        "pagination_key"
    } else {
        "xact_id"
    };
    if json {
        let mut payload = serde_json::json!({
            "output_kind": output_kind,
            "input_timestamp": args.timestamp,
            "input_format": match args.input {
                TimeInputFormat::Iso => "iso",
                TimeInputFormat::Unix => "unix",
            },
            "unix_seconds": unix_seconds,
            "counter": args.counter,
            "xact_id": xact.to_string(),
            "pretty_version": pretty,
        });
        if args.pagination_key {
            payload["pagination_key"] = serde_json::json!(format_pagination_key(pagination_key));
            payload["pagination_row_num"] = serde_json::json!(0);
        }
        println!("{}", serde_json::to_string(&payload)?);
    } else if args.pagination_key {
        println!("{}", format_pagination_key(pagination_key));
    } else {
        println!("{xact}");
    }
    Ok(())
}

fn run_inspect(json: bool, args: InspectArgs) -> Result<()> {
    let info = inspect_xact_like_input(&args.value)?;
    if json {
        let mut payload = serde_json::to_value(&info)?;
        payload["iso"] = serde_json::json!(display_iso(&info, args.utc));
        payload["timezone"] = serde_json::json!(timezone_label(args.utc));
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        let mut lines = vec![
            format!("Input kind: {}", input_kind_label(info.input_kind)),
            format!("Xact ID: {}", info.xact_id),
            format!("Pretty version: {}", info.pretty_version),
            format!("Unix seconds: {}", info.unix_seconds),
            format!(
                "{}: {}",
                iso_display_label(args.utc),
                display_iso(&info, args.utc)
            ),
            format!("Counter: {}", info.counter),
        ];
        if let Some(pagination_key) = info.pagination_key {
            lines.insert(1, format!("Pagination key: {pagination_key}"));
        }
        if let Some(row_num) = info.pagination_row_num {
            lines.push(format!("Pagination row number: {row_num}"));
        }
        println!("{}", lines.join("\n"));
    }
    Ok(())
}

fn inspect_xact_like_input(value: &str) -> Result<XactInfo> {
    let is_pagination_key = is_pagination_key_like(value);
    let (input_kind, xact_id, pagination_key, pagination_row_num) = if is_pagination_key {
        let parsed = parse_pagination_key(value)?;
        let unix_seconds = pagination_key_to_unix_seconds(parsed);
        let counter = pagination_key_xact_counter(parsed);
        let xact_id = build_xact_id(unix_seconds, counter);
        (
            InputKind::PaginationKey,
            xact_id.to_string(),
            Some(format_pagination_key(parsed)),
            Some(pagination_key_row_num(parsed)),
        )
    } else if is_pretty_version(value) {
        (
            InputKind::PrettyVersionId,
            load_pretty_xact(value)?,
            None,
            None,
        )
    } else {
        let parsed = parse_xact_id(value)?;
        (InputKind::XactId, parsed.to_string(), None, None)
    };

    let xact = parse_xact_id(&xact_id)?;
    let unix_seconds = xact_to_unix_seconds(xact);
    let iso_utc = unix_seconds_to_iso_utc(unix_seconds)?;
    let iso_local = unix_seconds_to_iso_local(unix_seconds)?;
    Ok(XactInfo {
        input_kind,
        xact_id,
        pretty_version: prettify_xact(xact),
        pagination_key,
        pagination_row_num,
        unix_seconds,
        iso_utc,
        iso_local,
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

fn parse_pagination_key(value: &str) -> Result<u64> {
    let numeric = value
        .strip_prefix('p')
        .or_else(|| value.strip_prefix('P'))
        .ok_or_else(|| {
            anyhow!("invalid pagination key '{value}' (expected p followed by digits)")
        })?;

    if numeric.is_empty() || !numeric.chars().all(|c| c.is_ascii_digit()) {
        bail!("invalid pagination key '{value}' (expected p followed by digits)");
    }

    numeric
        .parse::<u64>()
        .with_context(|| format!("invalid pagination key '{value}'"))
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

fn build_pagination_key(unix_seconds: u64, counter: u16, row_num: u16) -> u64 {
    ((unix_seconds & 0xffff_ffff) << 32) | (u64::from(counter) << 16) | u64::from(row_num)
}

fn format_pagination_key(pagination_key: u64) -> String {
    format!("p{pagination_key:020}")
}

fn pagination_key_to_unix_seconds(pagination_key: u64) -> u64 {
    pagination_key >> 32
}

fn pagination_key_xact_counter(pagination_key: u64) -> u16 {
    ((pagination_key >> 16) & 0xffff) as u16
}

fn pagination_key_row_num(pagination_key: u64) -> u16 {
    (pagination_key & 0xffff) as u16
}

fn unix_seconds_to_utc_datetime(unix_seconds: u64) -> Result<DateTime<Utc>> {
    let dt = DateTime::<Utc>::from_timestamp(unix_seconds as i64, 0).ok_or_else(|| {
        anyhow!("cannot represent unix timestamp as UTC datetime: {unix_seconds}")
    })?;
    Ok(dt)
}

fn unix_seconds_to_iso_utc(unix_seconds: u64) -> Result<String> {
    let dt = unix_seconds_to_utc_datetime(unix_seconds)?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn unix_seconds_to_iso_local(unix_seconds: u64) -> Result<String> {
    let dt = unix_seconds_to_utc_datetime(unix_seconds)?.with_timezone(&Local);
    Ok(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn is_pretty_version(value: &str) -> bool {
    value.len() == 16 && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_pagination_key_like(value: &str) -> bool {
    value.starts_with('p') || value.starts_with('P')
}

fn input_kind_label(input_kind: InputKind) -> &'static str {
    match input_kind {
        InputKind::XactId => "xact_id",
        InputKind::PrettyVersionId => "pretty_version_id",
        InputKind::PaginationKey => "pagination_key",
    }
}

fn display_iso(info: &XactInfo, utc: bool) -> &str {
    if utc {
        &info.iso_utc
    } else {
        &info.iso_local
    }
}

fn timezone_label(utc: bool) -> &'static str {
    if utc {
        "utc"
    } else {
        "local"
    }
}

fn iso_display_label(utc: bool) -> &'static str {
    if utc {
        "ISO UTC"
    } else {
        "ISO local"
    }
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
    fn from_time_to_pagination_key_uses_xact_counter() {
        let unix_seconds = 1_778_727_718u64;
        let counter = 31_627u16;
        let pagination_key = build_pagination_key(unix_seconds, counter, 0);
        assert_eq!(
            format_pagination_key(pagination_key),
            "p07639577379371417600"
        );
        assert_eq!(pagination_key_to_unix_seconds(pagination_key), unix_seconds);
        assert_eq!(pagination_key_xact_counter(pagination_key), counter);
        assert_eq!(pagination_key_row_num(pagination_key), 0);
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
    fn inspect_decodes_pagination_key_time() {
        let info = inspect_xact_like_input("p07639577379371417602").unwrap();
        assert!(matches!(info.input_kind, InputKind::PaginationKey));
        assert_eq!(
            info.pagination_key.as_deref(),
            Some("p07639577379371417602")
        );
        assert_eq!(info.xact_id, "1000197162952719243");
        assert_eq!(info.unix_seconds, 1_778_727_718);
        assert_eq!(info.iso_utc, "2026-05-14T03:01:58Z");
        assert_eq!(info.counter, 31_627);
        assert_eq!(info.pagination_row_num, Some(2));
    }

    #[test]
    fn pagination_key_parser_accepts_short_form() {
        let info = inspect_xact_like_input("p0").unwrap();
        assert!(matches!(info.input_kind, InputKind::PaginationKey));
        assert_eq!(
            info.pagination_key.as_deref(),
            Some("p00000000000000000000")
        );
        assert_eq!(info.unix_seconds, 0);
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
