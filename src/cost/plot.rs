//! Cost line charts, rendered with plotters.
//!
//! The same drawing routine feeds three backends: a custom text backend for the
//! console (plotters has none), and plotters' SVG and bitmap backends for
//! `--save-fig`. The x-axis (abscissa) is labeled with the point labels (dates
//! for `--group-by day`, category names otherwise); the y-axis is the cost
//! scale.

use std::cell::RefCell;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::{anyhow, bail, Context, Result};
use plotters::coord::Shift;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, VPos};
use plotters_backend::{
    BackendColor, BackendCoord, BackendStyle, BackendTextStyle, DrawingBackend, DrawingErrorKind,
};

use crate::utils::format_cost;

/// Image size (pixels) for saved figures.
const FIGURE_SIZE: (u32, u32) = (1200, 700);

#[derive(Copy, Clone)]
enum PixelState {
    Empty,
    HLine,
    VLine,
    Cross,
    Pixel,
    Filled,
    Text(char),
}

impl PixelState {
    fn to_char(self) -> char {
        match self {
            Self::Empty => ' ',
            Self::HLine => '-',
            Self::VLine => '|',
            Self::Cross => '+',
            Self::Pixel => '.',
            Self::Filled => '#',
            Self::Text(c) => c,
        }
    }

    fn update(&mut self, new_state: PixelState) {
        let next_state = match (*self, new_state) {
            (Self::HLine, Self::VLine) => Self::Cross,
            (Self::VLine, Self::HLine) => Self::Cross,
            // Text and filled bars win over lines/pixels.
            (_, Self::Text(c)) => Self::Text(c),
            (_, Self::Filled) => Self::Filled,
            (Self::Filled, _) => Self::Filled,
            (_, Self::Pixel) => Self::Pixel,
            (Self::Pixel, _) => Self::Pixel,
            (_, new) => new,
        };
        *self = next_state;
    }
}

/// A [`DrawingBackend`] that rasterizes into a character grid and, on
/// `present()`, writes the rendered rows into a shared string buffer.
struct TextDrawingBackend {
    width: u32,
    height: u32,
    grid: Vec<PixelState>,
    out: Rc<RefCell<String>>,
}

impl TextDrawingBackend {
    fn new(width: u32, height: u32, out: Rc<RefCell<String>>) -> Self {
        Self {
            width,
            height,
            grid: vec![PixelState::Empty; (width * height) as usize],
            out,
        }
    }

    fn index(&self, x: i32, y: i32) -> Option<usize> {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return None;
        }
        Some((y * self.width as i32 + x) as usize)
    }

    fn set(&mut self, x: i32, y: i32, state: PixelState) {
        if let Some(index) = self.index(x, y) {
            self.grid[index].update(state);
        }
    }
}

impl DrawingBackend for TextDrawingBackend {
    type ErrorType = std::io::Error;

    fn get_size(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    fn ensure_prepared(&mut self) -> Result<(), DrawingErrorKind<std::io::Error>> {
        Ok(())
    }

    fn present(&mut self) -> Result<(), DrawingErrorKind<std::io::Error>> {
        let mut out = self.out.borrow_mut();
        out.clear();
        for r in 0..self.height as usize {
            let mut buf = String::new();
            for c in 0..self.width as usize {
                buf.push(self.grid[r * self.width as usize + c].to_char());
            }
            out.push_str(buf.trim_end());
            out.push('\n');
        }
        Ok(())
    }

    fn draw_pixel(
        &mut self,
        pos: (i32, i32),
        color: BackendColor,
    ) -> Result<(), DrawingErrorKind<std::io::Error>> {
        if color.alpha > 0.3 {
            self.set(pos.0, pos.1, PixelState::Pixel);
        }
        Ok(())
    }

    fn draw_line<S: BackendStyle>(
        &mut self,
        from: (i32, i32),
        to: (i32, i32),
        style: &S,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        if from.0 == to.0 {
            let x = from.0;
            let (y0, y1) = (from.1.min(to.1), from.1.max(to.1));
            for y in y0..=y1 {
                self.set(x, y, PixelState::VLine);
            }
            return Ok(());
        }
        if from.1 == to.1 {
            let y = from.1;
            let (x0, x1) = (from.0.min(to.0), from.0.max(to.0));
            for x in x0..=x1 {
                self.set(x, y, PixelState::HLine);
            }
            return Ok(());
        }
        plotters_backend::rasterizer::draw_line(self, from, to, style)
    }

    fn draw_rect<S: BackendStyle>(
        &mut self,
        upper_left: BackendCoord,
        bottom_right: BackendCoord,
        style: &S,
        fill: bool,
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        let (x0, x1) = (
            upper_left.0.min(bottom_right.0),
            upper_left.0.max(bottom_right.0),
        );
        let (y0, y1) = (
            upper_left.1.min(bottom_right.1),
            upper_left.1.max(bottom_right.1),
        );
        if fill {
            for y in y0..=y1 {
                for x in x0..=x1 {
                    self.set(x, y, PixelState::Filled);
                }
            }
        } else {
            self.draw_line((x0, y0), (x1, y0), style)?;
            self.draw_line((x0, y1), (x1, y1), style)?;
            self.draw_line((x0, y0), (x0, y1), style)?;
            self.draw_line((x1, y0), (x1, y1), style)?;
        }
        Ok(())
    }

    fn estimate_text_size<S: BackendTextStyle>(
        &self,
        text: &str,
        _: &S,
    ) -> Result<(u32, u32), DrawingErrorKind<Self::ErrorType>> {
        Ok((text.len() as u32, 1))
    }

    fn draw_text<S: BackendTextStyle>(
        &mut self,
        text: &str,
        style: &S,
        pos: (i32, i32),
    ) -> Result<(), DrawingErrorKind<Self::ErrorType>> {
        let (width, height) = self.estimate_text_size(text, style)?;
        let (width, height) = (width as i32, height as i32);
        let dx = match style.anchor().h_pos {
            HPos::Left => 0,
            HPos::Right => -width,
            HPos::Center => -width / 2,
        };
        let dy = match style.anchor().v_pos {
            VPos::Top => 0,
            VPos::Center => -height / 2,
            VPos::Bottom => -height,
        };
        let x = (pos.0 + dx).max(0);
        let y = (pos.1 + dy).max(0);
        for (offset, chr) in text.chars().enumerate() {
            self.set(x + offset as i32, y, PixelState::Text(chr));
        }
        Ok(())
    }
}

fn truncate_label(label: &str, max: usize) -> String {
    let max = max.max(3);
    if label.chars().count() <= max {
        return label.to_string();
    }
    let keep = max.saturating_sub(1);
    format!("{}\u{2026}", label.chars().take(keep).collect::<String>())
}

/// Backend-specific layout, in the backend's own units (characters for the
/// console, pixels for images).
struct ChartLayout {
    /// Number of x-axis tick labels to attempt.
    tick_count: usize,
    /// Point marker radius (0 to omit markers).
    marker_size: i32,
    /// Blank space reserved on the right so the last label isn't clipped.
    right_margin: i32,
    /// Width of the left (y-axis label) area.
    left_labels: i32,
}

/// Draw the cost line chart onto any plotters backend. The x-axis (abscissa) is
/// labeled from `labels` by point index; the y-axis is the cost scale.
fn draw_cost_chart<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    title: &str,
    points: &[(String, f64)],
    labels: &[String],
    layout: &ChartLayout,
) -> std::result::Result<(), Box<dyn Error>>
where
    DB::ErrorType: 'static,
{
    let max_cost = points.iter().map(|(_, cost)| *cost).fold(0.0_f64, f64::max);
    let count = points.len();
    // Pad the domain by half a bucket on each side so the first and last points
    // (and their centered labels) sit inside the plotting area. Keep it small
    // and fixed: a large pad distorts plotters' "nice" tick placement and yields
    // too few labels.
    let last = ((count as f64) - 1.0).max(1.0);
    let pad = 0.5;
    let y_max = (max_cost * 1.15).max(f64::MIN_POSITIVE);

    let mut chart = ChartBuilder::on(area)
        .margin(1)
        // Reserve space on the right so the last label isn't clipped.
        .margin_right(layout.right_margin.max(1))
        .caption(title, ("sans-serif", (8).percent_height()))
        // Left area is sized by the caller to the y-axis labels (no wide gutter).
        .set_label_area_size(LabelAreaPosition::Left, layout.left_labels.max(4))
        .set_label_area_size(LabelAreaPosition::Bottom, (10i32).percent_height())
        .build_cartesian_2d((-pad)..(last + pad), 0f64..y_max)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .x_labels(layout.tick_count)
        .x_label_formatter(&|value| {
            // Only label integer positions that map to a point.
            if (value.round() - value).abs() > 1e-6 || *value < 0.0 {
                return String::new();
            }
            labels
                .get(value.round() as usize)
                .cloned()
                .unwrap_or_default()
        })
        .y_label_formatter(&|value| format_cost(*value))
        .draw()?;

    chart.draw_series(LineSeries::new(
        points
            .iter()
            .enumerate()
            .map(|(index, (_, cost))| (index as f64, cost.max(0.0))),
        RED,
    ))?;

    if layout.marker_size > 0 {
        chart.draw_series(points.iter().enumerate().map(|(index, (_, cost))| {
            Circle::new(
                (index as f64, cost.max(0.0)),
                layout.marker_size,
                RED.filled(),
            )
        }))?;
    }

    Ok(())
}

/// Render a line chart to a string for the console. The bottom axis (abscissa)
/// is labeled with the point labels (e.g. dates for `--group-by day`); the left
/// axis is the cost scale. Points are drawn left-to-right in the given order.
pub(super) fn render_cost_chart(
    title: &str,
    points: &[(String, f64)],
    size: (u32, u32),
) -> Result<String> {
    if points.is_empty() {
        return Ok(String::new());
    }
    let max_cost = points.iter().map(|(_, cost)| *cost).fold(0.0_f64, f64::max);
    if max_cost <= 0.0 {
        return Ok(String::new());
    }

    let (width, height) = size;
    let count = points.len();

    // Cap displayed label width, then size the tick density to it so as many
    // labels as fit are shown without overlapping (short labels -> more ticks).
    let labels: Vec<String> = points
        .iter()
        .map(|(label, _)| truncate_label(label, 16))
        .collect();
    let max_label = labels
        .iter()
        .map(|label| label.chars().count())
        .max()
        .unwrap_or(6)
        .max(3);
    let tick_count = ((width as usize) / (max_label + 2)).clamp(2, count);
    let layout = ChartLayout {
        tick_count,
        marker_size: if count <= width as usize / 4 { 1 } else { 0 },
        right_margin: ((width as i32) * 6 / 100).max(3),
        // Console cells are one character wide, so the label area is a character
        // count: the widest cost tick plus the axis tick and a space.
        left_labels: format_cost(max_cost * 1.15).len() as i32 + 2,
    };

    let out = Rc::new(RefCell::new(String::new()));
    {
        let area = TextDrawingBackend::new(width, height, out.clone()).into_drawing_area();
        draw_cost_chart(&area, title, points, &labels, &layout)
            .map_err(|err| anyhow!("failed to render chart: {err}"))?;
        area.present()
            .map_err(|err| anyhow!("failed to render chart: {err}"))?;
    }

    Ok(Rc::try_unwrap(out)
        .map(RefCell::into_inner)
        .unwrap_or_default())
}

#[derive(Clone, Copy)]
enum Figure {
    Svg,
    Png,
}

/// Save a cost chart to `path`. The format is chosen by extension (`.svg` or
/// `.png`); a path with no extension is written as SVG. Returns the path
/// actually written.
pub(super) fn save_cost_chart(
    title: &str,
    points: &[(String, f64)],
    path: &Path,
) -> Result<PathBuf> {
    if points.is_empty() {
        bail!("no rows to plot");
    }
    let max_cost = points.iter().map(|(_, cost)| *cost).fold(0.0_f64, f64::max);
    if max_cost <= 0.0 {
        bail!("no cost to plot for the selected window and filters");
    }

    let (figure, out_path) = match path.extension().and_then(|ext| ext.to_str()) {
        None => (Figure::Svg, path.with_extension("svg")),
        Some(ext) => match ext.to_ascii_lowercase().as_str() {
            "svg" => (Figure::Svg, path.to_path_buf()),
            "png" => (Figure::Png, path.to_path_buf()),
            other => bail!("unsupported figure format '.{other}'; use .svg or .png"),
        },
    };

    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }

    let (labels, tick_count) = file_chart_inputs(points);
    let layout = ChartLayout {
        tick_count,
        marker_size: 3,
        right_margin: (FIGURE_SIZE.0 as i32) * 6 / 100,
        // Image label areas are in pixels; reserve ~9% of the width for y labels.
        left_labels: (FIGURE_SIZE.0 as i32) * 9 / 100,
    };

    match figure {
        Figure::Svg => {
            let area = SVGBackend::new(&out_path, FIGURE_SIZE).into_drawing_area();
            area.fill(&WHITE)
                .map_err(|err| anyhow!("failed to initialize figure: {err}"))?;
            draw_cost_chart(&area, title, points, &labels, &layout)
                .map_err(|err| anyhow!("failed to draw figure: {err}"))?;
            area.present()
                .map_err(|err| anyhow!("failed to write figure: {err}"))?;
        }
        Figure::Png => {
            let area = BitMapBackend::new(&out_path, FIGURE_SIZE).into_drawing_area();
            area.fill(&WHITE)
                .map_err(|err| anyhow!("failed to initialize figure: {err}"))?;
            draw_cost_chart(&area, title, points, &labels, &layout)
                .map_err(|err| anyhow!("failed to draw figure: {err}"))?;
            area.present()
                .map_err(|err| anyhow!("failed to write figure: {err}"))?;
        }
    }

    Ok(out_path)
}

/// Render the chart to an in-memory PNG (for inline terminal display). Returns
/// `None` when there is nothing to plot.
pub(super) fn render_png_bytes(
    title: &str,
    points: &[(String, f64)],
    size: (u32, u32),
) -> Result<Option<Vec<u8>>> {
    if points.is_empty() {
        return Ok(None);
    }
    let max_cost = points.iter().map(|(_, cost)| *cost).fold(0.0_f64, f64::max);
    if max_cost <= 0.0 {
        return Ok(None);
    }

    let (labels, tick_count) = file_chart_inputs(points);
    let layout = ChartLayout {
        tick_count,
        marker_size: 3,
        right_margin: (size.0 as i32) * 6 / 100,
        left_labels: (size.0 as i32) * 9 / 100,
    };
    let tmp = tempfile::Builder::new()
        .suffix(".png")
        .tempfile()
        .context("failed to create temporary image file")?;
    {
        let area = BitMapBackend::new(tmp.path(), size).into_drawing_area();
        area.fill(&WHITE)
            .map_err(|err| anyhow!("failed to initialize figure: {err}"))?;
        draw_cost_chart(&area, title, points, &labels, &layout)
            .map_err(|err| anyhow!("failed to draw figure: {err}"))?;
        area.present()
            .map_err(|err| anyhow!("failed to render figure: {err}"))?;
    }
    let bytes = std::fs::read(tmp.path()).context("failed to read rendered chart")?;
    Ok(Some(bytes))
}

/// Full labels (no truncation) and a capped tick count, shared by file and
/// inline-image rendering.
fn file_chart_inputs(points: &[(String, f64)]) -> (Vec<String>, usize) {
    let labels = points.iter().map(|(label, _)| label.clone()).collect();
    let tick_count = points.len().clamp(2, 12);
    (labels, tick_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_zero_points_render_nothing() {
        assert!(render_cost_chart("Cost", &[], (80, 20)).unwrap().is_empty());
        let zero = [("a".to_string(), 0.0), ("b".to_string(), 0.0)];
        assert!(render_cost_chart("Cost", &zero, (80, 20))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn renders_a_line_chart_with_x_labels() {
        let points = [
            ("2026-07-19".to_string(), 0.04),
            ("2026-07-20".to_string(), 0.08),
            ("2026-07-21".to_string(), 0.15),
        ];
        let chart = render_cost_chart("Cost by Day", &points, (100, 24)).unwrap();
        assert!(!chart.trim().is_empty());
        assert!(chart.contains("Cost by Day"), "{chart}");
        // x-axis (abscissa) tick labels are present.
        assert!(chart.contains("2026-07-19"), "{chart}");
        // y-axis dollar labels are present.
        assert!(chart.contains('$'), "{chart}");
    }

    #[test]
    fn saves_svg_png_and_defaults_to_svg() {
        let dir = tempfile::tempdir().unwrap();
        let points = [
            ("2026-07-20".to_string(), 0.08),
            ("2026-07-21".to_string(), 0.15),
        ];

        let svg = save_cost_chart("Cost by Day", &points, &dir.path().join("chart.svg")).unwrap();
        assert_eq!(svg.extension().unwrap(), "svg");
        assert!(std::fs::read_to_string(&svg).unwrap().contains("<svg"));

        let png = save_cost_chart("Cost by Day", &points, &dir.path().join("chart.png")).unwrap();
        let png_bytes = std::fs::read(&png).unwrap();
        assert_eq!(&png_bytes[..8], b"\x89PNG\r\n\x1a\n");

        // No extension defaults to SVG.
        let no_ext = save_cost_chart("Cost", &points, &dir.path().join("chart")).unwrap();
        assert_eq!(no_ext.extension().unwrap(), "svg");
        assert!(no_ext.exists());

        // Unsupported extensions are rejected.
        assert!(save_cost_chart("Cost", &points, &dir.path().join("chart.pdf")).is_err());
    }

    #[test]
    fn truncates_long_labels() {
        assert_eq!(truncate_label("short", 24), "short");
        let long = "a-really-really-long-model-name-that-overflows";
        let truncated = truncate_label(long, 24);
        assert_eq!(truncated.chars().count(), 24);
        assert!(truncated.ends_with('\u{2026}'));
    }
}
