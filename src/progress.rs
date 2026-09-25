use std::{
    collections::VecDeque,
    path::Path,
    time::{Duration, Instant},
};

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::http::StreamFallbackReason;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CellState {
    Pending,
    InProgress,
    HalfDone,
    Done,
}

pub(crate) fn render_grid(f: &mut ratatui::Frame, area: Rect, states: &[CellState]) {
    let height = area.height as usize;
    let width = area.width as usize;
    if height == 0 || width == 0 {
        return;
    }

    let visual_cells = height.saturating_mul(width);
    let real_cells = states.len();
    if visual_cells == 0 {
        return;
    }

    let mut lines: Vec<Line> = Vec::with_capacity(height);
    // For visual cells that map to zero real blocks (can happen when V > R), inherit the previous
    // visual cell color to keep the "cursor" / progress visually consistent. If the first visual
    // cell is unmapped, treat it as Done.
    let mut prev_visual = CellState::Done;
    for row in 0..height {
        let mut spans: Vec<Span> = Vec::with_capacity(width);
        for col in 0..width {
            // IMPORTANT: derive indexing from the *current* area width so terminal resizes
            // are reflected correctly.
            let idx = row * width + col;
            // Map this visual cell to a (possibly empty) range of "real" blocks.
            // This allows resume runs to render even if the terminal size changed.
            //
            // start = floor(idx * R / V)
            // end   = floor((idx+1) * R / V)
            let start = idx.saturating_mul(real_cells) / visual_cells;
            let end = (idx.saturating_add(1)).saturating_mul(real_cells) / visual_cells;

            // If this visual cell corresponds to zero real blocks, inherit previous visual state.
            let st = if start >= end || start >= real_cells {
                prev_visual
            } else {
                // Aggregate the real block states into one visual state (scaled/condensed view).
                //
                // Rules (as requested):
                // - if any real block is being worked on (InProgress) -> orange
                // - else if some done and some not done -> yellow
                // - else if all done -> green
                // - else (all not done) -> grey
                let slice = &states[start..end.min(real_cells)];
                let total = slice.len().max(1);
                let mut done_count = 0usize;
                let mut pending_count = 0usize;
                let mut inprog_count = 0usize;
                for st in slice {
                    match st {
                        CellState::Done => {
                            done_count += 1;
                        }
                        CellState::InProgress => {
                            inprog_count += 1;
                        }
                        CellState::Pending => {
                            pending_count += 1;
                        }
                        // HalfDone is not expected in the real-block state array, but handle it defensively.
                        CellState::HalfDone => {
                            // Treat as "mixed" for safety.
                            pending_count += 1;
                        }
                    }
                }
                if inprog_count > 0 {
                    CellState::InProgress
                } else if done_count > 0 && pending_count > 0 {
                    CellState::HalfDone
                } else if done_count == total {
                    CellState::Done
                } else {
                    CellState::Pending
                }
            };
            prev_visual = st;
            let color = match st {
                CellState::Pending => Color::DarkGray,
                // "Orange" for in-progress.
                CellState::InProgress => Color::Rgb(255, 165, 0),
                // Yellow for half-done.
                CellState::HalfDone => Color::Yellow,
                CellState::Done => Color::Green,
            };
            spans.push(Span::styled("█", Style::default().fg(color)));
        }
        lines.push(Line::from(spans));
    }

    let p = Paragraph::new(lines);
    f.render_widget(p, area);
}

pub(crate) struct ProgressStats {
    pub(crate) bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) done_cells: usize,
    pub(crate) total_cells: usize,
    pub(crate) enabled: u64,
    pub(crate) max_workers: usize,
    pub(crate) active: u64,
    pub(crate) errors: u64,
    pub(crate) since_progress: Duration,
    pub(crate) speed_bps: f64,
}

/// Smoothed throughput over the most recent five seconds (or the full elapsed
/// interval during startup), sampled whenever the status line is refreshed.
#[derive(Default)]
pub(crate) struct RollingSpeed {
    samples: VecDeque<(Instant, u64)>,
}

impl RollingSpeed {
    pub(crate) fn sample(&mut self, now: Instant, bytes: u64) -> f64 {
        self.samples.push_back((now, bytes));
        let cutoff = now.checked_sub(Duration::from_secs(5)).unwrap_or(now);
        while self.samples.len() > 1 && self.samples.get(1).is_some_and(|(at, _)| *at <= cutoff) {
            self.samples.pop_front();
        }
        let Some((start, start_bytes)) = self.samples.front().copied() else {
            return 0.0;
        };
        let elapsed = now.duration_since(start).as_secs_f64();
        if elapsed <= 0.0 {
            0.0
        } else {
            bytes.saturating_sub(start_bytes) as f64 / elapsed
        }
    }
}

impl ProgressStats {
    fn percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.bytes.min(self.total_bytes) as f64 * 100.0 / self.total_bytes as f64).min(100.0)
        }
    }
}

pub(crate) fn render_status(
    f: &mut ratatui::Frame,
    area: Rect,
    url: &str,
    output: &Path,
    stats: &ProgressStats,
) {
    let ProgressStats {
        bytes,
        total_bytes,
        done_cells,
        total_cells,
        enabled,
        max_workers,
        active,
        errors,
        since_progress,
        speed_bps,
    } = stats;
    let pct = stats.percent();
    let idle_workers = enabled.saturating_sub(*active);
    let bytes_hr = human_bytes(*bytes as f64);
    let total_hr = human_bytes(*total_bytes as f64);
    let status = format!(
        "{pct:6.2}%  {bytes_hr}/{total_hr}  5s {speed}/s  blocks {done_cells}/{total_cells}  workers {enabled}/{max_workers}  active {active}  idle_workers {idle_workers}  errors {errors}  idle {idle:.0}s  -> {out}  ({url})",
        speed = human_bytes(*speed_bps),
        idle = since_progress.as_secs_f64(),
        out = output.display(),
    );
    let p = Paragraph::new(status).style(Style::default().fg(Color::White));
    f.render_widget(p, area);
}

pub(crate) fn human_bytes(bps: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bps.max(0.0);
    let mut u = 0usize;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{v:.0}{}", UNITS[u])
    } else {
        format!("{v:.3}{}", UNITS[u])
    }
}

pub(crate) fn format_noui_status_line(stats: &ProgressStats) -> String {
    let bytes_hr = human_bytes(stats.bytes as f64);
    let total_hr = human_bytes(stats.total_bytes as f64);
    let pct = stats.percent();
    format!(
        "{pct:6.2}%  {bytes_hr}/{total_hr}  5s {}/s  blocks {}/{}  workers {}/{}  active {}  idle_workers {}  errors {}  idle {:.0}s",
        human_bytes(stats.speed_bps),
        stats.done_cells,
        stats.total_cells,
        stats.enabled,
        stats.max_workers,
        stats.active,
        stats.enabled.saturating_sub(stats.active),
        stats.errors,
        stats.since_progress.as_secs_f64()
    )
}

pub(crate) fn format_stream_status_line(
    bytes: u64,
    total_hint: Option<u64>,
    speed_bps: f64,
    idle: Duration,
    read_timeouts: u64,
    reason: StreamFallbackReason,
) -> String {
    let bytes_hr = human_bytes(bytes as f64);
    let suffix = format!(
        "idle {:.0}s  consecutive_read_timeouts {read_timeouts}  resume unavailable ({})",
        idle.as_secs_f64(),
        reason.label()
    );
    match total_hint {
        Some(total) if total > 0 => {
            let total_hr = human_bytes(total as f64);
            let pct = ((bytes.min(total) as f64) * 100.0 / (total as f64)).min(100.0);
            format!(
                "stream {pct:6.2}%  {bytes_hr}/{total_hr}  5s {}/s  {suffix}",
                human_bytes(speed_bps),
            )
        }
        _ => format!(
            "stream  {bytes_hr} downloaded  5s {}/s  total unknown  {suffix}",
            human_bytes(speed_bps),
        ),
    }
}

#[cfg(test)]
mod speed_tests {
    use super::RollingSpeed;
    use std::time::{Duration, Instant};

    #[test]
    fn rolling_speed_uses_recent_five_seconds_and_goes_idle() {
        let start = Instant::now();
        let mut speed = RollingSpeed::default();
        assert_eq!(speed.sample(start, 0), 0.0);
        assert_eq!(speed.sample(start + Duration::from_secs(1), 1_000), 1_000.0);
        assert_eq!(speed.sample(start + Duration::from_secs(6), 6_000), 1_000.0);
        assert_eq!(speed.sample(start + Duration::from_secs(12), 6_000), 0.0);
    }
}
