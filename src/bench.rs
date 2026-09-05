use crate::compute;
use crate::metric_registry::MetricFamily;
use anyhow::{Context, Result};
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static NEXT_SCRATCH_DIR_ID: AtomicU64 = AtomicU64::new(0);

struct ScratchDir {
    path: PathBuf,
    keep: bool,
}

impl ScratchDir {
    fn new(base: Option<&Path>, prefix: &str) -> Result<Self> {
        let keep = base.is_some();
        let path = if let Some(base) = base {
            base.to_path_buf()
        } else {
            let mut path = std::env::temp_dir();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before UNIX_EPOCH")?
                .as_nanos();
            let unique_id = NEXT_SCRATCH_DIR_ID.fetch_add(1, Ordering::Relaxed);
            path.push(format!(
                "{prefix}-{}-{timestamp}-{unique_id}",
                std::process::id()
            ));
            path
        };

        fs::create_dir_all(&path)?;
        Ok(Self { path, keep })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        if !self.keep && self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct StageSummary {
    min_ms: f64,
    median_ms: f64,
    mean_ms: f64,
    max_ms: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct OutputSummary {
    metric: String,
    rows: usize,
    columns: usize,
    bytes: u64,
}

#[derive(Default)]
struct Timings {
    monthly: Vec<f64>,
    activity_tiers: Vec<f64>,
    lifecycle: Vec<f64>,
    page_week: Vec<f64>,
    compute_all: Vec<f64>,
}

impl Timings {
    fn record_iteration(&mut self, iteration: &IterationResult) {
        self.monthly.push(to_ms(iteration.monthly));
        self.activity_tiers.push(to_ms(iteration.activity_tiers));
        self.lifecycle.push(to_ms(iteration.lifecycle));
        self.page_week.push(to_ms(iteration.page_week));
        self.compute_all.push(to_ms(iteration.compute_all));
    }
}

struct IterationResult {
    monthly: Duration,
    activity_tiers: Duration,
    lifecycle: Duration,
    page_week: Duration,
    compute_all: Duration,
    outputs: Vec<OutputSummary>,
}

pub fn run(
    wikis: &[String],
    data_dir: &Path,
    output_dir: &Path,
    warmup: usize,
    iterations: usize,
    keep_outputs: bool,
) -> Result<()> {
    if wikis.is_empty() {
        anyhow::bail!("benchmark requires at least one wiki");
    }
    if iterations == 0 {
        anyhow::bail!("benchmark requires at least one measured iteration");
    }

    for wiki in wikis {
        println!("Benchmarking {wiki}");
        println!(
            "  dataset: {}",
            data_dir.join("parquet").join(wiki).display()
        );
        println!("  warmup: {warmup}  measured: {iterations}");

        for warmup_idx in 0..warmup {
            let _ = benchmark_iteration(wiki, data_dir, output_dir, warmup_idx, false, false)?;
        }

        let mut timings = Timings::default();
        let mut outputs = Vec::new();

        for iteration in 0..iterations {
            let capture = iteration == 0;
            let result =
                benchmark_iteration(wiki, data_dir, output_dir, iteration, keep_outputs, capture)?;
            if capture {
                outputs = result.outputs.clone();
            }
            timings.record_iteration(&result);
        }

        print_stage_summary("monthly", &timings.monthly);
        print_stage_summary("activity_tiers", &timings.activity_tiers);
        print_stage_summary("lifecycle", &timings.lifecycle);
        print_stage_summary("page_week", &timings.page_week);
        print_stage_summary("compute_all", &timings.compute_all);

        if !outputs.is_empty() {
            print_output_summaries(&outputs);
        }

        if keep_outputs {
            println!(
                "  kept outputs under {}",
                output_dir.join("bench").display()
            );
        }
        println!();
    }

    Ok(())
}

fn benchmark_iteration(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    iteration: usize,
    keep_outputs: bool,
    capture_outputs: bool,
) -> Result<IterationResult> {
    let family_root = if keep_outputs {
        Some(
            output_dir
                .join("bench")
                .join(wiki)
                .join(format!("iter-{iteration}"))
                .join("families"),
        )
    } else {
        None
    };
    let full_root = if keep_outputs {
        Some(
            output_dir
                .join("bench")
                .join(wiki)
                .join(format!("iter-{iteration}"))
                .join("full"),
        )
    } else {
        None
    };

    let family_dir = ScratchDir::new(family_root.as_deref(), "wiki-econ-bench-families")?;
    let full_dir = ScratchDir::new(full_root.as_deref(), "wiki-econ-bench-full")?;

    let family_duration = |family| -> Result<Duration> {
        let started = Instant::now();
        compute::execute_family(wiki, data_dir, family_dir.path(), family)?;
        Ok(started.elapsed())
    };
    let monthly = family_duration(MetricFamily::Monthly)?;
    let activity_tiers = family_duration(MetricFamily::ActivityTiers)?;
    let lifecycle = family_duration(MetricFamily::Lifecycle)?;
    let page_week = family_duration(MetricFamily::PageWeek)?;

    let compute_all_started = Instant::now();
    compute::compute_all(wiki, data_dir, full_dir.path())?;
    let compute_all = compute_all_started.elapsed();

    let outputs = if capture_outputs {
        collect_output_summaries(full_dir.path(), wiki)?
    } else {
        Vec::new()
    };

    Ok(IterationResult {
        monthly,
        activity_tiers,
        lifecycle,
        page_week,
        compute_all,
        outputs,
    })
}

fn collect_output_summaries(output_dir: &Path, wiki: &str) -> Result<Vec<OutputSummary>> {
    let wiki_dir = output_dir.join(wiki);
    let mut paths: Vec<PathBuf> = fs::read_dir(&wiki_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    paths.sort();

    let mut outputs = Vec::new();
    for path in paths {
        let bytes = fs::metadata(&path)?.len();
        let mut reader = ParquetReader::new(fs::File::open(&path)?);
        let rows = reader.num_rows()?;
        let columns = ParquetReader::new(fs::File::open(&path)?)
            .with_slice(Some((0, 0)))
            .finish()?
            .width();
        let metric = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .context("metric output path has no valid file stem")?
            .to_string();
        outputs.push(OutputSummary {
            metric,
            rows,
            columns,
            bytes,
        });
    }

    Ok(outputs)
}

fn print_stage_summary(name: &str, samples: &[f64]) {
    let summary = summarize(samples).expect("stage should have at least one sample");
    println!(
        "  {:<12} mean {:>8.2} ms  median {:>8.2} ms  min {:>8.2} ms  max {:>8.2} ms",
        name, summary.mean_ms, summary.median_ms, summary.min_ms, summary.max_ms
    );
}

fn print_output_summaries(outputs: &[OutputSummary]) {
    println!("  outputs:");
    for output in outputs {
        println!(
            "    {:<22} rows {:>8}  cols {:>3}  bytes {:>8}",
            output.metric, output.rows, output.columns, output.bytes
        );
    }
}

fn summarize(samples: &[f64]) -> Option<StageSummary> {
    if samples.is_empty() {
        return None;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean_ms = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median_ms = if sorted.len().is_multiple_of(2) {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };

    Some(StageSummary {
        min_ms: *sorted.first().expect("non-empty"),
        median_ms,
        mean_ms,
        max_ms: *sorted.last().expect("non-empty"),
    })
}

fn to_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn write_compute_input(data_dir: &Path, wiki: &str) -> Result<()> {
        let parquet_dir = data_dir.join("parquet").join(wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new(
                "event_entity".into(),
                vec!["revision", "revision", "revision", "revision"],
            ),
            Column::new(
                "event_type".into(),
                vec!["create", "create", "create", "create"],
            ),
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2024-01-01 00:00:00.0",
                    "2024-01-15 00:00:00.0",
                    "2024-02-01 00:00:00.0",
                    "2024-02-10 00:00:00.0",
                ],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2, 1, 3]),
            Column::new(
                "event_user_is_bot_by".into(),
                vec![None::<&str>, None, Some("bot"), None],
            ),
            Column::new(
                "event_user_is_anonymous".into(),
                vec!["false", "false", "false", "true"],
            ),
            Column::new(
                "event_user_is_temporary".into(),
                vec!["false", "true", "false", "false"],
            ),
            Column::new("page_namespace".into(), vec![0_i32, 0, 1, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11, 12, 13]),
            Column::new("revision_text_bytes_diff".into(), vec![10_i64, 20, -5, 15]),
            Column::new(
                "revision_is_identity_reverted".into(),
                vec!["false", "false", "true", "false"],
            ),
            Column::new(
                "revision_minor_edit".into(),
                vec!["false", "true", "false", "false"],
            ),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    #[test]
    fn summarize_handles_even_and_odd_samples() {
        assert_eq!(summarize(&[]), None);

        let odd = summarize(&[3.0, 1.0, 2.0]).expect("odd summary");
        assert_eq!(
            odd,
            StageSummary {
                min_ms: 1.0,
                median_ms: 2.0,
                mean_ms: 2.0,
                max_ms: 3.0,
            }
        );

        let even = summarize(&[4.0, 1.0, 2.0, 3.0]).expect("even summary");
        assert_eq!(
            even,
            StageSummary {
                min_ms: 1.0,
                median_ms: 2.5,
                mean_ms: 2.5,
                max_ms: 4.0,
            }
        );
    }

    #[test]
    fn run_requires_wikis_and_iterations() {
        let err = run(&[], Path::new("data"), Path::new("output"), 0, 1, false)
            .expect_err("missing wikis should fail");
        assert!(err.to_string().contains("at least one wiki"));

        let err = run(
            &["frwiki".to_string()],
            Path::new("data"),
            Path::new("output"),
            0,
            0,
            false,
        )
        .expect_err("zero iterations should fail");
        assert!(err.to_string().contains("at least one measured iteration"));
    }

    #[test]
    fn benchmark_iteration_collects_outputs() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "benchwiki";

        write_compute_input(data_dir.path(), wiki)?;
        let result = benchmark_iteration(wiki, data_dir.path(), output_dir.path(), 0, false, true)?;

        assert_eq!(result.outputs.len(), 8);
        assert!(
            result
                .outputs
                .iter()
                .any(|summary| summary.metric == "inequality" && summary.columns == 14)
        );
        Ok(())
    }

    #[test]
    fn run_keeps_outputs_when_requested() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "benchwiki";

        write_compute_input(data_dir.path(), wiki)?;
        let wikis = [wiki.to_string()];
        run(&wikis, data_dir.path(), output_dir.path(), 1, 1, true)?;

        assert!(
            output_dir
                .path()
                .join("bench")
                .join(wiki)
                .join("iter-0")
                .join("full")
                .join(wiki)
                .join("gdp.parquet")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn run_uses_temporary_outputs_by_default() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "benchwiki";

        write_compute_input(data_dir.path(), wiki)?;
        let wikis = [wiki.to_string()];
        run(&wikis, data_dir.path(), output_dir.path(), 0, 1, false)?;

        assert!(!output_dir.path().join("bench").exists());
        Ok(())
    }
}
