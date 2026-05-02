use anyhow::Result;
use polars::prelude::*;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use tracing::info;

/// Merge per-wiki metric parquet files into combined files at the output root.
/// e.g., output/nlwiki/inequality.parquet + output/dewiki/inequality.parquet
///     → output/inequality.parquet (with wiki column distinguishing them)
pub fn merge_outputs(output_dir: &Path) -> Result<()> {
    info!(output_dir = %output_dir.display(), "merging wiki outputs");

    // Discover all metric names from subdirectories
    let mut metric_files: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        // Skip leading-underscore directories (markers, internal scratch). The
        // current layout never reads them but the filter is defensive against
        // future per-wiki sidecar dirs (e.g. `_patrol_parts`) being
        // accidentally treated as wiki output dirs and dragged into the merge
        // because their name happens to lack a leading underscore.
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with('_'))
        {
            continue;
        }
        let wiki_dir = entry.path();
        for file_entry in fs::read_dir(&wiki_dir)? {
            let file_entry = file_entry?;
            let path = file_entry.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                let metric_name = path.file_name().unwrap().to_string_lossy().to_string();
                metric_files
                    .entry(metric_name)
                    .or_default()
                    .push(path.to_string_lossy().to_string());
            }
        }
    }

    for (metric_name, paths) in &metric_files {
        let lazy_frames: Vec<LazyFrame> = paths
            .iter()
            .map(|p| LazyFrame::scan_parquet(p.as_str().into(), Default::default()))
            .collect::<PolarsResult<_>>()?;
        let mut combined = concat(lazy_frames, Default::default())?.collect()?;

        let dest = output_dir.join(metric_name);
        let mut file = fs::File::create(&dest)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut combined)?;

        info!(
            metric = metric_name,
            path = %dest.display(),
            wikis = paths.len(),
            rows = combined.height(),
            columns = combined.width(),
            "merged metric output"
        );
    }

    materialize_dashboard_artifacts(output_dir)?;

    info!(output_dir = %output_dir.display(), "finished merge");
    Ok(())
}

fn materialize_dashboard_artifacts(output_dir: &Path) -> Result<()> {
    let generator_dir = env::var("WIKI_ECON_GENERATOR_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| Path::new("site").join("data-build"));
    materialize_dashboard_artifacts_from_dir(output_dir, &generator_dir)
}

fn materialize_dashboard_artifacts_from_dir(output_dir: &Path, generator_dir: &Path) -> Result<()> {
    for script_name in [
        "defaults_business.json.sh",
        "defaults_gdp.json.sh",
        "defaults_inequality.json.sh",
        "defaults_labor.json.sh",
        "defaults_patrol.json.sh",
        "manifest.json.sh",
    ] {
        let script_path = generator_dir.join(script_name);
        if !script_path.is_file() {
            continue;
        }
        let output = Command::new("bash")
            .arg(&script_path)
            .env("WIKI_ECON_OUTPUT_DIR", output_dir)
            .output()?;
        if !output.status.success() {
            anyhow::bail!(
                "dashboard artifact generator failed: {}",
                script_path.display()
            );
        }
        let json_path = output_dir.join(script_name.trim_end_matches(".sh"));
        fs::write(&json_path, output.stdout)?;
        info!(
            script = %script_path.display(),
            path = %json_path.display(),
            "materialized dashboard artifact"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};

    fn write_metric(output_dir: &Path, wiki: &str, metric: &str, value: i64) -> Result<()> {
        let wiki_dir = output_dir.join(wiki);
        fs::create_dir_all(&wiki_dir)?;
        let path = wiki_dir.join(format!("{metric}.parquet"));
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("wiki".into(), vec![wiki]),
            Column::new("value".into(), vec![value]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    #[test]
    fn merge_outputs_combines_per_wiki_metrics() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "enwiki", "metric", 1)?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;

        merge_outputs(output_dir.path())?;

        let merged_path = output_dir.path().join("metric.parquet");
        let merged_path = merged_path.to_string_lossy().to_string();
        let merged =
            LazyFrame::scan_parquet(merged_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(merged.height(), 2);

        Ok(())
    }

    #[test]
    fn merge_outputs_skips_underscore_prefixed_subdirectories() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "frwiki", "metric", 1)?;
        // Sidecar dir mimicking _patrol_parts at the wiki-output root level.
        // Without the underscore filter, merge would walk it and try to
        // concatenate its parquets into the merged output with a foreign
        // schema.
        let sidecar = output_dir.path().join("_internal");
        fs::create_dir_all(&sidecar)?;
        let mut sidecar_df =
            DataFrame::new_infer_height(vec![Column::new("unrelated_col".into(), vec!["x"])])?;
        let mut sidecar_file = fs::File::create(sidecar.join("strange.parquet"))?;
        ParquetWriter::new(&mut sidecar_file).finish(&mut sidecar_df)?;

        merge_outputs(output_dir.path())?;

        let merged = output_dir.path().join("metric.parquet");
        let merged_path = merged.to_string_lossy().to_string();
        let df =
            LazyFrame::scan_parquet(merged_path.as_str().into(), Default::default())?.collect()?;
        // Only the legitimate wiki dir contributes rows.
        assert_eq!(df.height(), 1);
        // The sidecar's "strange" parquet must not have been promoted to the
        // root.
        assert!(!output_dir.path().join("strange.parquet").exists());
        Ok(())
    }

    #[test]
    fn merge_outputs_ignores_non_directory_entries_and_non_parquet_files() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        fs::write(output_dir.path().join("README.txt"), b"not a wiki dir")?;
        let wiki_dir = output_dir.path().join("enwiki");
        fs::create_dir_all(&wiki_dir)?;
        fs::write(wiki_dir.join("notes.txt"), b"not parquet")?;

        merge_outputs(output_dir.path())?;

        assert!(!output_dir.path().join("notes.txt").exists());
        assert!(!output_dir.path().join("README.txt.parquet").exists());
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_runs_json_generators() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let script = generator_dir.path().join("defaults_gdp.json.sh");
        fs::write(&script, "#!/bin/sh\nprintf '{\"ok\":true}'\n")?;

        materialize_dashboard_artifacts_from_dir(output_dir.path(), generator_dir.path())?;

        assert_eq!(
            fs::read_to_string(output_dir.path().join("defaults_gdp.json"))?,
            "{\"ok\":true}"
        );
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_errors_on_failed_generator() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let script = generator_dir.path().join("defaults_gdp.json.sh");
        fs::write(&script, "#!/bin/sh\nexit 1\n")?;

        let err = materialize_dashboard_artifacts_from_dir(output_dir.path(), generator_dir.path())
            .expect_err("failed generator should surface an error");

        assert!(
            err.to_string()
                .contains("dashboard artifact generator failed")
        );
        Ok(())
    }
}
