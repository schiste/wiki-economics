use anyhow::Result;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const ANALYTICAL_DIRNAME: &str = "parquet";
pub const WAREHOUSE_DIRNAME: &str = "warehouse";
const MARKERS_DIRNAME: &str = "_markers";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionSpec {
    pub year: i32,
    pub year_month: String,
    pub dir: PathBuf,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarkerManifest {
    pub source: Option<String>,
    pub rows: usize,
    pub analytical_paths: Vec<PathBuf>,
    pub warehouse_paths: Vec<PathBuf>,
}

pub fn analytical_wiki_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir.join(ANALYTICAL_DIRNAME).join(wiki)
}

pub fn warehouse_wiki_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir.join(WAREHOUSE_DIRNAME).join(wiki)
}

pub fn marker_path(data_dir: &Path, wiki: &str, source_id: &str) -> PathBuf {
    analytical_wiki_dir(data_dir, wiki)
        .join(MARKERS_DIRNAME)
        .join(format!("{source_id}.done"))
}

pub fn write_marker_manifest(
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
    manifest: &MarkerManifest,
) -> Result<PathBuf> {
    let marker = marker_path(data_dir, wiki, source_id);
    marker.parent().map(fs::create_dir_all).transpose()?;

    let mut lines = Vec::new();
    if let Some(source) = &manifest.source {
        lines.push(format!("source={source}"));
    }
    lines.push(format!("rows={}", manifest.rows));
    lines.push(format!(
        "analytical_parts={}",
        manifest.analytical_paths.len()
    ));
    for path in &manifest.analytical_paths {
        let relative = path.strip_prefix(data_dir).unwrap_or(path);
        lines.push(format!("analytical_path={}", relative.display()));
    }
    lines.push(format!(
        "warehouse_parts={}",
        manifest.warehouse_paths.len()
    ));
    for path in &manifest.warehouse_paths {
        let relative = path.strip_prefix(data_dir).unwrap_or(path);
        lines.push(format!("warehouse_path={}", relative.display()));
    }
    lines.push(String::new());

    fs::write(&marker, lines.join("\n"))?;
    Ok(marker)
}

pub fn read_marker_manifest(
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
) -> Result<Option<MarkerManifest>> {
    let marker = marker_path(data_dir, wiki, source_id);
    if !marker.exists() {
        return Ok(None);
    }

    let mut manifest = MarkerManifest::default();
    for line in fs::read_to_string(marker)?.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "source" => manifest.source = Some(value.to_string()),
            "rows" => manifest.rows = value.parse().unwrap_or(0),
            "analytical_path" => manifest.analytical_paths.push(data_dir.join(value)),
            "warehouse_path" => manifest.warehouse_paths.push(data_dir.join(value)),
            _ => {}
        }
    }

    Ok(Some(manifest))
}

pub fn marker_manifest_is_valid(data_dir: &Path, wiki: &str, source_id: &str) -> Result<bool> {
    let Some(manifest) = read_marker_manifest(data_dir, wiki, source_id)? else {
        return Ok(false);
    };
    if manifest.rows == 0 {
        return Ok(true);
    }
    if manifest.analytical_paths.is_empty() || manifest.warehouse_paths.is_empty() {
        return Ok(false);
    }
    Ok(manifest
        .analytical_paths
        .iter()
        .chain(&manifest.warehouse_paths)
        .all(|path| path.exists()))
}

pub fn month_partition_dir(root: &Path, year: i32, year_month: &str) -> PathBuf {
    root.join(format!("year={year}"))
        .join(format!("year_month={year_month}"))
}

pub fn collect_parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_parquet_files_recursive(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_parquet_files_recursive(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == MARKERS_DIRNAME)
            {
                continue;
            }
            collect_parquet_files_recursive(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }

    Ok(())
}

pub fn collect_partition_specs(root: &Path) -> Result<Vec<PartitionSpec>> {
    let mut partitions: BTreeMap<(i32, String), PathBuf> = BTreeMap::new();
    collect_partition_specs_recursive(root, &mut partitions)?;
    Ok(partitions
        .into_iter()
        .map(|((year, year_month), dir)| PartitionSpec {
            year,
            year_month,
            dir,
        })
        .collect())
}

fn collect_partition_specs_recursive(
    root: &Path,
    partitions: &mut BTreeMap<(i32, String), PathBuf>,
) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for year_entry in fs::read_dir(root)? {
        let year_entry = year_entry?;
        if !year_entry.file_type()?.is_dir() {
            continue;
        }
        let year_path = year_entry.path();
        let year_name = year_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if year_name == MARKERS_DIRNAME {
            continue;
        }
        let Some(year) = year_name
            .strip_prefix("year=")
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };

        for month_entry in fs::read_dir(&year_path)? {
            let month_entry = month_entry?;
            if !month_entry.file_type()?.is_dir() {
                continue;
            }
            let month_path = month_entry.path();
            let month_name = month_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let Some(year_month) = month_name.strip_prefix("year_month=") else {
                continue;
            };

            partitions.insert((year, year_month.to_string()), month_path);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn collect_parquet_files_recurses_and_skips_markers() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("_markers"))?;
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-01"))?;
        fs::write(root.join("_markers").join("skip.parquet"), b"")?;
        let parquet_path = root
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        fs::write(parquet_path, b"")?;

        let files = collect_parquet_files(root)?;
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("part-0.parquet"));
        Ok(())
    }

    #[test]
    fn collect_partition_specs_discovers_partition_dirs() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-01"))?;
        fs::create_dir_all(root.join("year=2023").join("year_month=2023-12"))?;

        let partitions = collect_partition_specs(root)?;
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].year, 2023);
        assert_eq!(partitions[0].year_month, "2023-12");
        assert_eq!(partitions[1].year, 2024);
        assert_eq!(partitions[1].year_month, "2024-01");
        Ok(())
    }

    #[test]
    fn collect_partition_specs_returns_empty_for_missing_root() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let partitions = collect_partition_specs(&temp_dir.path().join("missing"))?;
        assert!(partitions.is_empty());
        Ok(())
    }

    #[test]
    fn collect_partition_specs_skips_invalid_entries() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("_markers"))?;
        fs::write(root.join("root-file.txt"), b"ignored")?;
        fs::create_dir_all(root.join("year=bad"))?;
        fs::create_dir_all(root.join("year=2024"))?;
        fs::write(root.join("year=2024").join("month.txt"), b"ignored")?;
        fs::create_dir_all(root.join("year=2024").join("bad-month"))?;
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-03"))?;

        let partitions = collect_partition_specs(root)?;
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].year, 2024);
        assert_eq!(partitions[0].year_month, "2024-03");
        Ok(())
    }

    #[test]
    fn marker_path_lives_under_analytical_markers_dir() {
        let marker = marker_path(Path::new("data"), "frwiki", "source");
        assert_eq!(
            marker,
            Path::new("data/parquet/frwiki/_markers/source.done")
        );
    }

    #[test]
    fn marker_manifest_round_trips_relative_paths() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let analytical = analytical_wiki_dir(temp_dir.path(), "frwiki")
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        let warehouse = warehouse_wiki_dir(temp_dir.path(), "frwiki")
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        let manifest = MarkerManifest {
            source: Some("raw/source.tsv.bz2".to_string()),
            rows: 12,
            analytical_paths: vec![analytical.clone()],
            warehouse_paths: vec![warehouse.clone()],
        };

        write_marker_manifest(temp_dir.path(), "frwiki", "source", &manifest)?;
        let loaded = read_marker_manifest(temp_dir.path(), "frwiki", "source")?
            .expect("marker should exist");

        assert_eq!(loaded, manifest);
        Ok(())
    }

    #[test]
    fn marker_manifest_validation_requires_both_output_layers() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let analytical = analytical_wiki_dir(temp_dir.path(), "frwiki")
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        let warehouse = warehouse_wiki_dir(temp_dir.path(), "frwiki")
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        analytical.parent().map(fs::create_dir_all).transpose()?;
        warehouse.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&analytical, b"a")?;
        fs::write(&warehouse, b"w")?;

        let manifest = MarkerManifest {
            source: None,
            rows: 1,
            analytical_paths: vec![analytical.clone()],
            warehouse_paths: vec![warehouse.clone()],
        };
        write_marker_manifest(root, "frwiki", "source", &manifest)?;
        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);

        fs::remove_file(&warehouse)?;
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn marker_manifest_allows_zero_row_sources_without_outputs() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = MarkerManifest {
            source: None,
            rows: 0,
            analytical_paths: Vec::new(),
            warehouse_paths: Vec::new(),
        };
        write_marker_manifest(root, "frwiki", "source", &manifest)?;

        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn read_marker_manifest_ignores_invalid_lines() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let marker = marker_path(root, "frwiki", "source");
        marker.parent().map(fs::create_dir_all).transpose()?;
        let contents = "source=raw/source.tsv.bz2\nthis-is-invalid\nrows=0\nwarehouse_parts=0\n";
        fs::write(&marker, contents)?;

        let manifest = read_marker_manifest(root, "frwiki", "source")?.expect("marker exists");

        assert_eq!(manifest.source.as_deref(), Some("raw/source.tsv.bz2"));
        assert_eq!(manifest.rows, 0);
        Ok(())
    }

    #[test]
    fn marker_manifest_is_invalid_when_rows_exist_without_output_paths() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = MarkerManifest {
            source: None,
            rows: 2,
            analytical_paths: Vec::new(),
            warehouse_paths: Vec::new(),
        };
        write_marker_manifest(root, "frwiki", "source", &manifest)?;

        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }
}
