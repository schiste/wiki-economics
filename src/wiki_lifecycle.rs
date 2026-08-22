use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

pub fn published_wikis(path: Option<&Path>) -> Result<Option<BTreeSet<String>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read wiki lifecycle registry {}", path.display()))?;
    let registry: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse wiki lifecycle registry {}", path.display()))?;
    ensure!(
        registry.get("schema_version").and_then(Value::as_u64) == Some(1),
        "wiki lifecycle registry {} must use schema_version 1",
        path.display()
    );
    let entries = registry
        .get("wikis")
        .and_then(Value::as_object)
        .with_context(|| {
            format!(
                "wiki lifecycle registry {} has no wikis object",
                path.display()
            )
        })?;
    let mut published = BTreeSet::new();
    for (wiki, entry) in entries {
        let publication = entry
            .get("publication")
            .and_then(Value::as_str)
            .with_context(|| format!("wiki lifecycle entry {wiki} has no publication state"))?;
        ensure!(
            matches!(publication, "published" | "hidden" | "retired"),
            "wiki lifecycle entry {wiki} has invalid publication state {publication}"
        );
        if publication == "published" {
            published.insert(wiki.clone());
        }
    }
    Ok(Some(published))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn publication_filter_is_optional_and_selects_only_published_wikis() -> Result<()> {
        assert_eq!(published_wikis(None)?, None);
        let temp_dir = TestDir::new()?;
        let path = temp_dir.path().join("lifecycle.json");
        let registry = br#"{"schema_version":1,"wikis":{"nlwiki":{"publication":"published"},"frwiki":{"publication":"hidden"},"oldwiki":{"publication":"retired"}}}"#;
        fs::write(&path, registry)?;
        assert_eq!(
            published_wikis(Some(&path))?,
            Some(BTreeSet::from(["nlwiki".to_string()]))
        );
        Ok(())
    }

    #[test]
    fn publication_filter_fails_closed_on_invalid_registries() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let path = temp_dir.path().join("lifecycle.json");
        let cases = [
            (b"{".as_slice(), "failed to parse"),
            (br#"{"schema_version":2,"wikis":{}}"#, "schema_version 1"),
            (br#"{"schema_version":1}"#, "no wikis object"),
            (
                br#"{"schema_version":1,"wikis":{"nlwiki":{}}}"#,
                "no publication state",
            ),
            (
                br#"{"schema_version":1,"wikis":{"nlwiki":{"publication":"gone"}}}"#,
                "invalid publication state",
            ),
        ];
        for (contents, message) in cases {
            fs::write(&path, contents)?;
            let error = published_wikis(Some(&path)).expect_err("invalid registry must fail");
            assert!(error.to_string().contains(message));
        }
        let missing = temp_dir.path().join("missing.json");
        assert!(published_wikis(Some(&missing)).is_err());
        Ok(())
    }
}
