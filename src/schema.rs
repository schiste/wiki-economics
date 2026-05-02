/// MediaWiki History Dump schema (76 columns)
/// Reference: https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Edits/MediaWiki_history_dumps
use polars::prelude::*;

/// Column names in TSV order. The dump has no header row.
pub const COLUMNS: &[&str] = &[
    // Event global fields (0–4)
    "wiki_db",
    "event_entity",
    "event_type",
    "event_timestamp",
    "event_comment",
    // Event user fields (5–25)
    "event_user_id",
    "event_user_central_id",
    "event_user_text_historical",
    "event_user_text",
    "event_user_blocks_historical",
    "event_user_blocks",
    "event_user_groups_historical",
    "event_user_groups",
    "event_user_is_bot_by_historical",
    "event_user_is_bot_by",
    "event_user_is_created_by_self",
    "event_user_is_created_by_system",
    "event_user_is_created_by_peer",
    "event_user_is_anonymous",
    "event_user_is_temporary",
    "event_user_is_permanent",
    "event_user_registration_timestamp",
    "event_user_creation_timestamp",
    "event_user_first_edit_timestamp",
    "event_user_revision_count",
    "event_user_seconds_since_previous_revision",
    // Page fields (26–38)
    "page_id",
    "page_title_historical",
    "page_title",
    "page_namespace_historical",
    "page_namespace_is_content_historical",
    "page_namespace",
    "page_namespace_is_content",
    "page_is_redirect",
    "page_is_deleted",
    "page_creation_timestamp",
    "page_first_edit_timestamp",
    "page_revision_count",
    "page_seconds_since_previous_revision",
    // User fields (39–57)
    "user_id",
    "user_central_id",
    "user_text_historical",
    "user_text",
    "user_blocks_historical",
    "user_blocks",
    "user_groups_historical",
    "user_groups",
    "user_is_bot_by_historical",
    "user_is_bot_by",
    "user_is_created_by_self",
    "user_is_created_by_system",
    "user_is_created_by_peer",
    "user_is_anonymous",
    "user_is_temporary",
    "user_is_permanent",
    "user_registration_timestamp",
    "user_creation_timestamp",
    "user_first_edit_timestamp",
    // Revision fields (58–75)
    "revision_id",
    "revision_parent_id",
    "revision_minor_edit",
    "revision_deleted_parts",
    "revision_deleted_parts_are_suppressed",
    "revision_text_bytes",
    "revision_text_bytes_diff",
    "revision_text_sha1",
    "revision_content_model",
    "revision_content_format",
    "revision_is_deleted_by_page_deletion",
    "revision_deleted_by_page_deletion_timestamp",
    "revision_is_identity_reverted",
    "revision_first_identity_reverting_revision_id",
    "revision_seconds_to_identity_revert",
    "revision_is_identity_revert",
    "revision_is_from_before_page_creation",
    "revision_tags",
];

/// Columns we read from TSV during ingest before filtering to revision-create rows.
pub const INGEST_COLUMNS: &[&str] = &[
    "wiki_db",
    "event_entity",
    "event_type",
    "event_timestamp",
    "event_user_id",
    "event_user_text",
    "event_user_is_bot_by",
    "event_user_is_anonymous",
    "event_user_is_temporary",
    "event_user_registration_timestamp",
    "event_user_first_edit_timestamp",
    "page_id",
    "page_title",
    "page_namespace",
    "page_namespace_is_content",
    "page_is_redirect",
    "revision_id",
    "revision_parent_id",
    "revision_minor_edit",
    "revision_text_bytes",
    "revision_text_bytes_diff",
    "revision_is_identity_reverted",
    "revision_is_identity_revert",
    "revision_tags",
];

/// Richer normalized revision-create warehouse layer.
pub const WAREHOUSE_COLUMNS: &[&str] = &[
    "wiki_db",
    "event_timestamp",
    "event_user_id",
    "event_user_text",
    "event_user_is_bot_by",
    "event_user_is_anonymous",
    "event_user_is_temporary",
    "event_user_registration_timestamp",
    "event_user_first_edit_timestamp",
    "page_id",
    "page_title",
    "page_namespace",
    "page_namespace_is_content",
    "page_is_redirect",
    "revision_id",
    "revision_parent_id",
    "revision_minor_edit",
    "revision_text_bytes",
    "revision_text_bytes_diff",
    "revision_is_identity_reverted",
    "revision_is_identity_revert",
    "revision_tags",
    "year_month",
    "year",
    "year_month_key",
    "user_type",
    "is_reverted",
    "is_minor",
];

/// Ultra-slim analytical layer used directly by the compute pipeline.
pub const ANALYTICAL_COLUMNS: &[&str] = &[
    "year_month",
    "year",
    "year_month_key",
    "user_type",
    "event_user_id",
    "page_namespace",
    "revision_id",
    "revision_text_bytes_diff",
    "is_reverted",
    "is_minor",
];

/// Build the full Polars schema for reading TSV dumps.
/// Everything is read as String/Utf8 initially — we cast during ingest.
pub fn dump_schema() -> Schema {
    let mut schema = Schema::default();
    for &col in COLUMNS {
        schema.insert(col.into(), DataType::String);
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_contains(columns: &[&str], required: &str, label: &str) {
        assert!(columns.contains(&required), "{label} missing {required}");
    }

    #[test]
    fn schema_lists_expected_column_count() {
        assert_eq!(COLUMNS.len(), 76);
    }

    #[test]
    fn keep_columns_cover_required_metrics_inputs() {
        for required in [
            "event_timestamp",
            "event_user_id",
            "page_namespace",
            "revision_id",
            "revision_text_bytes_diff",
        ] {
            assert!(INGEST_COLUMNS.contains(&required), "missing {required}");
        }
    }

    #[test]
    fn warehouse_and_analytical_columns_match_pipeline_contracts() {
        for required in ["year_month", "year_month_key", "user_type", "is_reverted"] {
            assert_contains(WAREHOUSE_COLUMNS, required, "warehouse");
        }
        for required in [
            "year_month",
            "year",
            "year_month_key",
            "user_type",
            "event_user_id",
            "page_namespace",
            "revision_id",
            "revision_text_bytes_diff",
            "is_reverted",
            "is_minor",
        ] {
            assert_contains(ANALYTICAL_COLUMNS, required, "analytical");
        }
    }

    #[test]
    fn dump_schema_marks_every_column_as_string() {
        let schema = dump_schema();

        for &column in COLUMNS {
            assert_eq!(schema.get(column), Some(&DataType::String));
        }
    }
}
