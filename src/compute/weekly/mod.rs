/// Semantic version for page-week reduction, reconciliation, and lag values.
/// The selected deterministic bucket topology is appended to this value.
pub(crate) const ALGORITHM_VERSION: &str = "page-week-v1-two-level-bounded";
pub(crate) const CONTRIBUTION_ALGORITHM_VERSION: &str =
    "page-week-month-contribution-v1-monday-boundary";

pub(crate) const METRICS: [&str; 1] = ["page_weekly_edits"];
