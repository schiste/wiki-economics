/// Semantic version for monthly stateless aggregates.
///
/// Increment this when GDP, GDP user-type share, inequality, or monthly labor
/// semantics change. Physical scan scheduling alone does not require a bump.
pub(crate) const ALGORITHM_VERSION: &str =
    "monthly-stateless-v4-historical-actor-suppressed-accounting";

pub(crate) const METRICS: [&str; 4] = ["gdp", "gdp_user_type_share", "inequality", "labor_monthly"];
