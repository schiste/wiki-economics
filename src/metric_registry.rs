use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub(crate) enum MetricId {
    BusinessFunnel,
    Gdp,
    GdpActivityTiers,
    GdpUserTypeShare,
    Inequality,
    LaborChurn,
    LaborCohorts,
    LaborMonthly,
    PageWeeklyEdits,
    Patrol,
}

impl MetricId {
    pub(crate) const ALL: [Self; 10] = [
        Self::BusinessFunnel,
        Self::Gdp,
        Self::GdpActivityTiers,
        Self::GdpUserTypeShare,
        Self::Inequality,
        Self::LaborChurn,
        Self::LaborCohorts,
        Self::LaborMonthly,
        Self::PageWeeklyEdits,
        Self::Patrol,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BusinessFunnel => "business_funnel",
            Self::Gdp => "gdp",
            Self::GdpActivityTiers => "gdp_activity_tiers",
            Self::GdpUserTypeShare => "gdp_user_type_share",
            Self::Inequality => "inequality",
            Self::LaborChurn => "labor_churn",
            Self::LaborCohorts => "labor_cohorts",
            Self::LaborMonthly => "labor_monthly",
            Self::PageWeeklyEdits => "page_weekly_edits",
            Self::Patrol => "patrol",
        }
    }

    pub(crate) fn parquet_name(self) -> String {
        format!("{}.parquet", self.as_str())
    }

    pub(crate) fn from_artifact_identity(identity: &str) -> Option<Self> {
        let name = identity.rsplit('/').next().unwrap_or(identity);
        name.strip_suffix(".parquet")?.parse().ok()
    }

    pub(crate) const fn definition(self) -> &'static MetricDefinition {
        &METRIC_DEFINITIONS[self as usize]
    }
}

impl fmt::Display for MetricId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MetricId {
    type Err = UnknownMetricId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|metric| metric.as_str() == value)
            .ok_or_else(|| UnknownMetricId(value.to_owned()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnknownMetricId(String);

impl fmt::Display for UnknownMetricId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown metric {:?}", self.0)
    }
}

impl std::error::Error for UnknownMetricId {}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetricFamily {
    Monthly,
    ActivityTiers,
    Lifecycle,
    PageWeek,
    Patrol,
}

impl MetricFamily {
    pub(crate) const CORE: [Self; 4] = [
        Self::Monthly,
        Self::ActivityTiers,
        Self::Lifecycle,
        Self::PageWeek,
    ];
    pub(crate) const ALL: [Self; 5] = [
        Self::Monthly,
        Self::ActivityTiers,
        Self::Lifecycle,
        Self::PageWeek,
        Self::Patrol,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Monthly => "monthly",
            Self::ActivityTiers => "activity_tiers",
            Self::Lifecycle => "lifecycle",
            Self::PageWeek => "page_week",
            Self::Patrol => "patrol",
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        self.as_str()
    }

    pub(crate) const fn metrics(self) -> &'static [&'static str] {
        match self {
            Self::Monthly => &["gdp", "gdp_user_type_share", "inequality", "labor_monthly"],
            Self::ActivityTiers => &["gdp_activity_tiers"],
            Self::Lifecycle => &["business_funnel", "labor_cohorts", "labor_churn"],
            Self::PageWeek => &["page_weekly_edits"],
            Self::Patrol => &["patrol"],
        }
    }

    pub(crate) fn for_metric(metric: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| family.metrics().contains(&metric))
    }

    pub(crate) const fn base_algorithm_version(self) -> &'static str {
        match self {
            Self::Monthly => crate::compute::monthly::ALGORITHM_VERSION,
            Self::ActivityTiers => crate::compute::activity::ALGORITHM_VERSION,
            Self::Lifecycle => crate::compute::lifecycle::ALGORITHM_VERSION,
            Self::PageWeek => crate::compute::weekly::ALGORITHM_VERSION,
            Self::Patrol => crate::patrol::algorithm_version(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FieldKind {
    String,
    I32,
    I64,
    U32,
    F64,
}

impl FieldKind {
    pub(crate) const fn parquet_name(self) -> &'static str {
        match self {
            Self::String => "String",
            Self::I32 => "Int32",
            Self::I64 => "Int64",
            Self::U32 => "UInt32",
            Self::F64 => "Float64",
        }
    }
}

pub(crate) type FieldDefinition = (&'static str, FieldKind);

const fn field(name: &'static str, kind: FieldKind) -> FieldDefinition {
    (name, kind)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OrderingContract {
    WikiMajor,
    StablePageHashBucketPageKeyWeek,
}

impl OrderingContract {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::WikiMajor => "wiki-major/v1",
            Self::StablePageHashBucketPageKeyWeek => "stable-page-hash-bucket/page-key/week/v1",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicationScope {
    MergedAndPerWiki,
    PerWikiOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BrowserPartitioning {
    PerWikiAndGlobalYearShards,
    RustDefaultsOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum AggregationSemantics {
    Additive,
    Ratio {
        numerators: &'static [&'static str],
        denominator: &'static str,
    },
    DistinctAtGrain {
        grain: &'static [&'static str],
    },
    SufficientStatistic {
        components: &'static [&'static str],
    },
    NonComposable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AggregationRule {
    pub(crate) columns: &'static [&'static str],
    pub(crate) semantics: AggregationSemantics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MetricDefinition {
    pub(crate) id: MetricId,
    pub(crate) name: &'static str,
    pub(crate) family: MetricFamily,
    pub(crate) algorithm_version: &'static str,
    pub(crate) schema: &'static [FieldDefinition],
    pub(crate) date_column: Option<&'static str>,
    pub(crate) ordering: OrderingContract,
    pub(crate) conservation_column: Option<&'static str>,
    pub(crate) publication_scope: PublicationScope,
    pub(crate) browser_partitioning: BrowserPartitioning,
    pub(crate) aggregation: &'static [AggregationRule],
}

const BUSINESS_SCHEMA: &[FieldDefinition] = &[
    field("cohort_year", FieldKind::String),
    field("cohort_size", FieldKind::U32),
    field("reached_5", FieldKind::U32),
    field("reached_25", FieldKind::U32),
    field("reached_100", FieldKind::U32),
    field("wiki", FieldKind::String),
];
const GDP_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("page_namespace", FieldKind::I32),
    field("user_type", FieldKind::String),
    field("gross_bytes_added", FieldKind::I64),
    field("net_bytes", FieldKind::I64),
    field("total_edits", FieldKind::U32),
    field("productive_edits", FieldKind::U32),
    field("reverted_edits", FieldKind::U32),
    field("unique_editors", FieldKind::U32),
    field("minor_edits", FieldKind::U32),
    field("bytes_per_edit", FieldKind::F64),
    field("bytes_per_editor", FieldKind::F64),
    field("revert_rate", FieldKind::F64),
    field("wiki", FieldKind::String),
];
const GDP_TIERS_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("period", FieldKind::String),
    field("period_start", FieldKind::String),
    field("period_end", FieldKind::String),
    field("period_type", FieldKind::String),
    field("period_months", FieldKind::U32),
    field("user_type", FieldKind::String),
    field("activity_tier", FieldKind::String),
    field("tier_rank", FieldKind::U32),
    field("editors", FieldKind::U32),
    field("total_edits", FieldKind::U32),
    field("net_bytes", FieldKind::I64),
    field("gross_bytes", FieldKind::I64),
    field("wiki", FieldKind::String),
];
const GDP_SHARE_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("user_type", FieldKind::String),
    field("edits", FieldKind::U32),
    field("net_bytes", FieldKind::I64),
    field("editors", FieldKind::U32),
    field("wiki", FieldKind::String),
];
const INEQUALITY_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("period", FieldKind::String),
    field("period_start", FieldKind::String),
    field("period_end", FieldKind::String),
    field("period_type", FieldKind::String),
    field("period_months", FieldKind::U32),
    field("user_type", FieldKind::String),
    field("gini", FieldKind::F64),
    field("theil", FieldKind::F64),
    field("palma", FieldKind::F64),
    field("min_editors_50pct", FieldKind::U32),
    field("total_editors", FieldKind::U32),
    field("total_edits", FieldKind::U32),
    field("wiki", FieldKind::String),
];
const CHURN_SCHEMA: &[FieldDefinition] = &[
    field("period", FieldKind::String),
    field("active_editors", FieldKind::U32),
    field("arrivals", FieldKind::U32),
    field("departures", FieldKind::U32),
    field("period_type", FieldKind::String),
    field("arrival_rate", FieldKind::F64),
    field("departure_rate", FieldKind::F64),
    field("wiki", FieldKind::String),
];
const COHORTS_SCHEMA: &[FieldDefinition] = &[
    field("cohort_year", FieldKind::String),
    field("year", FieldKind::String),
    field("survived_editors", FieldKind::U32),
    field("initial_editors", FieldKind::U32),
    field("wiki", FieldKind::String),
];
const LABOR_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("page_namespace", FieldKind::I32),
    field("user_type", FieldKind::String),
    field("unique_editors", FieldKind::U32),
    field("total_edits", FieldKind::U32),
    field("net_bytes", FieldKind::I64),
    field("reverted_edits", FieldKind::U32),
    field("wiki", FieldKind::String),
];
const WEEKLY_SCHEMA: &[FieldDefinition] = &[
    field("week_start", FieldKind::String),
    field("iso_year", FieldKind::I32),
    field("iso_week", FieldKind::I32),
    field("page_id", FieldKind::I64),
    field("page_title", FieldKind::String),
    field("page_namespace", FieldKind::I32),
    field("edits", FieldKind::U32),
    field("previous_week_edits", FieldKind::U32),
    field("wow_change", FieldKind::I64),
    field("wow_rate", FieldKind::F64),
    field("wiki", FieldKind::String),
];
const PATROL_SCHEMA: &[FieldDefinition] = &[
    field("year_month", FieldKind::String),
    field("wiki", FieldKind::String),
    field("page_namespace", FieldKind::I32),
    field("user_type", FieldKind::String),
    field("total_patrols", FieldKind::I64),
    field("unique_patrollers", FieldKind::I32),
    field("patrol_new_pages", FieldKind::I64),
    field("patrol_diffs", FieldKind::I64),
    field("median_latency_hours", FieldKind::F64),
    field("p90_latency_hours", FieldKind::F64),
    field("patrolled_revisions", FieldKind::I64),
    field("autopatrolled_revisions", FieldKind::I64),
    field("total_revisions", FieldKind::I64),
    field("patrol_coverage_pct", FieldKind::F64),
    field("adjusted_coverage_pct", FieldKind::F64),
    field("top1_pct", FieldKind::F64),
    field("min_patrollers_50pct", FieldKind::I32),
];

const WIKI_MONTH_NAMESPACE_TYPE: &[&str] = &["wiki", "year_month", "page_namespace", "user_type"];
const WIKI_MONTH_TYPE: &[&str] = &["wiki", "year_month", "user_type"];
const WIKI_PERIOD_TYPE: &[&str] = &["wiki", "period", "period_type", "user_type"];
const BUSINESS_AGGREGATION: &[AggregationRule] = &[AggregationRule {
    columns: &["cohort_size", "reached_5", "reached_25", "reached_100"],
    semantics: AggregationSemantics::DistinctAtGrain {
        grain: &["wiki", "cohort_year"],
    },
}];
const GDP_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &[
            "gross_bytes_added",
            "net_bytes",
            "total_edits",
            "productive_edits",
            "reverted_edits",
            "minor_edits",
        ],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["unique_editors"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: WIKI_MONTH_NAMESPACE_TYPE,
        },
    },
    AggregationRule {
        columns: &["bytes_per_edit"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["net_bytes"],
            denominator: "total_edits",
        },
    },
    AggregationRule {
        columns: &["bytes_per_editor"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["net_bytes"],
            denominator: "unique_editors",
        },
    },
    AggregationRule {
        columns: &["revert_rate"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["reverted_edits"],
            denominator: "total_edits",
        },
    },
];
const ACTIVITY_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["total_edits", "net_bytes", "gross_bytes"],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["editors"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: &[
                "wiki",
                "period",
                "period_type",
                "user_type",
                "activity_tier",
            ],
        },
    },
];
const SHARE_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["edits", "net_bytes"],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["editors"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: WIKI_MONTH_TYPE,
        },
    },
];
const INEQUALITY_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["total_edits"],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["total_editors"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: WIKI_PERIOD_TYPE,
        },
    },
    AggregationRule {
        columns: &["theil"],
        semantics: AggregationSemantics::SufficientStatistic {
            components: &["theil", "total_edits", "total_editors"],
        },
    },
    AggregationRule {
        columns: &["gini", "palma", "min_editors_50pct"],
        semantics: AggregationSemantics::NonComposable,
    },
];
const CHURN_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["active_editors", "arrivals", "departures"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: &["wiki", "period", "period_type"],
        },
    },
    AggregationRule {
        columns: &["arrival_rate"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["arrivals"],
            denominator: "active_editors",
        },
    },
    AggregationRule {
        columns: &["departure_rate"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["departures"],
            denominator: "active_editors",
        },
    },
];
const COHORTS_AGGREGATION: &[AggregationRule] = &[AggregationRule {
    columns: &["survived_editors", "initial_editors"],
    semantics: AggregationSemantics::DistinctAtGrain {
        grain: &["wiki", "cohort_year", "year"],
    },
}];
const LABOR_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["total_edits", "net_bytes", "reverted_edits"],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["unique_editors"],
        semantics: AggregationSemantics::DistinctAtGrain {
            grain: WIKI_MONTH_NAMESPACE_TYPE,
        },
    },
];
const WEEKLY_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &["edits"],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["previous_week_edits", "wow_change", "wow_rate"],
        semantics: AggregationSemantics::NonComposable,
    },
];
const PATROL_AGGREGATION: &[AggregationRule] = &[
    AggregationRule {
        columns: &[
            "total_patrols",
            "patrol_new_pages",
            "patrol_diffs",
            "patrolled_revisions",
            "autopatrolled_revisions",
            "total_revisions",
        ],
        semantics: AggregationSemantics::Additive,
    },
    AggregationRule {
        columns: &["unique_patrollers"],
        semantics: AggregationSemantics::NonComposable,
    },
    AggregationRule {
        columns: &["patrol_coverage_pct"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["patrolled_revisions"],
            denominator: "total_revisions",
        },
    },
    AggregationRule {
        columns: &["adjusted_coverage_pct"],
        semantics: AggregationSemantics::Ratio {
            numerators: &["patrolled_revisions", "autopatrolled_revisions"],
            denominator: "total_revisions",
        },
    },
    AggregationRule {
        columns: &[
            "median_latency_hours",
            "p90_latency_hours",
            "top1_pct",
            "min_patrollers_50pct",
        ],
        semantics: AggregationSemantics::NonComposable,
    },
];

pub(crate) const METRIC_DEFINITIONS: [MetricDefinition; 10] = [
    MetricDefinition {
        id: MetricId::BusinessFunnel,
        name: "business_funnel",
        family: MetricFamily::Lifecycle,
        algorithm_version: crate::compute::lifecycle::ALGORITHM_VERSION,
        schema: BUSINESS_SCHEMA,
        date_column: Some("cohort_year"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: None,
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: BUSINESS_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::Gdp,
        name: "gdp",
        family: MetricFamily::Monthly,
        algorithm_version: crate::compute::monthly::ALGORITHM_VERSION,
        schema: GDP_SCHEMA,
        date_column: Some("year_month"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: Some("total_edits"),
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: GDP_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::GdpActivityTiers,
        name: "gdp_activity_tiers",
        family: MetricFamily::ActivityTiers,
        algorithm_version: crate::compute::activity::ALGORITHM_VERSION,
        schema: GDP_TIERS_SCHEMA,
        date_column: Some("period_start"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: Some("total_edits"),
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: ACTIVITY_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::GdpUserTypeShare,
        name: "gdp_user_type_share",
        family: MetricFamily::Monthly,
        algorithm_version: crate::compute::monthly::ALGORITHM_VERSION,
        schema: GDP_SHARE_SCHEMA,
        date_column: Some("year_month"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: Some("edits"),
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: SHARE_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::Inequality,
        name: "inequality",
        family: MetricFamily::Monthly,
        algorithm_version: crate::compute::monthly::ALGORITHM_VERSION,
        schema: INEQUALITY_SCHEMA,
        date_column: Some("period_start"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: None,
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: INEQUALITY_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::LaborChurn,
        name: "labor_churn",
        family: MetricFamily::Lifecycle,
        algorithm_version: crate::compute::lifecycle::ALGORITHM_VERSION,
        schema: CHURN_SCHEMA,
        date_column: Some("period"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: None,
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: CHURN_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::LaborCohorts,
        name: "labor_cohorts",
        family: MetricFamily::Lifecycle,
        algorithm_version: crate::compute::lifecycle::ALGORITHM_VERSION,
        schema: COHORTS_SCHEMA,
        date_column: Some("year"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: None,
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: COHORTS_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::LaborMonthly,
        name: "labor_monthly",
        family: MetricFamily::Monthly,
        algorithm_version: crate::compute::monthly::ALGORITHM_VERSION,
        schema: LABOR_SCHEMA,
        date_column: Some("year_month"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: Some("total_edits"),
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: LABOR_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::PageWeeklyEdits,
        name: "page_weekly_edits",
        family: MetricFamily::PageWeek,
        algorithm_version: crate::compute::weekly::ALGORITHM_VERSION,
        schema: WEEKLY_SCHEMA,
        date_column: Some("week_start"),
        ordering: OrderingContract::StablePageHashBucketPageKeyWeek,
        conservation_column: Some("edits"),
        publication_scope: PublicationScope::PerWikiOnly,
        browser_partitioning: BrowserPartitioning::RustDefaultsOnly,
        aggregation: WEEKLY_AGGREGATION,
    },
    MetricDefinition {
        id: MetricId::Patrol,
        name: "patrol",
        family: MetricFamily::Patrol,
        algorithm_version: crate::patrol::algorithm_version(),
        schema: PATROL_SCHEMA,
        date_column: Some("year_month"),
        ordering: OrderingContract::WikiMajor,
        conservation_column: Some("total_patrols"),
        publication_scope: PublicationScope::MergedAndPerWiki,
        browser_partitioning: BrowserPartitioning::PerWikiAndGlobalYearShards,
        aggregation: PATROL_AGGREGATION,
    },
];

pub(crate) fn definitions() -> impl ExactSizeIterator<Item = &'static MetricDefinition> {
    METRIC_DEFINITIONS.iter()
}

pub(crate) const CATALOG_JSON_PATH: &str = "config/generated/metric-catalog.json";
pub(crate) const CATALOG_MARKDOWN_PATH: &str = "docs/generated/metric-catalog.md";
const CATALOG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct MetricCatalogDocument {
    schema_version: u32,
    metrics: Vec<MetricCatalogEntry>,
}

#[derive(Debug, Serialize)]
struct MetricCatalogEntry {
    id: MetricId,
    family: MetricFamily,
    algorithm_version: String,
    schema: Vec<CatalogField>,
    aggregation: Vec<CatalogAggregationRule>,
    publication: CatalogPublication,
    receipt: CatalogReceipt,
    fingerprint: CatalogFingerprint,
    browser: CatalogBrowser,
}

#[derive(Debug, Serialize)]
struct CatalogField {
    name: String,
    data_type: FieldKind,
}

#[derive(Debug, Serialize)]
struct CatalogAggregationRule {
    columns: Vec<String>,
    #[serde(flatten)]
    semantics: AggregationSemantics,
}

#[derive(Debug, Serialize)]
struct CatalogPublication {
    scope: PublicationScope,
    per_wiki_artifact: String,
    merged_artifact: Option<String>,
}

#[derive(Debug, Serialize)]
struct CatalogReceipt {
    date_column: Option<String>,
    ordering_contract: String,
    conservation_columns: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CatalogFingerprint {
    artifact_identity: String,
    family: MetricFamily,
    algorithm_version: String,
}

#[derive(Debug, Serialize)]
struct CatalogBrowser {
    partitioning: BrowserPartitioning,
    per_wiki_path: Option<String>,
    global_path: Option<String>,
}

fn catalog_document() -> MetricCatalogDocument {
    let metrics = definitions()
        .map(|definition| {
            let artifact = definition.id.parquet_name();
            let browser_enabled =
                definition.browser_partitioning == BrowserPartitioning::PerWikiAndGlobalYearShards;
            MetricCatalogEntry {
                id: definition.id,
                family: definition.family,
                algorithm_version: definition.algorithm_version.to_string(),
                schema: definition
                    .schema
                    .iter()
                    .map(|(name, kind)| CatalogField {
                        name: (*name).to_string(),
                        data_type: *kind,
                    })
                    .collect(),
                aggregation: definition
                    .aggregation
                    .iter()
                    .map(|rule| CatalogAggregationRule {
                        columns: rule
                            .columns
                            .iter()
                            .map(|column| (*column).to_string())
                            .collect(),
                        semantics: rule.semantics,
                    })
                    .collect(),
                publication: CatalogPublication {
                    scope: definition.publication_scope,
                    per_wiki_artifact: format!("{{wiki}}/{artifact}"),
                    merged_artifact: (definition.publication_scope
                        == PublicationScope::MergedAndPerWiki)
                        .then_some(artifact.clone()),
                },
                receipt: CatalogReceipt {
                    date_column: definition.date_column.map(str::to_string),
                    ordering_contract: definition.ordering.as_str().to_string(),
                    conservation_columns: definition
                        .conservation_column
                        .map(|column| vec![column.to_string()])
                        .unwrap_or_default(),
                },
                fingerprint: CatalogFingerprint {
                    artifact_identity: artifact,
                    family: definition.family,
                    algorithm_version: definition.algorithm_version.to_string(),
                },
                browser: CatalogBrowser {
                    partitioning: definition.browser_partitioning,
                    per_wiki_path: browser_enabled
                        .then(|| format!("browser-data/{}/{{wiki}}.parquet", definition.name)),
                    global_path: browser_enabled
                        .then(|| format!("browser-data/{}/all-{{year}}.parquet", definition.name)),
                },
            }
        })
        .collect();
    MetricCatalogDocument {
        schema_version: CATALOG_SCHEMA_VERSION,
        metrics,
    }
}

pub(crate) fn catalog_json() -> Result<Vec<u8>> {
    let mut rendered = serde_json::to_vec_pretty(&catalog_document())?;
    rendered.push(b'\n');
    Ok(rendered)
}

fn aggregation_label(rule: &AggregationRule) -> String {
    let columns = rule.columns.join(", ");
    match rule.semantics {
        AggregationSemantics::Additive => format!("additive: {columns}"),
        AggregationSemantics::Ratio {
            numerators,
            denominator,
        } => format!(
            "ratio: {columns} ← ({}) / {denominator}",
            numerators.join(" + ")
        ),
        AggregationSemantics::DistinctAtGrain { grain } => {
            format!("distinct-at-grain: {columns} @ {}", grain.join(" + "))
        }
        AggregationSemantics::SufficientStatistic { components } => format!(
            "sufficient-statistic: {columns} from {}",
            components.join(" + ")
        ),
        AggregationSemantics::NonComposable => format!("non-composable: {columns}"),
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

pub(crate) fn catalog_markdown() -> String {
    let mut output = String::from(
        "# Generated metric catalog\n\n<!-- Generated from src/metric_registry.rs by `wiki-econ metric-catalog`. Do not edit by hand. -->\n\nThe tables below are deterministic projections of the canonical Rust metric registry.\n\n## Publication, receipts, fingerprints, and browser layout\n\n| Metric | Family / algorithm | Publication | Receipt contract | Fingerprint identity | Browser partitioning |\n| --- | --- | --- | --- | --- | --- |\n",
    );
    for definition in definitions() {
        let publication = match definition.publication_scope {
            PublicationScope::MergedAndPerWiki => "merged + per-wiki",
            PublicationScope::PerWikiOnly => "per-wiki only",
        };
        let browser = match definition.browser_partitioning {
            BrowserPartitioning::PerWikiAndGlobalYearShards => {
                "per-wiki files + global year shards"
            }
            BrowserPartitioning::RustDefaultsOnly => "Rust defaults only",
        };
        let receipt = format!(
            "date: {}; order: {}; conserve: {}",
            definition.date_column.unwrap_or("—"),
            definition.ordering.as_str(),
            definition.conservation_column.unwrap_or("—")
        );
        output.push_str(&format!(
            "| `{}` | `{}` / `{}` | {} | {} | `{}` | {} |\n",
            definition.name,
            definition.family.as_str(),
            definition.algorithm_version,
            publication,
            markdown_cell(&receipt),
            definition.id.parquet_name(),
            browser,
        ));
    }
    output.push_str(
        "\n## Schemas and aggregation semantics\n\n| Metric | Schema | Aggregation contracts |\n| --- | --- | --- |\n",
    );
    for definition in definitions() {
        let schema = definition
            .schema
            .iter()
            .map(|(name, kind)| format!("{name}:{}", kind.parquet_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let aggregation = definition
            .aggregation
            .iter()
            .map(aggregation_label)
            .collect::<Vec<_>>()
            .join("; ");
        output.push_str(&format!(
            "| `{}` | {} | {} |\n",
            definition.name,
            markdown_cell(&schema),
            markdown_cell(&aggregation),
        ));
    }
    output
}

fn generated_paths(workspace_dir: &Path) -> Result<[(PathBuf, Vec<u8>); 2]> {
    Ok([
        (workspace_dir.join(CATALOG_JSON_PATH), catalog_json()?),
        (
            workspace_dir.join(CATALOG_MARKDOWN_PATH),
            catalog_markdown().into_bytes(),
        ),
    ])
}

pub(crate) fn sync_generated_catalog(workspace_dir: &Path, check: bool) -> Result<()> {
    for (path, expected) in generated_paths(workspace_dir)? {
        if check {
            let actual = fs::read(&path).with_context(|| {
                format!("generated metric catalog is missing: {}", path.display())
            })?;
            ensure!(
                actual == expected,
                "generated metric catalog is stale: {}; run `cargo run --locked -- metric-catalog`",
                path.display()
            );
            continue;
        }
        let parent = path
            .parent()
            .context("generated metric catalog path has no parent")?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .context("generated metric catalog path is not UTF-8")?,
            std::process::id()
        ));
        let result = (|| -> Result<()> {
            let mut file = File::create(&temporary)?;
            file.write_all(&expected)?;
            file.sync_all()?;
            fs::rename(&temporary, &path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use std::collections::BTreeSet;

    #[test]
    fn registry_is_total_unique_and_round_trips_typed_ids() {
        assert_eq!(METRIC_DEFINITIONS.len(), MetricId::ALL.len());
        let mut names = BTreeSet::new();
        for (index, metric) in MetricId::ALL.into_iter().enumerate() {
            let definition = metric.definition();
            assert_eq!(definition.id, metric);
            assert_eq!(definition.name, metric.as_str());
            assert_eq!(METRIC_DEFINITIONS[index].id, metric);
            assert!(names.insert(metric.as_str()));
            assert_eq!(metric.as_str().parse::<MetricId>().unwrap(), metric);
            assert_eq!(
                MetricId::from_artifact_identity(&format!("nlwiki/{}.parquet", metric)),
                Some(metric)
            );
            assert!(!definition.schema.is_empty());
            let schema_names = definition
                .schema
                .iter()
                .map(|field| field.0)
                .collect::<BTreeSet<_>>();
            assert_eq!(schema_names.len(), definition.schema.len());
            assert!(!definition.algorithm_version.is_empty());
            assert!(!definition.aggregation.is_empty());
        }
        assert!("not_a_metric".parse::<MetricId>().is_err());
        assert_eq!(MetricId::from_artifact_identity("manifest.json"), None);
    }

    #[test]
    fn every_measure_column_has_exactly_one_aggregation_contract() {
        for definition in definitions() {
            let schema = definition
                .schema
                .iter()
                .map(|field| field.0)
                .collect::<BTreeSet<_>>();
            let dimensions = definition
                .aggregation
                .iter()
                .flat_map(|rule| rule.columns)
                .copied()
                .collect::<Vec<_>>();
            let unique = dimensions.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(unique.len(), dimensions.len(), "{}", definition.id);
            assert!(unique.iter().all(|column| schema.contains(column)));
            for rule in definition.aggregation {
                match rule.semantics {
                    AggregationSemantics::Ratio {
                        numerators,
                        denominator,
                    } => {
                        assert!(!numerators.is_empty());
                        assert!(numerators.iter().all(|column| schema.contains(column)));
                        assert!(schema.contains(denominator));
                    }
                    AggregationSemantics::DistinctAtGrain { grain } => {
                        assert!(!grain.is_empty());
                        assert!(grain.iter().all(|column| schema.contains(column)));
                    }
                    AggregationSemantics::SufficientStatistic { components } => {
                        assert!(!components.is_empty());
                        assert!(components.iter().all(|column| schema.contains(column)));
                    }
                    AggregationSemantics::Additive | AggregationSemantics::NonComposable => {}
                }
            }
        }
    }

    #[test]
    fn family_membership_and_publication_scope_are_canonical() {
        assert_eq!(MetricFamily::Monthly.metrics().len(), 4);
        assert_eq!(MetricFamily::ActivityTiers.metrics().len(), 1);
        assert_eq!(MetricFamily::Lifecycle.metrics().len(), 3);
        assert_eq!(MetricFamily::PageWeek.metrics().len(), 1);
        assert_eq!(MetricFamily::Patrol.metrics().len(), 1);
        for definition in definitions() {
            assert_eq!(
                definition.algorithm_version,
                definition.family.base_algorithm_version()
            );
            assert!(definition.family.metrics().contains(&definition.name));
        }
        assert_eq!(
            MetricId::PageWeeklyEdits.definition().publication_scope,
            PublicationScope::PerWikiOnly
        );
        assert_eq!(
            MetricId::PageWeeklyEdits.definition().browser_partitioning,
            BrowserPartitioning::RustDefaultsOnly
        );
    }

    #[test]
    fn semantic_edge_cases_are_explicit() {
        let gdp = MetricId::Gdp.definition();
        assert!(gdp.aggregation.iter().any(|rule| {
            rule.columns == ["bytes_per_edit"]
                && rule.semantics
                    == AggregationSemantics::Ratio {
                        numerators: &["net_bytes"],
                        denominator: "total_edits",
                    }
        }));
        let inequality = MetricId::Inequality.definition();
        assert!(inequality.aggregation.iter().any(|rule| {
            rule.columns.contains(&"theil")
                && matches!(
                    rule.semantics,
                    AggregationSemantics::SufficientStatistic { .. }
                )
        }));
        let patrol = MetricId::Patrol.definition();
        assert!(patrol.aggregation.iter().any(|rule| {
            rule.columns.contains(&"median_latency_hours")
                && rule.semantics == AggregationSemantics::NonComposable
        }));
    }

    #[test]
    fn generated_catalog_projects_every_runtime_contract_deterministically() -> Result<()> {
        let first = catalog_json()?;
        assert_eq!(first, catalog_json()?);
        let document: serde_json::Value = serde_json::from_slice(&first)?;
        let metrics = document["metrics"]
            .as_array()
            .context("catalog metrics should be an array")?;
        assert_eq!(metrics.len(), MetricId::ALL.len());
        let weekly = metrics
            .iter()
            .find(|metric| metric["id"] == "page_weekly_edits")
            .context("weekly metric should be catalogued")?;
        assert_eq!(weekly["publication"]["scope"], "per_wiki_only");
        assert_eq!(weekly["receipt"]["conservation_columns"][0], "edits");
        assert_eq!(weekly["browser"]["partitioning"], "rust_defaults_only");
        assert_eq!(
            weekly["fingerprint"]["algorithm_version"],
            crate::compute::weekly::ALGORITHM_VERSION
        );
        let markdown = catalog_markdown();
        assert_eq!(markdown, catalog_markdown());
        assert!(markdown.contains("## Publication, receipts, fingerprints, and browser layout"));
        assert!(markdown.contains("sufficient-statistic: theil"));
        assert!(markdown.contains("non-composable: median_latency_hours"));
        Ok(())
    }

    #[test]
    fn generated_catalog_write_and_check_are_fail_closed() -> Result<()> {
        let workspace = TestDir::new()?;
        assert!(sync_generated_catalog(workspace.path(), true).is_err());
        sync_generated_catalog(workspace.path(), false)?;
        sync_generated_catalog(workspace.path(), true)?;

        let json = workspace.path().join(CATALOG_JSON_PATH);
        fs::write(&json, b"{}")?;
        assert!(sync_generated_catalog(workspace.path(), true).is_err());

        fs::remove_file(&json)?;
        fs::create_dir(&json)?;
        assert!(sync_generated_catalog(workspace.path(), false).is_err());
        let parent = json
            .parent()
            .context("catalog fixture should have a parent")?;
        assert!(
            !parent
                .join(format!(
                    ".{}.{}.tmp",
                    json.file_name()
                        .and_then(|name| name.to_str())
                        .context("catalog fixture should be UTF-8")?,
                    std::process::id()
                ))
                .exists()
        );
        Ok(())
    }
}
