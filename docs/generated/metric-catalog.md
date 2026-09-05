# Generated metric catalog

<!-- Generated from src/metric_registry.rs by `wiki-econ metric-catalog`. Do not edit by hand. -->

The tables below are deterministic projections of the canonical Rust metric registry.

## Publication, receipts, fingerprints, and browser layout

| Metric | Family / algorithm | Publication | Receipt contract | Fingerprint identity | Browser partitioning |
| --- | --- | --- | --- | --- | --- |
| `business_funnel` | `lifecycle` / `editor-lifecycle-v3-explicit-identified-registered-editors` | merged + per-wiki | date: cohort_year; order: wiki-major/v1; conserve: — | `business_funnel.parquet` | per-wiki files + global year shards |
| `gdp` | `monthly` / `monthly-stateless-v5-exact-period-inequality` | merged + per-wiki | date: year_month; order: wiki-major/v1; conserve: total_edits | `gdp.parquet` | per-wiki files + global year shards |
| `gdp_activity_tiers` | `activity_tiers` / `activity-tiers-v5-exclusive-period-user-type` | merged + per-wiki | date: period_start; order: wiki-major/v1; conserve: total_edits | `gdp_activity_tiers.parquet` | per-wiki files + global year shards |
| `gdp_user_type_share` | `monthly` / `monthly-stateless-v5-exact-period-inequality` | merged + per-wiki | date: year_month; order: wiki-major/v1; conserve: edits | `gdp_user_type_share.parquet` | per-wiki files + global year shards |
| `inequality` | `monthly` / `monthly-stateless-v5-exact-period-inequality` | merged + per-wiki | date: period_start; order: wiki-major/v1; conserve: — | `inequality.parquet` | per-wiki files + global year shards |
| `labor_churn` | `lifecycle` / `editor-lifecycle-v3-explicit-identified-registered-editors` | merged + per-wiki | date: period; order: wiki-major/v1; conserve: — | `labor_churn.parquet` | per-wiki files + global year shards |
| `labor_cohorts` | `lifecycle` / `editor-lifecycle-v3-explicit-identified-registered-editors` | merged + per-wiki | date: year; order: wiki-major/v1; conserve: — | `labor_cohorts.parquet` | per-wiki files + global year shards |
| `labor_monthly` | `monthly` / `monthly-stateless-v5-exact-period-inequality` | merged + per-wiki | date: year_month; order: wiki-major/v1; conserve: total_edits | `labor_monthly.parquet` | per-wiki files + global year shards |
| `page_weekly_edits` | `page_week` / `page-week-v2-governed-parallel-buckets` | per-wiki only | date: week_start; order: stable-page-hash-bucket/page-key/week/v1; conserve: edits | `page_weekly_edits.parquet` | Rust defaults only |
| `patrol` | `patrol` / `patrol-metrics-v5-complete-snapshot-months` | merged + per-wiki | date: year_month; order: wiki-major/v1; conserve: total_patrols | `patrol.parquet` | per-wiki files + global year shards |

## Schemas and aggregation semantics

| Metric | Schema | Aggregation contracts |
| --- | --- | --- |
| `business_funnel` | cohort_year:String, cohort_size:UInt32, reached_5:UInt32, reached_25:UInt32, reached_100:UInt32, wiki:String | distinct-at-grain: cohort_size, reached_5, reached_25, reached_100 @ wiki + cohort_year |
| `gdp` | year_month:String, page_namespace:Int32, user_type:String, gross_bytes_added:Int64, net_bytes:Int64, total_edits:UInt32, productive_edits:UInt32, reverted_edits:UInt32, unique_editors:UInt32, minor_edits:UInt32, bytes_per_edit:Float64, bytes_per_editor:Float64, revert_rate:Float64, wiki:String | additive: gross_bytes_added, net_bytes, total_edits, productive_edits, reverted_edits, minor_edits; distinct-at-grain: unique_editors @ wiki + year_month + page_namespace + user_type; ratio: bytes_per_edit ← (net_bytes) / total_edits; ratio: bytes_per_editor ← (net_bytes) / unique_editors; ratio: revert_rate ← (reverted_edits) / total_edits |
| `gdp_activity_tiers` | year_month:String, period:String, period_start:String, period_end:String, period_type:String, period_months:UInt32, user_type:String, activity_tier:String, tier_rank:UInt32, editors:UInt32, total_edits:UInt32, net_bytes:Int64, gross_bytes:Int64, wiki:String | additive: total_edits, net_bytes, gross_bytes; distinct-at-grain: editors @ wiki + period + period_type + user_type + activity_tier |
| `gdp_user_type_share` | year_month:String, user_type:String, edits:UInt32, net_bytes:Int64, editors:UInt32, wiki:String | additive: edits, net_bytes; distinct-at-grain: editors @ wiki + year_month + user_type |
| `inequality` | year_month:String, period:String, period_start:String, period_end:String, period_type:String, period_months:UInt32, user_type:String, gini:Float64, theil:Float64, palma:Float64, min_editors_50pct:UInt32, total_editors:UInt32, total_edits:UInt32, wiki:String | additive: total_edits; distinct-at-grain: total_editors @ wiki + period + period_type + user_type; sufficient-statistic: theil from theil + total_edits + total_editors; non-composable: gini, palma, min_editors_50pct |
| `labor_churn` | period:String, active_editors:UInt32, arrivals:UInt32, departures:UInt32, period_type:String, arrival_rate:Float64, departure_rate:Float64, wiki:String | distinct-at-grain: active_editors, arrivals, departures @ wiki + period + period_type; ratio: arrival_rate ← (arrivals) / active_editors; ratio: departure_rate ← (departures) / active_editors |
| `labor_cohorts` | cohort_year:String, year:String, survived_editors:UInt32, initial_editors:UInt32, wiki:String | distinct-at-grain: survived_editors, initial_editors @ wiki + cohort_year + year |
| `labor_monthly` | year_month:String, page_namespace:Int32, user_type:String, unique_editors:UInt32, total_edits:UInt32, net_bytes:Int64, reverted_edits:UInt32, wiki:String | additive: total_edits, net_bytes, reverted_edits; distinct-at-grain: unique_editors @ wiki + year_month + page_namespace + user_type |
| `page_weekly_edits` | week_start:String, iso_year:Int32, iso_week:Int32, page_id:Int64, page_title:String, page_namespace:Int32, edits:UInt32, previous_week_edits:UInt32, wow_change:Int64, wow_rate:Float64, wiki:String | additive: edits; non-composable: previous_week_edits, wow_change, wow_rate |
| `patrol` | year_month:String, wiki:String, page_namespace:Int32, user_type:String, total_patrols:Int64, unique_patrollers:Int32, patrol_new_pages:Int64, patrol_diffs:Int64, median_latency_hours:Float64, p90_latency_hours:Float64, patrolled_revisions:Int64, autopatrolled_revisions:Int64, total_revisions:Int64, patrol_coverage_pct:Float64, adjusted_coverage_pct:Float64, top1_pct:Float64, min_patrollers_50pct:Int32 | additive: total_patrols, patrol_new_pages, patrol_diffs, patrolled_revisions, autopatrolled_revisions, total_revisions; non-composable: unique_patrollers; ratio: patrol_coverage_pct ← (patrolled_revisions) / total_revisions; ratio: adjusted_coverage_pct ← (patrolled_revisions + autopatrolled_revisions) / total_revisions; non-composable: median_latency_hours, p90_latency_hours, top1_pct, min_patrollers_50pct |
