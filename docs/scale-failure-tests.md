# Scale-oriented failure tests

The scale suite protects boundedness and transaction semantics with compact
fixtures. It does not allocate an enwiki-sized dataset. The following tests are
the canonical evidence for each production failure mode.

| Invariant | Regression evidence |
|---|---|
| Monthly discovery crosses a year boundary | `snapshot_plan::tests::monthly_inventory_crosses_year_boundary_and_keeps_partial_final_month` |
| A partial final month remains planned | `fetch::tests::build_file_list_supports_monthly_wikis_through_partial_final_month` |
| Missing, duplicated, or reordered sources fail closed | `snapshot_plan::tests::plan_validation_rejects_missing_duplicate_and_reordered_sources` and `fetch::tests::monthly_snapshot_completion_rejects_a_missing_expected_month` |
| Interrupted ranged downloads resume at the exact byte offset | `fetch::tests::download_file_retries_after_partial_read_and_resumes` |
| A kill after Parquet rename but before marker publication rebuilds only that source | `ingest::tests::source_restart_recovers_parquets_renamed_before_marker_publication` |
| A kill before Parquet rename removes abandoned transaction temporaries | `ingest::tests::source_restart_removes_parquet_temporaries_left_before_rename` |
| Source-window restart adopts the single unambiguous partial input | `fetch::tests::source_window_adopts_and_resumes_an_interrupted_download` and `fetch::tests::source_window_recovery_rejects_ambiguous_inputs_and_adopts_final_files` |
| Active Parquet writers never exceed the configured cap | `compute::tests::large_logical_topology_caps_writers_and_reclaims_scratch_per_unit` and `compute::tests::two_level_routing_rejects_more_secondary_writers_than_budgeted` |
| Logical bucket count may greatly exceed active writers | `compute::tests::large_logical_topology_caps_writers_and_reclaims_scratch_per_unit` exercises 2,048 logical buckets with a 32-writer peak |
| Two-level output equals the flat implementation | `compute::tests::two_level_weekly_buckets_match_flat_output_and_conserve_every_level` |
| Source concurrency does not change Parquet bytes | `ingest::tests::source_concurrency_does_not_change_fragment_bytes` runs the real monthly ingest with one and two Rayon workers |
| Completed secondary scratch is reclaimed one unit at a time | `compute::tests::large_logical_topology_caps_writers_and_reclaims_scratch_per_unit` |
| Disk reserve exhaustion stops before starting another source | `source_window::tests::disk_reserve_exhaustion_stops_after_committed_source_without_finalizing` |
| Candidate validation failure preserves the current generation | `source_window::tests::candidate_validation_failure_preserves_current_generation_pointer` |
| Site publication failure preserves the existing release | `site/build-site.test.cjs`: `site builds are switched atomically and failed staging is discarded` |
| Complete generation rollover never mixes histories | `end_to_end_tests::snapshot_rollover_computes_only_the_new_generation` |

When adding a new large-wiki execution path, extend this table with a compact
invariant test before relying on a capacity run. Capacity benchmarks measure
headroom; they do not replace deterministic failure tests.
