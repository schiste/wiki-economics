use super::*;

const INCREMENTAL_ALGORITHM_VERSION: &str = PATROL_COMPUTE_ALGORITHM_VERSION;
const RIGHTS_STATE_VERSION: &str = "patrol-rights-state-v1";
const SOURCE_INDEX_VERSION: &str = "patrol-source-reference-index-v1";
const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
const SOURCE_INDEX_SCHEMA_VERSION: u32 = 1;
const ABSENT_MONTH_DIGEST: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

macro_rules! patrol_try {
    ($operation:expr) => {
        match $operation {
            Ok(value) => value,
            Err(error) => return Err(error.into()),
        }
    };
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RightsCheckpoint {
    schema_version: u32,
    through_month: String,
    rights_prefix_digest: String,
    groups_digest: String,
    active_users: Vec<String>,
    state_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceMonthIndex {
    schema_version: u32,
    wiki: String,
    action_month: String,
    patrol_artifact_sha256: String,
    revision_dependencies: BTreeMap<String, String>,
    events_input_digest: String,
    rows: u64,
    unresolved_revision_ids: u64,
    index_sha256: String,
}

impl SourceMonthIndex {
    fn canonical_hash(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"wiki-economics\0patrol-source-month-index\0");
        digest.update(self.schema_version.to_be_bytes());
        for value in [
            self.wiki.as_str(),
            self.action_month.as_str(),
            self.patrol_artifact_sha256.as_str(),
            self.events_input_digest.as_str(),
        ] {
            update_digest_string(&mut digest, value);
        }
        for (month, identity) in &self.revision_dependencies {
            update_digest_string(&mut digest, month);
            update_digest_string(&mut digest, identity);
        }
        digest.update(self.rows.to_be_bytes());
        digest.update(self.unresolved_revision_ids.to_be_bytes());
        hex::encode(digest.finalize())
    }

    fn validate_identity(&self, wiki: &str, action_month: &str, patrol_sha256: &str) -> Result<()> {
        anyhow::ensure!(
            self.schema_version == SOURCE_INDEX_SCHEMA_VERSION
                && self.wiki == wiki
                && self.action_month == action_month
                && self.patrol_artifact_sha256 == patrol_sha256
                && self.events_input_digest.len() == 64
                && self
                    .events_input_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                && self.index_sha256 == self.canonical_hash(),
            "patrol source-month index identity changed"
        );
        Ok(())
    }

    fn dependencies_match(&self, revision_digests: &BTreeMap<String, String>) -> bool {
        self.unresolved_revision_ids == 0
            && self
                .revision_dependencies
                .iter()
                .all(|(month, digest)| revision_digests.get(month) == Some(digest))
    }
}

impl RightsCheckpoint {
    fn new(
        through_month: &str,
        rights_prefix_digest: &str,
        groups_digest: &str,
        active_users: &BTreeSet<String>,
    ) -> Self {
        let mut checkpoint = Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            through_month: through_month.to_string(),
            rights_prefix_digest: rights_prefix_digest.to_string(),
            groups_digest: groups_digest.to_string(),
            active_users: active_users.iter().cloned().collect(),
            state_sha256: String::new(),
        };
        checkpoint.state_sha256 = checkpoint.canonical_hash();
        checkpoint
    }

    fn validate(&self, month: &str, prefix: &str, groups: &str) -> Result<()> {
        anyhow::ensure!(
            self.schema_version == CHECKPOINT_SCHEMA_VERSION
                && self.through_month == month
                && self.rights_prefix_digest == prefix
                && self.groups_digest == groups
                && self.active_users.windows(2).all(|pair| pair[0] < pair[1]),
            "patrol rights checkpoint identity changed"
        );
        anyhow::ensure!(
            self.state_sha256 == self.canonical_hash(),
            "patrol rights checkpoint state hash changed"
        );
        Ok(())
    }

    fn canonical_hash(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"wiki-economics\0patrol-rights-checkpoint\0");
        digest.update(self.schema_version.to_be_bytes());
        for value in [
            self.through_month.as_str(),
            self.rights_prefix_digest.as_str(),
            self.groups_digest.as_str(),
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        for user in &self.active_users {
            digest.update((user.len() as u64).to_be_bytes());
            digest.update(user.as_bytes());
        }
        hex::encode(digest.finalize())
    }
}

pub(super) fn compute(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    output_dir: &Path,
    rebuild: bool,
    limit_months: Option<usize>,
) -> Result<()> {
    let generation = generation::load(data_dir, wiki, snapshot)?;
    let generation_root = generation::generation_dir(data_dir, wiki, snapshot)?;
    let warehouse_generation = storage::read_generation_manifest(data_dir, wiki, snapshot)?;
    let recordable_run = limit_months.is_none();
    let fingerprinted_run = !rebuild && recordable_run;
    let inputs = patrol_stage_inputs(wiki, data_dir, Some(snapshot))?;
    let outputs = patrol_stage_outputs(wiki, output_dir);
    let receipt_path = patrol_stage_receipt(output_dir, wiki);
    let spec = fingerprint::StageSpec {
        stage: "patrol_compute",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version: INCREMENTAL_ALGORITHM_VERSION,
    };
    if fingerprinted_run && fingerprint::reusable(&receipt_path, spec, &inputs, &outputs)? {
        crate::observability::record_stage_reused("patrol_compute", Some(wiki));
        info!(
            wiki,
            snapshot, "reusing generation-aware patrol compute stage"
        );
        return Ok(());
    }

    clear_patrol_parts_dir(output_dir, wiki)?;
    let all_revision_partitions = collect_partition_files_by_month(data_dir, wiki, Some(snapshot))?;
    let (revision_digests, cache) = if warehouse_generation.schema_version == 3 {
        let revision_inventory =
            crate::canonical_month::ensure_snapshot_inventory(data_dir, wiki, snapshot)?;
        let digests = revision_inventory
            .identities
            .iter()
            .map(|identity| (identity.event_month.clone(), identity.digest.clone()))
            .collect::<BTreeMap<_, _>>();
        (
            digests,
            crate::cross_snapshot::CrossSnapshotCache::new(data_dir, wiki, snapshot)?,
        )
    } else {
        warn!(
            wiki,
            snapshot,
            generation_schema = warehouse_generation.schema_version,
            "using snapshot-scoped patrol recovery for a pre-compaction generation"
        );
        (
            patrol_try!(snapshot_scoped_revision_digests(
                data_dir,
                wiki,
                snapshot,
                &warehouse_generation,
                &all_revision_partitions,
            )),
            crate::cross_snapshot::CrossSnapshotCache::snapshot_scoped(data_dir, wiki, snapshot)?,
        )
    };
    let patrol_artifacts = generation
        .patrol_months
        .iter()
        .map(|artifact| (artifact.event_month.clone(), artifact))
        .collect::<BTreeMap<_, _>>();
    let rights_artifacts = generation
        .rights_months
        .iter()
        .map(|artifact| (artifact.event_month.clone(), artifact))
        .collect::<BTreeMap<_, _>>();
    let mut output_months = revision_digests
        .keys()
        .chain(patrol_artifacts.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|month| crate::compute::snapshot_contains_complete_month(snapshot, month))
        .collect::<Vec<_>>();
    if let Some(limit) = limit_months {
        output_months.truncate(limit);
    }
    anyhow::ensure!(
        !output_months.is_empty(),
        "patrol generation has no computable months"
    );

    let last_output_month = output_months
        .last()
        .context("patrol output month is missing")?;
    let timeline_months = output_months
        .iter()
        .chain(
            rights_artifacts
                .keys()
                .filter(|month| month.as_str() <= last_output_month.as_str()),
        )
        .cloned()
        .collect::<BTreeSet<_>>();
    let all_revision_files = all_revision_partitions
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let source_indexes = patrol_try!(ensure_source_indexes(
        wiki,
        &generation_root,
        &patrol_artifacts,
        &revision_digests,
        &all_revision_partitions,
        &all_revision_files,
        &cache,
    ));
    let groups_digest =
        string_set_digest("patrol-autopatrol-groups-v1", &generation.autopatrol_groups);
    let groups = generation
        .autopatrol_groups
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let output_set = output_months.iter().cloned().collect::<HashSet<_>>();
    let mut rights_month_digests = Vec::<String>::new();
    let rights_prefixes = timeline_months
        .iter()
        .map(|month| {
            if let Some(artifact) = rights_artifacts.get(month) {
                rights_month_digests.push(artifact.artifact_sha256.clone());
            }
            let digest_refs = rights_month_digests
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let digest =
                cache.derived_digest("patrol_rights_prefix", RIGHTS_STATE_VERSION, &digest_refs);
            (month.clone(), digest)
        })
        .collect::<BTreeMap<_, _>>();
    let mut active_users = BTreeSet::new();
    let mut rights_state_month: Option<String> = None;

    for month in &timeline_months {
        let rights_prefix_digest = rights_prefixes
            .get(month)
            .context("rights prefix is missing")?;

        if output_set.contains(month) {
            let revision_digest = revision_digests
                .get(month)
                .map_or(ABSENT_MONTH_DIGEST, String::as_str);
            let action_digest = source_indexes
                .get(month)
                .map_or(ABSENT_MONTH_DIGEST, |index| {
                    index.events_input_digest.as_str()
                });
            let reference_digests = source_indexes
                .values()
                .filter(|index| index.revision_dependencies.contains_key(month))
                .map(|index| format!("{}:{}", index.action_month, index.events_input_digest))
                .collect::<Vec<_>>();
            let mut input_parts = vec![
                revision_digest,
                rights_prefix_digest,
                &groups_digest,
                action_digest,
            ];
            input_parts.extend(reference_digests.iter().map(String::as_str));
            let input_digest =
                cache.derived_digest("patrol_month", INCREMENTAL_ALGORITHM_VERSION, &input_parts);
            let cached = (!rebuild)
                .then(|| {
                    cache.load(
                        "patrol_month",
                        INCREMENTAL_ALGORITHM_VERSION,
                        &input_digest,
                        "patrol",
                    )
                })
                .transpose()?
                .flatten();
            let mut frame = if let Some(frame) = cached {
                if rights_state_month.is_some() {
                    patrol_try!(advance_state_through(
                        month,
                        &timeline_months,
                        &rights_artifacts,
                        &rights_prefixes,
                        &generation_root,
                        &groups,
                        &groups_digest,
                        &cache,
                        &mut active_users,
                        &mut rights_state_month,
                    ));
                }
                frame
            } else {
                if rights_state_month.is_none() {
                    restore_rights_checkpoint(
                        month,
                        &timeline_months,
                        &rights_prefixes,
                        &groups_digest,
                        &cache,
                        &mut active_users,
                        &mut rights_state_month,
                    )?;
                }
                let intervals = patrol_try!(advance_state_through(
                    month,
                    &timeline_months,
                    &rights_artifacts,
                    &rights_prefixes,
                    &generation_root,
                    &groups,
                    &groups_digest,
                    &cache,
                    &mut active_users,
                    &mut rights_state_month,
                ));
                let mut frame = patrol_try!(compute_month(
                    wiki,
                    month,
                    &source_indexes,
                    &cache,
                    &all_revision_partitions,
                    &intervals,
                ));
                patrol_try!(cache.store(
                    "patrol_month",
                    INCREMENTAL_ALGORITHM_VERSION,
                    &input_digest,
                    "patrol",
                    &mut frame,
                ));
                frame
            };
            write_month_part(output_dir, wiki, month, &mut frame)?;
        } else if rights_state_month.is_some() {
            patrol_try!(advance_state_through(
                month,
                &timeline_months,
                &rights_artifacts,
                &rights_prefixes,
                &generation_root,
                &groups,
                &groups_digest,
                &cache,
                &mut active_users,
                &mut rights_state_month,
            ));
        }
    }

    let merged_path = merge_wiki_patrol_parts(output_dir, wiki)?;
    refresh_patrol_dashboard_artifacts(output_dir, merged_path.as_deref())?;
    if recordable_run {
        record_patrol_stage(&receipt_path, spec, &inputs, wiki, output_dir)?;
    }
    let stats = cache.stats();
    info!(
        wiki,
        snapshot,
        reused_artifacts = stats.reused_artifacts,
        rebuilt_artifacts = stats.rebuilt_artifacts,
        missing_artifacts = stats.missing_artifacts,
        missing_receipts = stats.missing_receipts,
        "completed bounded generation-aware patrol computation"
    );
    Ok(())
}

fn snapshot_scoped_revision_digests(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    manifest: &storage::GenerationManifest,
    partitions: &BTreeMap<i32, Vec<PathBuf>>,
) -> Result<BTreeMap<String, String>> {
    let fragments = manifest
        .fragments
        .iter()
        .map(|fragment| (data_dir.join(&fragment.path), fragment))
        .collect::<BTreeMap<_, _>>();

    partitions
        .iter()
        .map(|(month_key, files)| {
            let month = format_year_month(*month_key);
            let mut digest = Sha256::new();
            digest.update(b"wiki-economics\0patrol-snapshot-scoped-revision-month\0v1\0");
            for value in [wiki, snapshot, month.as_str()] {
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value.as_bytes());
            }
            digest.update(manifest.schema_version.to_be_bytes());
            let mut files = files.clone();
            files.sort();
            for file in files {
                let fragment = patrol_try!(fragments.get(&file).with_context(|| {
                    format!(
                        "revision partition is absent from the generation manifest: {}",
                        file.display()
                    )
                }));
                for value in [fragment.source_id.as_str(), fragment.sha256.as_str()] {
                    digest.update((value.len() as u64).to_be_bytes());
                    digest.update(value.as_bytes());
                }
                digest.update(fragment.rows.to_be_bytes());
            }
            Ok((month, hex::encode(digest.finalize())))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn ensure_source_indexes(
    wiki: &str,
    generation_root: &Path,
    patrol_artifacts: &BTreeMap<String, &generation::MonthArtifact>,
    revision_digests: &BTreeMap<String, String>,
    all_revision_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    all_revision_files: &[PathBuf],
    cache: &crate::cross_snapshot::CrossSnapshotCache,
) -> Result<BTreeMap<String, SourceMonthIndex>> {
    let mut indexes = BTreeMap::new();
    let mut rebuild = Vec::new();
    let mut shared_fallback_ids = HashSet::new();

    for (month, artifact) in patrol_artifacts {
        if let Some(index) = patrol_try!(load_reusable_source_index(
            wiki,
            month,
            artifact,
            revision_digests,
            cache,
        )) {
            indexes.insert(month.clone(), index);
            continue;
        }

        let (_, revision_ids, revision_lookup) = patrol_try!(load_source_month_context(
            month,
            artifact,
            generation_root,
            all_revision_partitions,
        ));
        shared_fallback_ids.extend(
            revision_ids
                .into_iter()
                .filter(|revision_id| !revision_lookup.contains_key(revision_id)),
        );
        rebuild.push((month, *artifact));
    }

    let shared_fallback = patrol_try!(load_shared_revision_fallback(
        wiki,
        all_revision_files,
        &shared_fallback_ids,
        load_revision_subset_by_ids_once,
    ));
    info!(
        wiki,
        reusable_source_months = indexes.len(),
        rebuilt_source_months = rebuild.len(),
        shared_fallback_revision_ids = shared_fallback_ids.len(),
        shared_fallback_matches = shared_fallback.len(),
        "prepared bounded patrol source-index rebuild"
    );

    for (month, artifact) in rebuild {
        let index = patrol_try!(build_source_index(
            wiki,
            month,
            artifact,
            generation_root,
            revision_digests,
            all_revision_partitions,
            &shared_fallback,
            cache,
        ));
        indexes.insert(month.clone(), index);
    }
    Ok(indexes)
}

fn load_reusable_source_index(
    wiki: &str,
    action_month: &str,
    artifact: &generation::MonthArtifact,
    revision_digests: &BTreeMap<String, String>,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
) -> Result<Option<SourceMonthIndex>> {
    if let Some(index) = patrol_try!(cache.load_json::<SourceMonthIndex>(
        "patrol_source_pointer",
        SOURCE_INDEX_VERSION,
        &artifact.artifact_sha256,
        "index",
    )) {
        index.validate_identity(wiki, action_month, &artifact.artifact_sha256)?;
        if index.dependencies_match(revision_digests)
            && patrol_try!(cache.reusable(
                "patrol_source_events",
                SOURCE_INDEX_VERSION,
                &index.events_input_digest,
                "events",
            ))
        {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn load_source_month_context(
    action_month: &str,
    artifact: &generation::MonthArtifact,
    generation_root: &Path,
    all_revision_partitions: &BTreeMap<i32, Vec<PathBuf>>,
) -> Result<(DataFrame, HashSet<i64>, HashMap<i64, RevisionMeta>)> {
    let path = generation::artifact_path(generation_root, artifact)?;
    let patrol_df = read_parquet_df(&path, Some(patrol_projection()))?;
    let action_month_key =
        parse_year_month_key(action_month).context("invalid patrol source action month")?;
    let pending = HashSet::from([action_month_key]);
    let revision_ids = collect_patrolled_revision_ids(&patrol_df, &pending)?;
    let revision_lookup = patrol_try!(load_revision_subset_by_ids_near_pending_months(
        all_revision_partitions,
        &[action_month_key],
        &revision_ids,
    ));
    Ok((patrol_df, revision_ids, revision_lookup))
}

fn load_shared_revision_fallback<F>(
    wiki: &str,
    all_revision_files: &[PathBuf],
    revision_ids: &HashSet<i64>,
    mut load: F,
) -> Result<HashMap<i64, RevisionMeta>>
where
    F: FnMut(&[PathBuf], &HashSet<i64>) -> Result<HashMap<i64, RevisionMeta>>,
{
    if revision_ids.is_empty() {
        return Ok(HashMap::new());
    }
    info!(
        wiki,
        missing_revision_ids = revision_ids.len(),
        "performing one shared full revision lookup for patrol source months"
    );
    load(all_revision_files, revision_ids)
}

#[allow(clippy::too_many_arguments)]
fn build_source_index(
    wiki: &str,
    action_month: &str,
    artifact: &generation::MonthArtifact,
    generation_root: &Path,
    revision_digests: &BTreeMap<String, String>,
    all_revision_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    shared_fallback: &HashMap<i64, RevisionMeta>,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
) -> Result<SourceMonthIndex> {
    let (patrol_df, revision_ids, mut revision_lookup) = patrol_try!(load_source_month_context(
        action_month,
        artifact,
        generation_root,
        all_revision_partitions,
    ));

    extend_lookup_from_shared(&revision_ids, shared_fallback, &mut revision_lookup);
    let unresolved_revision_ids = patrol_try!(u64::try_from(
        revision_ids
            .iter()
            .filter(|revision_id| !revision_lookup.contains_key(revision_id))
            .count(),
    ));
    let mut revision_dependencies = revision_lookup
        .values()
        .map(|meta| format_year_month(meta.year_month_key))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|month| {
            let digest = revision_digests
                .get(&month)
                .with_context(|| format!("patrol reference resolved outside snapshot: {month}"))?;
            Ok((month, digest.clone()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    revision_dependencies.retain(|_, digest| !digest.is_empty());
    let mut digest_parts = vec![artifact.artifact_sha256.as_str()];
    let dependency_parts = revision_dependencies
        .iter()
        .map(|(month, digest)| format!("{month}:{digest}"))
        .collect::<Vec<_>>();
    digest_parts.extend(dependency_parts.iter().map(String::as_str));
    if unresolved_revision_ids > 0 {
        digest_parts.push("unresolved-revision-ids");
    }
    let events_input_digest =
        cache.derived_digest("patrol_source_events", SOURCE_INDEX_VERSION, &digest_parts);
    let mut events = enriched_source_events(&patrol_df, &revision_lookup)?;
    if !patrol_try!(cache.reusable(
        "patrol_source_events",
        SOURCE_INDEX_VERSION,
        &events_input_digest,
        "events",
    )) {
        patrol_try!(cache.store(
            "patrol_source_events",
            SOURCE_INDEX_VERSION,
            &events_input_digest,
            "events",
            &mut events,
        ));
    }
    let mut index = SourceMonthIndex {
        schema_version: SOURCE_INDEX_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        action_month: action_month.to_string(),
        patrol_artifact_sha256: artifact.artifact_sha256.clone(),
        revision_dependencies,
        events_input_digest,
        rows: u64::try_from(patrol_df.height())?,
        unresolved_revision_ids,
        index_sha256: String::new(),
    };
    index.index_sha256 = index.canonical_hash();
    patrol_try!(cache.store_json(
        "patrol_source_pointer",
        SOURCE_INDEX_VERSION,
        &artifact.artifact_sha256,
        "index",
        &index,
    ));
    Ok(index)
}

fn extend_lookup_from_shared(
    revision_ids: &HashSet<i64>,
    shared_fallback: &HashMap<i64, RevisionMeta>,
    revision_lookup: &mut HashMap<i64, RevisionMeta>,
) {
    for revision_id in revision_ids {
        if !revision_lookup.contains_key(revision_id)
            && let Some(meta) = shared_fallback.get(revision_id)
        {
            revision_lookup.insert(*revision_id, *meta);
        }
    }
}

fn enriched_source_events(
    patrol_df: &DataFrame,
    revision_lookup: &HashMap<i64, RevisionMeta>,
) -> Result<DataFrame> {
    let timestamps = patrol_df.column("timestamp")?.str()?;
    let revision_ids = patrol_df.column("current_revision_id")?.i64()?;
    let previous_ids = patrol_df.column("prev_revision_id")?.i64()?;
    let users = patrol_df.column("user")?.str()?;
    let mut timestamp = Vec::with_capacity(patrol_df.height());
    let mut current_revision_id = Vec::with_capacity(patrol_df.height());
    let mut prev_revision_id = Vec::with_capacity(patrol_df.height());
    let mut user = Vec::with_capacity(patrol_df.height());
    let mut revision_month = Vec::with_capacity(patrol_df.height());
    let mut revision_timestamp_seconds = Vec::with_capacity(patrol_df.height());
    let mut page_namespace = Vec::with_capacity(patrol_df.height());
    let mut revision_user_type = Vec::with_capacity(patrol_df.height());
    for row in 0..patrol_df.height() {
        let revision_id = revision_ids.get(row).unwrap_or_default();
        let meta = revision_lookup.get(&revision_id);
        timestamp.push(timestamps.get(row));
        current_revision_id.push(revision_id);
        prev_revision_id.push(previous_ids.get(row).unwrap_or_default());
        user.push(users.get(row));
        revision_month.push(meta.map(|meta| format_year_month(meta.year_month_key)));
        revision_timestamp_seconds.push(meta.map(|meta| meta.timestamp_seconds));
        page_namespace.push(meta.map(|meta| meta.page_namespace));
        revision_user_type.push(meta.map(|meta| meta.user_type.as_str()));
    }
    DataFrame::new_infer_height(vec![
        Column::new("timestamp".into(), timestamp),
        Column::new("current_revision_id".into(), current_revision_id),
        Column::new("prev_revision_id".into(), prev_revision_id),
        Column::new("user".into(), user),
        Column::new("revision_month".into(), revision_month),
        Column::new(
            "revision_timestamp_seconds".into(),
            revision_timestamp_seconds,
        ),
        Column::new("page_namespace".into(), page_namespace),
        Column::new("revision_user_type".into(), revision_user_type),
    ])
    .map_err(Into::into)
}

fn load_source_events(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    index: &SourceMonthIndex,
) -> Result<DataFrame> {
    let events = patrol_try!(cache.load(
        "patrol_source_events",
        SOURCE_INDEX_VERSION,
        &index.events_input_digest,
        "events",
    ))
    .context("patrol source-month events cache is missing")?;
    anyhow::ensure!(
        u64::try_from(events.height())? == index.rows,
        "patrol source-month index row count changed"
    );
    Ok(events)
}

fn source_event_revision_lookup(events: &DataFrame) -> Result<HashMap<i64, RevisionMeta>> {
    let revision_ids = events.column("current_revision_id")?.i64()?;
    let months = events.column("revision_month")?.str()?;
    let timestamps = events.column("revision_timestamp_seconds")?.i64()?;
    let namespaces = events.column("page_namespace")?.i32()?;
    let user_types = events.column("revision_user_type")?.str()?;
    let mut lookup = HashMap::new();
    for row in 0..events.height() {
        if let (
            Some(revision_id),
            Some(month),
            Some(timestamp_seconds),
            Some(page_namespace),
            Some(user_type),
        ) = (
            revision_ids.get(row),
            months.get(row),
            timestamps.get(row),
            namespaces.get(row),
            user_types.get(row),
        ) {
            lookup.insert(
                revision_id,
                RevisionMeta {
                    timestamp_seconds,
                    year_month_key: parse_year_month_key(month)
                        .context("invalid enriched patrol revision month")?,
                    page_namespace,
                    user_type: parse_user_type(user_type)?,
                },
            );
        }
    }
    Ok(lookup)
}

fn parse_user_type(value: &str) -> Result<UserType> {
    match value {
        "registered" => Ok(UserType::Registered),
        "anonymous" => Ok(UserType::Anonymous),
        "temporary" => Ok(UserType::Temporary),
        "bot" => Ok(UserType::Bot),
        _ => anyhow::bail!("invalid enriched patrol user type {value:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_state_through(
    target_month: &str,
    timeline_months: &BTreeSet<String>,
    rights_artifacts: &BTreeMap<String, &generation::MonthArtifact>,
    rights_prefixes: &BTreeMap<String, String>,
    generation_root: &Path,
    groups: &HashSet<&str>,
    groups_digest: &str,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    active_users: &mut BTreeSet<String>,
    rights_state_month: &mut Option<String>,
) -> Result<AutopatrolIntervals> {
    let mut target_intervals = HashMap::new();
    let start_after = rights_state_month.clone();
    for month in timeline_months
        .iter()
        .filter(|month| {
            start_after
                .as_deref()
                .is_none_or(|start| month.as_str() > start)
        })
        .take_while(|month| month.as_str() <= target_month)
    {
        let rights_path = rights_artifacts
            .get(month)
            .map(|artifact| generation::artifact_path(generation_root, artifact))
            .transpose()?;
        let intervals = advance_rights_month(rights_path.as_deref(), month, groups, active_users)?;
        if month == target_month {
            target_intervals = intervals;
        }
        if month.ends_with("-12") {
            patrol_try!(write_or_validate_checkpoint(
                month,
                rights_prefixes
                    .get(month)
                    .context("rights prefix is missing")?,
                groups_digest,
                cache,
                active_users,
            ));
        }
        *rights_state_month = Some(month.clone());
    }
    anyhow::ensure!(
        rights_state_month.as_deref() == Some(target_month),
        "rights state did not advance to patrol output month"
    );
    Ok(target_intervals)
}

fn restore_rights_checkpoint(
    target_month: &str,
    timeline_months: &BTreeSet<String>,
    rights_prefixes: &BTreeMap<String, String>,
    groups_digest: &str,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    active_users: &mut BTreeSet<String>,
    rights_state_month: &mut Option<String>,
) -> Result<()> {
    for month in timeline_months
        .iter()
        .rev()
        .filter(|month| month.as_str() < target_month && month.ends_with("-12"))
    {
        let prefix = rights_prefixes
            .get(month)
            .context("rights prefix is missing")?;
        let digest = checkpoint_digest(cache, month, prefix, groups_digest);
        let Some(checkpoint) = patrol_try!(cache.load_json::<RightsCheckpoint>(
            "patrol_rights_checkpoint",
            RIGHTS_STATE_VERSION,
            &digest,
            "state",
        )) else {
            continue;
        };
        checkpoint.validate(month, prefix, groups_digest)?;
        *active_users = checkpoint.active_users.into_iter().collect();
        *rights_state_month = Some(month.clone());
        return Ok(());
    }
    active_users.clear();
    *rights_state_month = None;
    Ok(())
}

fn write_or_validate_checkpoint(
    month: &str,
    rights_prefix_digest: &str,
    groups_digest: &str,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    active_users: &BTreeSet<String>,
) -> Result<()> {
    let digest = checkpoint_digest(cache, month, rights_prefix_digest, groups_digest);
    if let Some(checkpoint) = patrol_try!(cache.load_json::<RightsCheckpoint>(
        "patrol_rights_checkpoint",
        RIGHTS_STATE_VERSION,
        &digest,
        "state",
    )) {
        checkpoint.validate(month, rights_prefix_digest, groups_digest)?;
        anyhow::ensure!(
            checkpoint.active_users == active_users.iter().cloned().collect::<Vec<_>>(),
            "patrol rights checkpoint state changed"
        );
    } else {
        patrol_try!(cache.store_json(
            "patrol_rights_checkpoint",
            RIGHTS_STATE_VERSION,
            &digest,
            "state",
            &RightsCheckpoint::new(month, rights_prefix_digest, groups_digest, active_users),
        ));
    }
    Ok(())
}

fn checkpoint_digest(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    month: &str,
    rights_prefix_digest: &str,
    groups_digest: &str,
) -> String {
    cache.derived_digest(
        "patrol_rights_checkpoint",
        RIGHTS_STATE_VERSION,
        &[month, rights_prefix_digest, groups_digest],
    )
}

fn compute_month(
    wiki: &str,
    month: &str,
    source_indexes: &BTreeMap<String, SourceMonthIndex>,
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    all_revision_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    autopatrol_intervals: &AutopatrolIntervals,
) -> Result<DataFrame> {
    let month_key = parse_year_month_key(month).context("invalid patrol event month")?;
    let mut patrolled_ids = HashSet::new();
    for index in source_indexes
        .values()
        .filter(|index| index.revision_dependencies.contains_key(month))
    {
        let events = load_source_events(cache, index)?;
        let revision_ids = events.column("current_revision_id")?.i64()?;
        let revision_months = events.column("revision_month")?.str()?;
        for row in 0..events.height() {
            if revision_months.get(row) == Some(month)
                && let Some(revision_id) = revision_ids.get(row)
            {
                patrolled_ids.insert(revision_id);
            }
        }
    }
    let pending = HashSet::from([month_key]);
    let month_partitions = all_revision_partitions
        .get(&month_key)
        .map(|files| BTreeMap::from([(month_key, files.clone())]))
        .unwrap_or_default();
    let summary = patrol_try!(build_revision_summary(
        &month_partitions,
        &patrolled_ids,
        &pending,
        autopatrol_intervals,
    ));
    let patrol_stats = if let Some(index) = source_indexes.get(month) {
        let patrol_df = load_source_events(cache, index)?;
        let action_lookup = source_event_revision_lookup(&patrol_df)?;
        aggregate_patrol_stats(&patrol_df, &pending, &action_lookup)?
    } else {
        HashMap::new()
    };
    let rows = patrol_month_rows(month_key, &summary, &patrol_stats);
    patrol_metrics_frame(wiki, &rows)
}

fn advance_rights_month(
    rights_path: Option<&Path>,
    month: &str,
    autopatrol_groups: &HashSet<&str>,
    active_users: &mut BTreeSet<String>,
) -> Result<AutopatrolIntervals> {
    let (month_start, month_end) = month_bounds(month)?;
    let mut events = BTreeMap::<String, Vec<(i64, bool, bool)>>::new();
    if let Some(path) = rights_path {
        let df = read_parquet_df(path, None)?;
        let timestamps = df.column("timestamp")?.str()?;
        let users = df.column("target_user")?.str()?;
        let old_groups = df.column("old_groups")?.str()?;
        let new_groups = df.column("new_groups")?.str()?;
        let mut previous_timestamp = None;
        for row in 0..df.height() {
            let timestamp_text = timestamps
                .get(row)
                .context("rights event has no timestamp")?;
            anyhow::ensure!(
                timestamp_text.starts_with(month),
                "rights event is outside its monthly partition"
            );
            let timestamp = parse_timestamp_seconds(timestamp_text)
                .context("rights event has an invalid timestamp")?;
            anyhow::ensure!(
                previous_timestamp.is_none_or(|previous| previous <= timestamp),
                "rights month is not ordered chronologically"
            );
            previous_timestamp = Some(timestamp);
            let username = users.get(row).context("rights event has no target user")?;
            let old_has =
                split_groups(old_groups.get(row)).any(|group| autopatrol_groups.contains(group));
            let new_has =
                split_groups(new_groups.get(row)).any(|group| autopatrol_groups.contains(group));
            if old_has != new_has {
                events
                    .entry(username.to_string())
                    .or_default()
                    .push((timestamp, old_has, new_has));
            }
        }
    }

    let users = active_users
        .iter()
        .cloned()
        .chain(events.keys().cloned())
        .collect::<BTreeSet<_>>();
    let mut intervals = HashMap::new();
    for username in users {
        let mut start = active_users.contains(&username).then_some(month_start);
        let mut user_intervals = Vec::new();
        for (timestamp, old_has, new_has) in events.remove(&username).unwrap_or_default() {
            if old_has && start.is_none() {
                start = Some(month_start);
            } else if !old_has && let Some(opened) = start.take() {
                user_intervals.push((opened, Some(timestamp)));
            }
            if new_has && start.is_none() {
                start = Some(timestamp);
            } else if !new_has && let Some(opened) = start.take() {
                user_intervals.push((opened, Some(timestamp)));
            }
        }
        if let Some(opened) = start {
            user_intervals.push((opened, None));
            active_users.insert(username.clone());
        } else {
            active_users.remove(&username);
        }
        user_intervals.retain(|(start, end)| {
            *start < month_end && end.is_none_or(|end| end > month_start && end > *start)
        });
        if !user_intervals.is_empty() {
            intervals.insert(username, user_intervals);
        }
    }
    Ok(intervals)
}

fn month_bounds(month: &str) -> Result<(i64, i64)> {
    let month_key = parse_year_month_key(month).context("invalid rights checkpoint month")?;
    anyhow::ensure!(month.len() == 7 && month_key % 100 >= 1 && month_key % 100 <= 12);
    let next = shift_month_key(month_key, 1).context("rights checkpoint month overflow")?;
    let start = parse_timestamp_seconds(&format!("{month}-01 00:00:00"))
        .context("invalid rights month start")?;
    let end = parse_timestamp_seconds(&format!("{}-01 00:00:00", format_year_month(next)))
        .context("invalid rights month end")?;
    Ok((start, end))
}

fn write_month_part(
    output_dir: &Path,
    wiki: &str,
    month: &str,
    frame: &mut DataFrame,
) -> Result<()> {
    let month_key = parse_year_month_key(month).context("invalid patrol output month")?;
    let path = patrol_part_path(output_dir, wiki, month_key);
    ensure_parent_dir(&path)?;
    let temporary = path.with_extension("parquet.tmp");
    let mut file = File::create(&temporary)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(frame)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    File::open(path.parent().context("patrol output month has no parent")?)?.sync_all()?;
    Ok(())
}

fn string_set_digest(domain: &str, values: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    for value in values {
        update_digest_string(&mut digest, value);
    }
    hex::encode(digest.finalize())
}

fn update_digest_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_month::MonthIdentity;
    use crate::test_support::TestDir;

    fn cache(root: &Path, wiki: &str) -> crate::cross_snapshot::CrossSnapshotCache {
        crate::cross_snapshot::CrossSnapshotCache::for_test(root, wiki, Vec::<MonthIdentity>::new())
    }

    #[test]
    fn snapshot_scoped_revision_digests_reject_an_unlisted_partition() -> Result<()> {
        let root = TestDir::new()?;
        let manifest = storage::GenerationManifest {
            schema_version: 2,
            wiki: "legacywiki".to_string(),
            snapshot_version: "2026-08".to_string(),
            source_plan_sha256: "0".repeat(64),
            compaction_manifest_path: None,
            compaction_manifest_sha256: None,
            fragments: Vec::new(),
        };
        let missing = root.path().join("unlisted.parquet");
        let error = snapshot_scoped_revision_digests(
            root.path(),
            "legacywiki",
            "2026-08",
            &manifest,
            &BTreeMap::from([(202_608, vec![missing.clone()])]),
        )
        .expect_err("an unlisted revision fragment must fail closed");
        assert!(error.to_string().contains(&missing.display().to_string()));
        Ok(())
    }

    #[test]
    fn shared_revision_fallback_scans_at_most_once() -> Result<()> {
        let revision_files = vec![PathBuf::from("history.parquet")];
        let revision_ids = HashSet::from([101_i64, 202_i64]);
        let mut calls = 0;
        let mut loader = |files: &[PathBuf], ids: &HashSet<i64>| {
            calls += 1;
            assert_eq!(files, revision_files);
            assert_eq!(ids, &revision_ids);
            Ok(HashMap::from([(
                101,
                RevisionMeta {
                    timestamp_seconds: 1,
                    year_month_key: 202_401,
                    page_namespace: 0,
                    user_type: UserType::Registered,
                },
            )]))
        };
        let loaded =
            load_shared_revision_fallback("testwiki", &revision_files, &revision_ids, &mut loader)?;
        assert_eq!(loaded.len(), 1);

        let empty = load_shared_revision_fallback(
            "testwiki",
            &revision_files,
            &HashSet::new(),
            &mut loader,
        )
        .expect("an empty fallback should not invoke the loader");
        assert!(empty.is_empty());
        assert_eq!(calls, 1, "an empty fallback must not scan history");

        let mut local = HashMap::from([(101, loaded[&101])]);
        let fallback = HashMap::from([(
            202,
            RevisionMeta {
                timestamp_seconds: 2,
                year_month_key: 202_402,
                page_namespace: 1,
                user_type: UserType::Anonymous,
            },
        )]);
        extend_lookup_from_shared(&revision_ids, &fallback, &mut local);
        assert_eq!(local.len(), 2);
        assert_eq!(local[&101].timestamp_seconds, 1);
        assert_eq!(local[&202].timestamp_seconds, 2);
        Ok(())
    }

    fn write_rights(path: &Path, rows: &[(&str, &str, &str, &str)]) -> Result<()> {
        let mut frame = DataFrame::new_infer_height(vec![
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "target_user".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "old_groups".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "new_groups".into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
        ])
        .expect("rights fixture columns should be valid");
        ParquetWriter::new(File::create(path)?).finish(&mut frame)?;
        Ok(())
    }

    #[test]
    fn checkpoint_and_source_index_contracts_are_authenticated() -> Result<()> {
        let root = TestDir::new()?;
        let cache = cache(root.path(), "testwiki");
        let active = BTreeSet::from(["A".to_string(), "B".to_string()]);
        let checkpoint = RightsCheckpoint::new("2024-12", &"ab".repeat(32), "groups", &active);
        checkpoint.validate("2024-12", &"ab".repeat(32), "groups")?;
        let mut changed = checkpoint.clone();
        changed.active_users.reverse();
        assert!(
            changed
                .validate("2024-12", &"ab".repeat(32), "groups")
                .is_err()
        );
        let mut changed = checkpoint.clone();
        changed.state_sha256 = "00".repeat(32);
        assert!(
            changed
                .validate("2024-12", &"ab".repeat(32), "groups")
                .is_err()
        );
        assert!(
            checkpoint
                .validate("2023-12", &"ab".repeat(32), "groups")
                .is_err()
        );

        let mut index = SourceMonthIndex {
            schema_version: SOURCE_INDEX_SCHEMA_VERSION,
            wiki: "testwiki".to_string(),
            action_month: "2024-01".to_string(),
            patrol_artifact_sha256: "aa".repeat(32),
            revision_dependencies: BTreeMap::from([("2024-01".to_string(), "bb".repeat(32))]),
            events_input_digest: "cc".repeat(32),
            rows: 2,
            unresolved_revision_ids: 0,
            index_sha256: String::new(),
        };
        index.index_sha256 = index.canonical_hash();
        index.validate_identity("testwiki", "2024-01", &"aa".repeat(32))?;
        assert!(
            index.dependencies_match(&BTreeMap::from([("2024-01".to_string(), "bb".repeat(32))]))
        );
        assert!(!index.dependencies_match(&BTreeMap::new()));
        let mut unresolved = index.clone();
        unresolved.unresolved_revision_ids = 1;
        assert!(!unresolved.dependencies_match(&index.revision_dependencies));
        index.index_sha256 = "invalid".to_string();
        assert!(
            index
                .validate_identity("testwiki", "2024-01", &"aa".repeat(32))
                .is_err()
        );

        let missing_index = SourceMonthIndex {
            index_sha256: String::new(),
            ..index.clone()
        };
        assert!(load_source_events(&cache, &missing_index).is_err());
        let mut one_row = DataFrame::new_infer_height(vec![Column::new("value".into(), [1_i64])])
            .expect("cache fixture should be valid");
        cache
            .store(
                "patrol_source_events",
                SOURCE_INDEX_VERSION,
                &"cc".repeat(32),
                "events",
                &mut one_row,
            )
            .expect("cache fixture should be writable");
        assert!(load_source_events(&cache, &missing_index).is_err());
        Ok(())
    }

    #[test]
    fn enriched_user_types_and_rights_state_cover_failure_boundaries() -> Result<()> {
        let root = TestDir::new()?;
        let user_types = ["registered", "anonymous", "temporary", "bot"];
        let events = DataFrame::new_infer_height(vec![
            Column::new("current_revision_id".into(), [1_i64, 2, 3, 4]),
            Column::new("revision_month".into(), ["2024-01"; 4]),
            Column::new("revision_timestamp_seconds".into(), [1_i64, 2, 3, 4]),
            Column::new("page_namespace".into(), [0_i32, 0, 1, 1]),
            Column::new("revision_user_type".into(), user_types),
        ])
        .expect("source event fixture should be valid");
        let lookup = source_event_revision_lookup(&events)?;
        assert_eq!(lookup.len(), 4);
        assert_eq!(parse_user_type("registered")?, UserType::Registered);
        assert_eq!(parse_user_type("anonymous")?, UserType::Anonymous);
        assert_eq!(parse_user_type("temporary")?, UserType::Temporary);
        assert_eq!(parse_user_type("bot")?, UserType::Bot);
        assert!(parse_user_type("unknown").is_err());
        assert!(month_bounds("bad").is_err());
        assert!(month_bounds("2024-13").is_err());

        let rights = root.path().join("rights.parquet");
        write_rights(
            &rights,
            &[
                ("2024-01-05 00:00:00", "A", "autopatrolled", "editor"),
                ("2024-01-10 00:00:00", "A", "editor", "autopatrolled"),
                ("2024-01-20 00:00:00", "A", "autopatrolled", "editor"),
            ],
        )
        .expect("rights fixture should be writable");
        let groups = HashSet::from(["autopatrolled"]);
        let mut active = BTreeSet::new();
        let intervals = advance_rights_month(Some(&rights), "2024-01", &groups, &mut active)?;
        assert_eq!(intervals.get("A").map(Vec::len), Some(2));
        assert!(active.is_empty());

        let close_active = root.path().join("close-active.parquet");
        write_rights(
            &close_active,
            &[("2024-01-15 00:00:00", "B", "editor", "autopatrolled")],
        )
        .expect("active-close fixture should be writable");
        active.insert("B".to_string());
        let closed = advance_rights_month(Some(&close_active), "2024-01", &groups, &mut active)?;
        assert_eq!(closed.get("B").map(Vec::len), Some(2));
        assert!(active.contains("B"));

        let outside = root.path().join("outside.parquet");
        write_rights(
            &outside,
            &[("2024-02-01 00:00:00", "A", "editor", "autopatrolled")],
        )
        .expect("outside-month fixture should be writable");
        assert!(advance_rights_month(Some(&outside), "2024-01", &groups, &mut active).is_err());
        let unordered = root.path().join("unordered.parquet");
        write_rights(
            &unordered,
            &[
                ("2024-01-20 00:00:00", "A", "editor", "autopatrolled"),
                ("2024-01-10 00:00:00", "A", "autopatrolled", "editor"),
            ],
        )
        .expect("unordered fixture should be writable");
        assert!(advance_rights_month(Some(&unordered), "2024-01", &groups, &mut active).is_err());
        Ok(())
    }

    #[test]
    fn rights_checkpoint_search_and_state_advancement_are_fail_closed() -> Result<()> {
        let root = TestDir::new()?;
        let cache = cache(root.path(), "checkpointwiki");
        let timeline = BTreeSet::from([
            "2023-12".to_string(),
            "2024-12".to_string(),
            "2025-01".to_string(),
        ]);
        let prefixes = timeline
            .iter()
            .map(|month| (month.clone(), format!("{month}-prefix")))
            .collect::<BTreeMap<_, _>>();
        let mut active = BTreeSet::new();
        let mut state_month = None;
        restore_rights_checkpoint(
            "2025-01",
            &timeline,
            &prefixes,
            "groups",
            &cache,
            &mut active,
            &mut state_month,
        )
        .expect("empty checkpoint search should succeed");
        assert!(state_month.is_none());

        active.insert("A".to_string());
        write_or_validate_checkpoint("2024-12", &prefixes["2024-12"], "groups", &cache, &active)?;
        write_or_validate_checkpoint("2024-12", &prefixes["2024-12"], "groups", &cache, &active)?;
        let changed = BTreeSet::from(["B".to_string()]);
        assert!(
            write_or_validate_checkpoint(
                "2024-12",
                &prefixes["2024-12"],
                "groups",
                &cache,
                &changed,
            )
            .is_err()
        );

        let rights_artifacts = BTreeMap::new();
        let generation_root = root.path().join("generation");
        let groups = HashSet::new();
        let mut no_state = None;
        assert!(
            advance_state_through(
                "2025-02",
                &timeline,
                &rights_artifacts,
                &prefixes,
                &generation_root,
                &groups,
                "groups",
                &cache,
                &mut active,
                &mut no_state,
            )
            .is_err()
        );
        Ok(())
    }
}
