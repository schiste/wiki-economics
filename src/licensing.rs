use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub const ARTIFACT_LICENSE_SPDX: &str = "MIT";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LicensePolicy {
    pub spdx_identifier: String,
    pub name: String,
    pub url: String,
    pub applies_to: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceDataset {
    pub id: String,
    pub name: String,
    pub url: String,
    pub terms_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrademarkPolicy {
    pub owner: String,
    pub status: String,
    pub policy_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PrivacyPolicy {
    pub notice: String,
    pub policy_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolforgePolicy {
    pub open_source_license_spdx: String,
    pub open_data_license_spdx: String,
    pub compliance_statement: String,
    pub terms_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationLicensing {
    pub schema_version: u8,
    pub project_name: String,
    pub project_url: String,
    pub license: LicensePolicy,
    pub attribution: String,
    pub independence_notice: String,
    pub legal_url: String,
    pub source_datasets: Vec<SourceDataset>,
    pub trademark: TrademarkPolicy,
    pub privacy: PrivacyPolicy,
    pub toolforge: ToolforgePolicy,
}

pub fn publication_policy() -> Result<PublicationLicensing> {
    let policy: PublicationLicensing =
        serde_json::from_str(include_str!("../config/publication-licensing.json"))?;
    ensure!(policy.schema_version == 1, "unsupported licensing schema");
    ensure!(
        policy.license.spdx_identifier == ARTIFACT_LICENSE_SPDX,
        "publication license must be {ARTIFACT_LICENSE_SPDX}"
    );
    ensure!(
        !policy.source_datasets.is_empty(),
        "publication source provenance cannot be empty"
    );
    ensure!(
        !policy.attribution.trim().is_empty()
            && !policy.independence_notice.trim().is_empty()
            && !policy.trademark.status.trim().is_empty(),
        "publication legal notices cannot be empty"
    );
    ensure!(
        policy.toolforge.open_source_license_spdx == ARTIFACT_LICENSE_SPDX
            && policy.toolforge.open_data_license_spdx == ARTIFACT_LICENSE_SPDX,
        "Toolforge source and data licensing must be {ARTIFACT_LICENSE_SPDX}"
    );
    Ok(policy)
}

pub fn generating_commit() -> Option<String> {
    option_env!("WIKI_ECON_BUILD_COMMIT").map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_policy_is_complete_and_machine_readable() -> Result<()> {
        let policy = publication_policy()?;
        assert_eq!(policy.project_name, "Wiki Economics");
        assert_eq!(policy.license.spdx_identifier, ARTIFACT_LICENSE_SPDX);
        assert_eq!(policy.source_datasets.len(), 3);
        assert!(policy.trademark.status.contains("No trademark license"));
        assert_eq!(
            policy.toolforge.open_data_license_spdx,
            ARTIFACT_LICENSE_SPDX
        );
        assert_eq!(
            generating_commit(),
            option_env!("WIKI_ECON_BUILD_COMMIT").map(str::to_string)
        );
        Ok(())
    }
}
