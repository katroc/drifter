use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Local,
    S3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    LocalToS3,
    S3ToLocal,
    S3ToS3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    Overwrite,
    Skip,
    Prompt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanPolicy {
    UploadOnly,
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferPolicy {
    pub conflict_policy: ConflictPolicy,
    pub scan_policy: ScanPolicy,
}

impl Default for TransferPolicy {
    fn default() -> Self {
        Self {
            conflict_policy: ConflictPolicy::Overwrite,
            scan_policy: ScanPolicy::UploadOnly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointProfile {
    pub id: Option<i64>,
    pub name: String,
    pub kind: EndpointKind,
    #[serde(default = "default_endpoint_config")]
    pub config: Value,
    pub credential_ref: Option<String>,
    #[serde(default)]
    pub is_default_source: bool,
    #[serde(default)]
    pub is_default_destination: bool,
}

fn default_endpoint_config() -> Value {
    Value::Object(Default::default())
}
