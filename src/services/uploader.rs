use crate::core::config::{Config, S3KeyMode, SseMode};
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, ServerSideEncryption,
};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::coordinator::ProgressInfo;
use crate::db;
use crate::utils::{lock_async_mutex, lock_mutex};
use rusqlite::Connection;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, warn};

use base64::{Engine as _, engine::general_purpose};
use sha2::{Digest, Sha256};
pub struct TaskGuard(pub tokio::task::JoinHandle<()>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub name: String, // Relative display name
    pub size: i64,
    pub last_modified: String,
    pub is_dir: bool,
    pub is_parent: bool,
}

#[derive(Debug, Clone)]
pub struct S3EndpointConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Clone)]
pub struct Uploader {}

impl Uploader {
    pub fn new(_config: &Config) -> Self {
        Self {}
    }

    async fn create_client(config: &Config) -> Result<(Client, String)> {
        let endpoint = S3EndpointConfig {
            bucket: config.s3_bucket.clone().unwrap_or_default(),
            prefix: config.s3_prefix.clone(),
            region: config.s3_region.clone(),
            endpoint: config.s3_endpoint.clone(),
            access_key: config.s3_access_key.clone(),
            secret_key: config.s3_secret_key.clone(),
        };
        Self::create_client_for_endpoint(&endpoint).await
    }

    async fn create_client_for_endpoint(
        endpoint_cfg: &S3EndpointConfig,
    ) -> Result<(Client, String)> {
        let bucket = endpoint_cfg.bucket.trim();
        let region = endpoint_cfg.region.as_deref().unwrap_or("us-east-1").trim();
        let endpoint = endpoint_cfg.endpoint.as_deref().map(str::trim);
        let access_key = endpoint_cfg.access_key.as_deref().map(str::trim);
        let secret_key = endpoint_cfg.secret_key.as_deref().map(str::trim);

        #[allow(deprecated)]
        let mut config_loader =
            aws_config::from_env().region(aws_config::Region::new(region.to_string()));

        if bucket.is_empty() {
            return Err(anyhow::anyhow!("S3 bucket not configured"));
        }

        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
            let creds = Credentials::new(ak.to_string(), sk.to_string(), None, None, "static");
            config_loader =
                config_loader.credentials_provider(SharedCredentialsProvider::new(creds));
        } else if access_key.is_some() || secret_key.is_some() {
            return Err(anyhow::anyhow!(
                "S3 Credentials incomplete: Both Access Key and Secret Key must be provided."
            ));
        }

        let sdk_config = config_loader.load().await;

        let mut client_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint) = endpoint {
            client_builder = client_builder.endpoint_url(endpoint).force_path_style(true);
        }
        let client = Client::from_conf(client_builder.build());
        Ok((client, bucket.to_string()))
    }

    fn normalized_prefix(prefix: Option<&str>) -> Option<String> {
        let raw = prefix?.trim();
        if raw.is_empty() {
            return None;
        }
        let trimmed = raw.trim_matches('/');
        if trimmed.is_empty() {
            None
        } else {
            Some(format!("{trimmed}/"))
        }
    }

    pub fn resolve_object_key(endpoint_cfg: &S3EndpointConfig, object_key: &str) -> String {
        let key = object_key.trim_start_matches('/');
        if key.is_empty() {
            return String::new();
        }

        let Some(prefix) = Self::normalized_prefix(endpoint_cfg.prefix.as_deref()) else {
            return key.to_string();
        };

        if key == prefix.trim_end_matches('/') || key.starts_with(&prefix) {
            key.to_string()
        } else {
            format!("{prefix}{key}")
        }
    }

    fn parent_folder_marker_key(key: &str) -> Option<String> {
        let trimmed = key.trim_end_matches('/');
        let (parent, _) = trimmed.rsplit_once('/')?;
        if parent.is_empty() {
            return None;
        }
        Some(format!("{}/", parent.trim_end_matches('/')))
    }

    fn normalize_endpoint_url(endpoint: Option<&str>) -> Option<String> {
        let raw = endpoint?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(trimmed.trim_end_matches('/').to_ascii_lowercase())
    }

    pub(crate) fn supports_server_side_copy(
        source_endpoint: &S3EndpointConfig,
        destination_endpoint: &S3EndpointConfig,
    ) -> bool {
        Self::normalize_endpoint_url(source_endpoint.endpoint.as_deref())
            == Self::normalize_endpoint_url(destination_endpoint.endpoint.as_deref())
    }

    fn encode_copy_source_key(key: &str) -> String {
        let mut encoded = String::with_capacity(key.len());
        for &b in key.as_bytes() {
            let keep =
                b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/' | b'!');
            if keep {
                encoded.push(char::from(b));
            } else {
                encoded.push('%');
                encoded.push_str(&format!("{:02X}", b));
            }
        }
        encoded
    }

    fn format_copy_source(bucket: &str, key: &str) -> String {
        format!("{bucket}/{}", Self::encode_copy_source_key(key))
    }

    fn apply_sse_create_multipart(
        mut req: aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
        config: &Config,
    ) -> aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder
    {
        match config.sse {
            SseMode::Off => req,
            SseMode::S3 => req.server_side_encryption(ServerSideEncryption::Aes256),
            SseMode::Kms => {
                req = req.server_side_encryption(ServerSideEncryption::AwsKms);
                if let Some(kms_key_id) = config.kms_key_id.as_deref().filter(|s| !s.is_empty()) {
                    req = req.ssekms_key_id(kms_key_id);
                }
                req
            }
        }
    }

    fn apply_sse_put_object(
        mut req: aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder,
        config: &Config,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        match config.sse {
            SseMode::Off => req,
            SseMode::S3 => req.server_side_encryption(ServerSideEncryption::Aes256),
            SseMode::Kms => {
                req = req.server_side_encryption(ServerSideEncryption::AwsKms);
                if let Some(kms_key_id) = config.kms_key_id.as_deref().filter(|s| !s.is_empty()) {
                    req = req.ssekms_key_id(kms_key_id);
                }
                req
            }
        }
    }

    fn derive_key_from_mode(config: &Config, source_path: &str, fallback_path: &Path) -> String {
        let fallback_name = fallback_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "file".to_string());
        let source = Path::new(source_path);

        if matches!(config.s3_key_mode, S3KeyMode::Template)
            && let Some(template) = config.s3_key_template.as_deref().filter(|s| !s.is_empty())
        {
            let filename = source
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| fallback_name.clone());
            let stem = source
                .file_stem()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| filename.clone());
            let ext = source
                .extension()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let date = chrono::Utc::now().format("%Y-%m-%d").to_string();

            let rendered = template
                .replace("{filename}", &filename)
                .replace("{basename}", &stem)
                .replace("{ext}", &ext)
                .replace("{date}", &date)
                .replace("{source}", source_path);

            let normalized = rendered.trim_matches('/').to_string();
            if !normalized.is_empty() {
                return normalized;
            }
        }

        source
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or(fallback_name)
    }

    pub async fn list_bucket_contents(
        config: &Config,
        subdir: Option<&str>,
    ) -> Result<Vec<S3Object>> {
        let (client, bucket) = Self::create_client(config).await?;

        // Base prefix from config (e.g. "uploads/")
        let base_prefix = config.s3_prefix.as_deref().unwrap_or("");

        // Final prefix = base_prefix + subdir
        // subdir might be "folder1/"
        let prefix = if let Some(sub) = subdir {
            format!("{}{}", base_prefix, sub)
        } else {
            base_prefix.to_string()
        };

        let mut objects = Vec::new();
        let mut discovered_dirs = HashSet::new();
        let mut continuation_token = None;

        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).delimiter("/"); // Enable directory mode

            if !prefix.is_empty() {
                req = req.prefix(&prefix);
            }
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("Failed to list objects")?;

            let is_truncated = resp.is_truncated().unwrap_or(false);
            let next_token = resp.next_continuation_token.clone();

            // 1. Handle Folders (CommonPrefixes)
            if let Some(common_prefixes) = resp.common_prefixes {
                for cp in common_prefixes {
                    if let Some(key) = cp.prefix {
                        discovered_dirs.insert(key.clone());

                        // Calculate relative name
                        let name = if key.starts_with(&prefix) {
                            key[prefix.len()..].to_string()
                        } else {
                            key.clone()
                        };

                        objects.push(S3Object {
                            key,
                            name,
                            size: 0,
                            last_modified: String::new(),
                            is_dir: true,
                            is_parent: false,
                        });
                    }
                }
            }

            // 2. Handle Files (Contents)
            if let Some(contents) = resp.contents {
                for obj in contents {
                    let key = obj.key().unwrap_or("").to_string();

                    // S3 sometimes returns the prefix itself as an object if it was created explicitly (0 byte file)
                    // If key == prefix, and we are browsing that prefix, it's the current dir, skip it.
                    if key == prefix {
                        continue;
                    }

                    // Treat trailing-slash markers as directories and deduplicate against CommonPrefixes.
                    if key.ends_with('/') {
                        if discovered_dirs.contains(&key) {
                            continue;
                        }

                        discovered_dirs.insert(key.clone());
                        let name = if key.starts_with(&prefix) {
                            key[prefix.len()..].to_string()
                        } else {
                            key.clone()
                        };

                        objects.push(S3Object {
                            key,
                            name,
                            size: 0,
                            last_modified: String::new(),
                            is_dir: true,
                            is_parent: false,
                        });
                        continue;
                    }

                    let size = obj.size().unwrap_or(0);
                    let last_modified = obj
                        .last_modified()
                        .map(|d| d.to_string())
                        .unwrap_or_default();

                    let name = if key.starts_with(&prefix) {
                        key[prefix.len()..].to_string()
                    } else {
                        key.clone()
                    };

                    objects.push(S3Object {
                        key,
                        name,
                        size,
                        last_modified,
                        is_dir: false,
                        is_parent: false,
                    });
                }
            }

            if is_truncated {
                continuation_token = next_token.map(|s| s.to_string());
            } else {
                break;
            }
        }

        // Sort: Folders first, then Files
        objects.sort_by(|a, b| {
            if a.is_dir && !b.is_dir {
                std::cmp::Ordering::Less
            } else if !a.is_dir && b.is_dir {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });

        Ok(objects)
    }

    /// Recursively list files under a folder key (prefix). Returns files only.
    pub async fn list_files_recursive(config: &Config, folder_key: &str) -> Result<Vec<S3Object>> {
        let (client, bucket) = Self::create_client(config).await?;

        let mut prefix = folder_key.to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut files = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("Failed to list folder contents")?;

            let is_truncated = resp.is_truncated().unwrap_or(false);
            let next_token = resp.next_continuation_token.clone();

            for obj in resp.contents.unwrap_or_default() {
                let key = obj.key().unwrap_or("").to_string();
                if key.is_empty() || key.ends_with('/') {
                    continue;
                }

                let size = obj.size().unwrap_or(0);
                let last_modified = obj
                    .last_modified()
                    .map(|d| d.to_string())
                    .unwrap_or_default();

                files.push(S3Object {
                    name: key.clone(),
                    key,
                    size,
                    last_modified,
                    is_dir: false,
                    is_parent: false,
                });
            }

            if is_truncated {
                continuation_token = next_token.map(|s| s.to_string());
            } else {
                break;
            }
        }

        files.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(files)
    }

    pub async fn object_exists(endpoint_cfg: &S3EndpointConfig, object_key: &str) -> Result<bool> {
        let (client, bucket) = Self::create_client_for_endpoint(endpoint_cfg).await?;
        let resolved_key = Self::resolve_object_key(endpoint_cfg, object_key);

        match client
            .head_object()
            .bucket(bucket)
            .key(resolved_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if let Some(service_err) = e.as_service_error()
                    && matches!(
                        service_err.code(),
                        Some("NotFound") | Some("NoSuchKey") | Some("404") | Some("NoSuchObject")
                    )
                {
                    return Ok(false);
                }
                Err(e).context("Failed to check destination object")
            }
        }
    }

    pub async fn copy_between_endpoints(
        source_endpoint: &S3EndpointConfig,
        destination_endpoint: &S3EndpointConfig,
        source_key: &str,
        destination_key: &str,
        part_size_mb: u64,
    ) -> Result<()> {
        let (source_client, source_bucket) =
            Self::create_client_for_endpoint(source_endpoint).await?;
        let (destination_client, destination_bucket) =
            Self::create_client_for_endpoint(destination_endpoint).await?;

        let resolved_source_key = Self::resolve_object_key(source_endpoint, source_key);
        let resolved_destination_key =
            Self::resolve_object_key(destination_endpoint, destination_key);

        if resolved_source_key.is_empty() {
            anyhow::bail!("source object key is empty");
        }
        if resolved_destination_key.is_empty() {
            anyhow::bail!("destination object key is empty");
        }

        if !Self::supports_server_side_copy(source_endpoint, destination_endpoint) {
            anyhow::bail!(
                "s3_to_s3 server-side copy requires matching S3 endpoint URLs; source endpoint '{}' and destination endpoint '{}' differ",
                source_endpoint.endpoint.as_deref().unwrap_or("aws-default"),
                destination_endpoint
                    .endpoint
                    .as_deref()
                    .unwrap_or("aws-default")
            );
        }

        let copy_source = Self::format_copy_source(&source_bucket, &resolved_source_key);

        let head = source_client
            .head_object()
            .bucket(&source_bucket)
            .key(&resolved_source_key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to read source object metadata s3://{}/{}",
                    source_bucket, resolved_source_key
                )
            })?;
        let source_size = head.content_length().unwrap_or(0).max(0) as u64;

        // PutObject can only store objects up to 5GiB.
        if source_size <= 5 * 1024 * 1024 * 1024 {
            destination_client
                .copy_object()
                .bucket(&destination_bucket)
                .key(&resolved_destination_key)
                .copy_source(&copy_source)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Failed to copy source object s3://{}/{} to s3://{}/{}",
                        source_bucket,
                        resolved_source_key,
                        destination_bucket,
                        resolved_destination_key
                    )
                })?;
            return Ok(());
        }

        let min_part_size = 5 * 1024 * 1024u64;
        let desired_part_size = part_size_mb.saturating_mul(1024 * 1024).max(min_part_size);
        let required_min_part_size = source_size.div_ceil(10_000);
        let part_size = desired_part_size.max(required_min_part_size);

        let create_multipart = destination_client
            .create_multipart_upload()
            .bucket(&destination_bucket)
            .key(&resolved_destination_key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to start multipart copy to s3://{}/{}",
                    destination_bucket, resolved_destination_key
                )
            })?;

        let upload_id = create_multipart
            .upload_id()
            .map(std::string::ToString::to_string)
            .context("multipart upload did not return upload_id")?;

        let copy_result = async {
            let mut completed_parts = Vec::new();
            let mut part_number: i32 = 1;
            let mut offset: u64 = 0;

            while offset < source_size {
                let end = std::cmp::min(offset + part_size - 1, source_size - 1);
                let range_header = format!("bytes={offset}-{end}");
                let part_resp = destination_client
                    .upload_part_copy()
                    .bucket(&destination_bucket)
                    .key(&resolved_destination_key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .copy_source(&copy_source)
                    .copy_source_range(range_header)
                    .send()
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to copy multipart part {} to s3://{}/{}",
                            part_number, destination_bucket, resolved_destination_key
                        )
                    })?;

                let etag = part_resp
                    .copy_part_result()
                    .and_then(|result| result.e_tag())
                    .map(std::string::ToString::to_string)
                    .context("multipart copy part missing ETag")?;
                completed_parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(etag)
                        .build(),
                );

                part_number += 1;
                offset = end + 1;
            }

            let completed_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            destination_client
                .complete_multipart_upload()
                .bucket(&destination_bucket)
                .key(&resolved_destination_key)
                .upload_id(&upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Failed to complete multipart copy to s3://{}/{}",
                        destination_bucket, resolved_destination_key
                    )
                })?;
            Result::<()>::Ok(())
        }
        .await;

        if let Err(e) = copy_result {
            let _ = destination_client
                .abort_multipart_upload()
                .bucket(&destination_bucket)
                .key(&resolved_destination_key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        Ok(())
    }

    pub async fn download_file(config: &Config, key: &str, dest: &Path) -> Result<()> {
        let (client, bucket) = Self::create_client(config).await?;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .context("Failed to get object")?;

        let mut body = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(dest)
            .await
            .context("Failed to create local file")?;

        tokio::io::copy(&mut body, &mut file)
            .await
            .context("Failed to write to file")?;
        Ok(())
    }

    pub async fn delete_file(config: &Config, key: &str) -> Result<()> {
        let (client, bucket) = Self::create_client(config).await?;

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .context("Failed to delete object")?;

        // S3 folders are virtual prefixes; preserve the parent folder marker so
        // deleting the last file in a folder doesn't make the folder disappear in the explorer.
        if let Some(marker_key) = Self::parent_folder_marker_key(key) {
            let put_req = client
                .put_object()
                .bucket(&bucket)
                .key(&marker_key)
                .content_length(0);
            let put_req = Self::apply_sse_put_object(put_req, config);
            if let Err(e) = put_req.send().await {
                warn!(
                    "Deleted {}, but failed to preserve folder marker {}: {}",
                    key, marker_key, e
                );
            }
        }
        Ok(())
    }

    /// Check whether a "folder" (prefix) is empty, allowing a trailing-slash marker object.
    pub async fn is_folder_empty(config: &Config, folder_key: &str) -> Result<bool> {
        let (client, bucket) = Self::create_client(config).await?;

        let mut prefix = folder_key.to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .delimiter("/")
            .max_keys(2)
            .send()
            .await
            .context("Failed to list folder contents")?;

        let has_children = resp
            .common_prefixes
            .as_ref()
            .map(|c| !c.is_empty())
            .unwrap_or(false)
            || resp.contents.as_ref().is_some_and(|contents| {
                contents
                    .iter()
                    .any(|obj| obj.key().map(|k| k != prefix).unwrap_or(false))
            });

        Ok(!has_children)
    }

    /// Recursively delete a folder (prefix) and all its contents.
    pub async fn delete_folder_recursive(config: &Config, folder_key: &str) -> Result<u64> {
        let (client, bucket) = Self::create_client(config).await?;

        let mut prefix = folder_key.to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut continuation_token = None;
        let mut deleted: u64 = 0;

        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("Failed to list folder contents")?;

            let mut identifiers: Vec<ObjectIdentifier> = Vec::new();
            let is_truncated = resp.is_truncated().unwrap_or(false);
            let next_token = resp.next_continuation_token.clone();

            let objects = resp.contents.unwrap_or_default();
            for obj in objects {
                if let Some(key) = obj.key {
                    identifiers.push(ObjectIdentifier::builder().key(key).build()?);
                }
            }

            if identifiers.is_empty() {
                // Ensure any trailing-slash marker is removed.
                let _ = client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&prefix)
                    .send()
                    .await;
            } else {
                let count = identifiers.len() as u64;
                let delete = Delete::builder()
                    .set_objects(Some(identifiers))
                    .quiet(true)
                    .build()?;
                client
                    .delete_objects()
                    .bucket(&bucket)
                    .delete(delete)
                    .send()
                    .await
                    .context("Failed to delete folder objects")?;
                deleted += count;
            }

            if is_truncated {
                continuation_token = next_token.map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(deleted)
    }

    /// Create a folder in S3 by uploading a 0-byte object with a trailing slash
    pub async fn create_folder(config: &Config, folder_key: &str) -> Result<()> {
        let (client, bucket) = Self::create_client(config).await?;

        // Ensure the key ends with a slash
        let key = if folder_key.ends_with('/') {
            folder_key.to_string()
        } else {
            format!("{}/", folder_key)
        };

        let put_req = client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .content_length(0);
        let put_req = Self::apply_sse_put_object(put_req, config);
        put_req.send().await.context("Failed to create folder")?;

        info!("Created S3 folder: {}", key);
        Ok(())
    }

    pub async fn check_connection(config: &Config) -> Result<String> {
        // Use helper, but construct customized error messages?
        // create_client checks basic config/creds.
        let (client, bucket) = match Self::create_client(config).await {
            Ok(c) => c,
            Err(e) => return Err(e),
        };

        match client.list_buckets().send().await {
            Ok(_) => Ok(format!("Connected to S3 successfully (Bucket: {})", bucket)),
            Err(e) => {
                // Try HeadBucket
                match client.head_bucket().bucket(&bucket).send().await {
                    Ok(_) => Ok(format!(
                        "Connected to S3 successfully (Access to '{}' confirmed)",
                        bucket
                    )),
                    Err(_) => Err(anyhow::anyhow!("S3 Connection Failed: {}", e)),
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upload_file(
        &self,
        config: &Config,
        path: &str,
        job_id: i64,
        progress: Arc<AsyncMutex<HashMap<i64, ProgressInfo>>>,
        conn_mutex: Arc<Mutex<Connection>>,
        initial_upload_id: Option<String>,
        cancellation_token: Arc<AtomicBool>,
    ) -> Result<bool> {
        let (client, bucket) = Self::create_client(config).await?;
        let prefix = config.s3_prefix.as_deref().unwrap_or_default();
        let concurrency = config.concurrency_upload_parts.max(1);
        // create_client already checks bucket

        let file_path = Path::new(path);

        // Get the original source path or pre-calculated key from the job
        let s3_key_path = {
            let conn = lock_mutex(&conn_mutex)?;
            if let Ok(Some(job)) = db::get_job(&conn, job_id) {
                // If ingest calculated a relative path, use it
                if let Some(key) = job.s3_key {
                    key
                } else {
                    Self::derive_key_from_mode(config, &job.source_path, file_path)
                }
            } else {
                Self::derive_key_from_mode(config, path, file_path)
            }
        };
        let key = format!("{}{}", prefix, s3_key_path);

        let mut upload_id = initial_upload_id.clone();
        let mut completed_parts = Vec::new();
        let mut completed_indices = HashSet::new();
        // Store raw hashes for local composite calculation: Map<PartNumber, Vec<u8>>
        let part_hashes = Arc::new(Mutex::new(HashMap::<i32, Vec<u8>>::new()));

        // 1. Try to resume if upload_id exists
        if let Some(uid) = &upload_id {
            // Notify UI
            info!("Resuming upload for job {} (Upload ID: {})", job_id, uid);
            {
                let mut p = lock_async_mutex(&progress).await;
                p.insert(
                    job_id,
                    ProgressInfo {
                        percent: -1.0,
                        details: "Resuming: Listing parts...".to_string(),
                        parts_done: 0,
                        parts_total: 0,
                        bytes_done: 0,
                        bytes_total: 0,
                        elapsed_secs: 0,
                    },
                );
            }

            // List existing parts
            let list_parts_output = client
                .list_parts()
                .bucket(&bucket)
                .key(&key)
                .upload_id(uid)
                .send()
                .await;

            match list_parts_output {
                Ok(output) => {
                    if let Some(parts) = output.parts {
                        for p in parts {
                            if let Some(pn) = p.part_number
                                && let Some(etag) = p.e_tag
                            {
                                let mut builder =
                                    CompletedPart::builder().part_number(pn).e_tag(etag);

                                // If we have a checksum from S3, save it locally for the composite calc
                                if let Some(cs) = p.checksum_sha256 {
                                    if let Ok(bytes) = general_purpose::STANDARD.decode(&cs)
                                        && let Ok(mut map) = part_hashes.lock()
                                    {
                                        map.insert(pn, bytes);
                                    }
                                    builder = builder.checksum_sha256(cs);
                                }

                                completed_parts.push(builder.build());
                                completed_indices.insert(pn);
                            }
                        }
                    }
                }
                Err(_) => {
                    // Invalid upload ID (expired or aborted), start fresh
                    upload_id = None;
                }
            }
        }

        // 2. Create new upload if needed
        if upload_id.is_none() {
            let create_req = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256);
            let create_req = Self::apply_sse_create_multipart(create_req, config);
            let create_output = create_req.send().await.map_err(|e| {
                let service_err = match e.as_service_error() {
                    Some(s) => format!(
                        "{}: {}",
                        s.code().unwrap_or("Unknown"),
                        s.message().unwrap_or("No message")
                    ),
                    None => e.to_string(),
                };
                error!(
                    "Failed to create multipart upload: {} (Bucket: {}, Key: {})",
                    service_err, bucket, key
                );
                anyhow::anyhow!(
                    "Failed to create multipart upload: {} (Bucket: {}, Key: {})",
                    service_err,
                    bucket,
                    key
                )
            })?;

            let new_uid = create_output
                .upload_id()
                .context("No upload ID")?
                .to_string();
            info!(
                "Created new multipart upload for job {}: {}",
                job_id, new_uid
            );

            // Save to DB
            {
                let conn = lock_mutex(&conn_mutex)?;
                db::update_job_upload_id(&conn, job_id, &new_uid)?;
            }
            upload_id = Some(new_uid);
        }

        let upload_id =
            upload_id.ok_or_else(|| anyhow::anyhow!("Failed to obtain or create Upload ID"))?;

        let mut file = File::open(path).await.context("Failed to open file")?;
        let file_size = file.metadata().await?.len();

        // Dynamic Part Size Calculation
        let min_part_size = 5 * 1024 * 1024; // S3 Min: 5MB
        let max_parts = 10000; // S3 Max parts

        // 1. Base config size (default 128MB)
        let config_part_size = (config.part_size_mb * 1024 * 1024) as usize;

        // 2. Adjust for parallelism: Attempt to create enough parts for all threads
        // If file_size=100MB, concurrency=4 -> target part size ~25MB
        let parallel_part_size = (file_size as usize)
            .checked_div(concurrency)
            .unwrap_or(file_size as usize);

        // 3. Select optimal size:
        // Start with the SMALLER of config vs parallel (to encourage parallelism)
        // But clamp to min_part_size (5MB).
        let mut part_size = config_part_size.min(parallel_part_size).max(min_part_size);

        // 4. Critical: Adjust for S3 10,000 part limit (override everything else)
        // If file is 5TB, part_size MUST be > 500MB regardless of config
        let required_min_part = file_size.div_ceil(max_parts);
        part_size = part_size.max(required_min_part as usize);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let uploaded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Initialize uploaded bytes with already completed parts size?
        // We lack part sizes in list_parts usually (it returns Part struct).
        // We can estimate or just track new uploads. For progress bar correctness on resume,
        let initial_bytes: u64 = completed_indices.len() as u64 * part_size as u64;
        uploaded_bytes.store(initial_bytes, std::sync::atomic::Ordering::Relaxed);

        // Monitor State
        let net_bytes = uploaded_bytes.clone();
        let current_part = Arc::new(std::sync::atomic::AtomicU64::new(1));

        let monitor_progress = progress.clone();
        let monitor_net = net_bytes.clone();
        let m_job_id = job_id;
        let m_file_size = file_size;
        let m_part_size = part_size;
        let m_total_parts = (file_size as usize).div_ceil(part_size);

        let monitor_handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

                let n = monitor_net.load(std::sync::atomic::Ordering::Relaxed);

                let pct = (n as f64 / m_file_size as f64) * 100.0;

                let part_size_display = if m_part_size >= 1024 * 1024 * 1024 {
                    format!("{:.2}GB", m_part_size as f64 / 1024.0 / 1024.0 / 1024.0)
                } else {
                    format!("{:.2}MB", m_part_size as f64 / 1024.0 / 1024.0)
                };

                // Calculate elapsed time
                let elapsed = start_time.elapsed();
                let elapsed_str = if elapsed.as_secs() >= 60 {
                    format!("{}m{}s", elapsed.as_secs() / 60, elapsed.as_secs() % 60)
                } else {
                    format!("{}s", elapsed.as_secs())
                };

                // Calculate parts completed
                let parts_done = n / m_part_size as u64;

                let details = format!(
                    "{}/{} parts, {} sized [{}]",
                    parts_done, m_total_parts, part_size_display, elapsed_str
                );

                {
                    let mut p = lock_async_mutex(&monitor_progress).await;
                    p.insert(
                        m_job_id,
                        ProgressInfo {
                            percent: pct,
                            details,
                            parts_done: parts_done as usize,
                            parts_total: m_total_parts,
                            bytes_done: n,
                            bytes_total: m_file_size,
                            elapsed_secs: elapsed.as_secs(),
                        },
                    );
                }

                if n >= m_file_size {
                    // Don't break, keep updating (likely 0 speeds) until aborted by main thread.
                    // This ensures we don't freeze on stale 'high speed' metrics if finalization takes time.
                }
            }
        });
        let _monitor_guard = TaskGuard(monitor_handle);

        let mut handles: Vec<tokio::task::JoinHandle<Result<CompletedPart>>> = Vec::new();
        let mut part_number = 1;

        loop {
            // Check cancellation
            if cancellation_token.load(Ordering::Relaxed) {
                // Guards will abort monitor and pending handles (if we wrapped handles? No, handles are just handles)
                for h in handles {
                    h.abort();
                }
                return Ok(false);
            }

            // Check if part is already done
            if completed_indices.contains(&part_number) {
                use std::io::SeekFrom;
                let current_pos = (part_number as u64 - 1) * part_size as u64;
                file.seek(SeekFrom::Start(current_pos + part_size as u64))
                    .await
                    .unwrap_or(current_pos);

                part_number += 1;
                // Update tracker
                current_part.store(part_number as u64, std::sync::atomic::Ordering::Relaxed);
                continue;
            }

            // Acquire permit BEFORE reading to throttle memory usage
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .context("Semaphore closed")?;

            // Allocate buffer strictly for this part (Zero-Copy logic: we read, then move the vec)
            let mut chunk = vec![0u8; part_size];

            use std::io::SeekFrom;
            let offset = (part_number as u64 - 1) * (part_size as u64);
            file.seek(SeekFrom::Start(offset)).await?;

            let mut bytes_read = 0;
            while bytes_read < part_size {
                let n = file
                    .read(&mut chunk[bytes_read..])
                    .await
                    .context("Failed to read file chunk")?;
                if n == 0 {
                    break;
                }
                bytes_read += n;
            }

            if bytes_read == 0 {
                drop(permit);
                break;
            }

            // Shrink vector to actual size (cheap pointer adjustment)
            chunk.truncate(bytes_read);
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let uid_clone = upload_id.clone();
            let uploaded_bytes = uploaded_bytes.clone(); // Still needed for fetch_add below
            let part_hashes_clone = part_hashes.clone();

            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completion

                // Calculate SHA256 Checksum for this part
                let mut hasher = Sha256::new();
                hasher.update(&chunk);
                let hash = hasher.finalize();
                let hash_bytes = hash.to_vec();
                let checksum_sha256 = general_purpose::STANDARD.encode(&hash_bytes);

                let body = aws_sdk_s3::primitives::ByteStream::from(chunk);

                let upload_part_output = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(uid_clone)
                    .part_number(part_number)
                    .body(body)
                    .checksum_sha256(checksum_sha256)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to upload part {}: {}", part_number, e))?;

                // Store hash for final composite calculation
                if let Ok(mut map) = part_hashes_clone.lock() {
                    map.insert(part_number, hash_bytes);
                }

                // Update Net Tracker manually (coarse progress)
                uploaded_bytes.fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);

                if let Some(etag) = upload_part_output.e_tag() {
                    Ok(CompletedPart::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .checksum_sha256(upload_part_output.checksum_sha256().unwrap_or_default())
                        .build())
                } else {
                    Err(anyhow::anyhow!("No ETag"))
                }
            });

            handles.push(handle);
            part_number += 1;
            current_part.store(part_number as u64, std::sync::atomic::Ordering::Relaxed);
        }

        // Wait for all parts
        // `completed_parts` currently holds PRE-EXISTING parts.
        // We must append new ones.

        for handle in handles {
            let part = handle.await??;
            completed_parts.push(part);
        }

        // Monitor aborted AFTER complete to keep showing stats (likely 0)
        // Update details to "Finalizing"
        {
            let mut p = lock_async_mutex(&progress).await;
            if let Some(mut info) = p.get(&job_id).cloned() {
                info.details = "Finalizing S3...".to_string();
                p.insert(job_id, info);
            }
        }

        // Sort parts by number (important for S3)
        completed_parts.sort_by_key(|a| a.part_number());

        // Complete Multipart Upload
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts.clone()))
            .build();

        let complete_output = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed complete: {}", e))?;

        // monitor_handle aborted by TaskGuard drop

        // Store Checksums in DB
        {
            let conn = lock_mutex(&conn_mutex)?;

            // S3 for multipart with SHA256 usually returns a composite checksum
            // e.g., "checksum-parts_count".
            let remote_checksum = complete_output.checksum_sha256();

            // Calculate Local Composite Checksum (Tree Hash logic for S3)
            // SHA256(HashPart1 + HashPart2 + ...)
            let local_checksum = if let Ok(map) = part_hashes.lock() {
                let mut parts: Vec<_> = map.iter().collect();
                parts.sort_by_key(|(k, _)| **k); // Sort by part number

                let mut hasher = Sha256::new();
                for (_, bytes) in parts {
                    hasher.update(bytes);
                }
                let composite_hash = hasher.finalize();
                let b64 = general_purpose::STANDARD.encode(composite_hash);
                // Append -PartCount
                let count = map.len();
                if count > 0 {
                    Some(format!("{}-{}", b64, count))
                } else {
                    None
                }
            } else {
                None
            };

            let _ =
                db::update_job_checksums(&conn, job_id, local_checksum.as_deref(), remote_checksum);
        }

        // Cleanup DB
        {
            let mut p = lock_async_mutex(&progress).await;
            p.remove(&job_id);
            // Optionally clear s3_upload_id from DB now that it's done?
            let conn = lock_mutex(&conn_mutex)?;
            // We can set it to NULL or keep it as record.
            // Setting to NULL is good to indicate "done/no active upload".
            let _ = conn.execute(
                "UPDATE jobs SET s3_upload_id = NULL WHERE id = ?",
                rusqlite::params![job_id],
            );
        }

        // monitor_handle aborted by TaskGuard

        Ok(true)
    }

    // Helper functions exposed for testing

    /// Calculate the optimal part size for S3 multipart upload
    /// Takes into account:
    /// - Configured part size (default 128MB)
    /// - Parallelism (smaller parts for better concurrency)
    /// - S3 minimum part size (5MB)
    /// - S3 maximum parts limit (10,000)
    #[allow(dead_code)]
    pub fn calculate_part_size(
        file_size: u64,
        config_part_size_mb: u64,
        concurrency: usize,
    ) -> usize {
        let min_part_size = 5 * 1024 * 1024; // S3 Min: 5MB
        let max_parts = 10000u64; // S3 Max parts

        let config_part_size = (config_part_size_mb * 1024 * 1024) as usize;
        let parallel_part_size = (file_size as usize)
            .checked_div(concurrency)
            .unwrap_or(file_size as usize);

        let mut part_size = config_part_size.min(parallel_part_size).max(min_part_size);

        // Critical: Adjust for S3 10,000 part limit
        let required_min_part = file_size.div_ceil(max_parts);
        part_size = part_size.max(required_min_part as usize);

        part_size
    }

    /// Calculate SHA256 checksum for a byte slice
    #[allow(dead_code)]
    pub fn calculate_checksum(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        general_purpose::STANDARD.encode(hash)
    }

    /// Calculate composite checksum from multiple part hashes
    /// This matches S3's multipart checksum format: base64(SHA256(hash1+hash2+...))-partcount
    #[allow(dead_code)]
    pub fn calculate_composite_checksum(part_hashes: Vec<Vec<u8>>) -> String {
        let count = part_hashes.len();
        let mut hasher = Sha256::new();
        for hash_bytes in &part_hashes {
            hasher.update(hash_bytes);
        }
        let composite_hash = hasher.finalize();
        let b64 = general_purpose::STANDARD.encode(composite_hash);
        format!("{}-{}", b64, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_object_key_without_prefix() {
        let endpoint = S3EndpointConfig {
            bucket: "bucket".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint: None,
            access_key: None,
            secret_key: None,
        };
        assert_eq!(
            Uploader::resolve_object_key(&endpoint, "a/b.txt"),
            "a/b.txt"
        );
    }

    #[test]
    fn test_resolve_object_key_with_prefix() {
        let endpoint = S3EndpointConfig {
            bucket: "bucket".to_string(),
            prefix: Some("incoming".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: None,
            access_key: None,
            secret_key: None,
        };
        assert_eq!(
            Uploader::resolve_object_key(&endpoint, "a/b.txt"),
            "incoming/a/b.txt"
        );
        assert_eq!(
            Uploader::resolve_object_key(&endpoint, "incoming/a/b.txt"),
            "incoming/a/b.txt"
        );
    }

    #[test]
    fn test_parent_folder_marker_key_nested_path() {
        let marker = Uploader::parent_folder_marker_key("docker/docker-compose.yml");
        assert_eq!(marker.as_deref(), Some("docker/"));
    }

    #[test]
    fn test_parent_folder_marker_key_deep_path() {
        let marker = Uploader::parent_folder_marker_key("a/b/c.txt");
        assert_eq!(marker.as_deref(), Some("a/b/"));
    }

    #[test]
    fn test_parent_folder_marker_key_root_file() {
        let marker = Uploader::parent_folder_marker_key("file.txt");
        assert_eq!(marker, None);
    }

    #[test]
    fn test_supports_server_side_copy_when_endpoints_match() {
        let source = S3EndpointConfig {
            bucket: "source".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://s3.example.com/".to_string()),
            access_key: None,
            secret_key: None,
        };
        let destination = S3EndpointConfig {
            bucket: "destination".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://s3.example.com".to_string()),
            access_key: None,
            secret_key: None,
        };
        assert!(Uploader::supports_server_side_copy(&source, &destination));
    }

    #[test]
    fn test_supports_server_side_copy_when_endpoints_differ() {
        let source = S3EndpointConfig {
            bucket: "source".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://s3-a.example.com".to_string()),
            access_key: None,
            secret_key: None,
        };
        let destination = S3EndpointConfig {
            bucket: "destination".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://s3-b.example.com".to_string()),
            access_key: None,
            secret_key: None,
        };
        assert!(!Uploader::supports_server_side_copy(&source, &destination));
    }

    #[test]
    fn test_format_copy_source_encodes_key() {
        let formatted = Uploader::format_copy_source("my-bucket", "folder/a b+lambda-λ.txt");
        assert_eq!(
            formatted,
            "my-bucket/folder/a%20b%2Blambda-%CE%BB.txt".to_string()
        );
    }

    // --- Part Size Calculation Tests ---

    #[test]
    fn test_calculate_part_size_respects_minimum() {
        // Even with tiny config, should enforce 5MB minimum
        let part_size = Uploader::calculate_part_size(
            10 * 1024 * 1024, // 10MB file
            1,                // 1MB config (too small)
            4,                // 4 threads
        );

        assert_eq!(part_size, 5 * 1024 * 1024, "Should enforce 5MB minimum");
    }

    #[test]
    fn test_calculate_part_size_uses_config_default() {
        // Normal file with standard config
        let part_size = Uploader::calculate_part_size(
            1024 * 1024 * 1024, // 1GB file
            128,                // 128MB config (default)
            4,                  // 4 threads
        );

        // Should use config size since it's reasonable
        assert_eq!(part_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_part_size_parallelism_optimization() {
        // Small file with high concurrency - should create smaller parts
        let part_size = Uploader::calculate_part_size(
            100 * 1024 * 1024, // 100MB file
            128,               // 128MB config
            8,                 // 8 threads
        );

        // Should use smaller size for parallelism: 100MB/8 = 12.5MB (rounded to 12MB range)
        // But at least 5MB
        assert!(part_size >= 5 * 1024 * 1024);
        assert!(part_size <= 128 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_part_size_respects_10k_limit() {
        // Huge file that would exceed 10k parts with default size
        let file_size = 5_000_000_000_000u64; // 5TB
        let part_size = Uploader::calculate_part_size(
            file_size, 128, // 128MB config
            4,
        );

        // With 128MB parts, 5TB would need 40,960 parts (exceeds 10k limit)
        // So part size should be increased to: 5TB / 10000 = 500MB
        let required_min = (file_size / 10000) as usize;
        assert!(
            part_size >= required_min,
            "Part size {} should be at least {} to stay under 10k parts",
            part_size,
            required_min
        );

        let total_parts = (file_size as usize).div_ceil(part_size);
        assert!(
            total_parts <= 10000,
            "Should not exceed 10k parts, got {}",
            total_parts
        );
    }

    #[test]
    fn test_calculate_part_size_edge_case_exact_10k_parts() {
        // File that's exactly 10k * 5MB = 50GB (minimum possible multipart)
        let file_size = 10000 * 5 * 1024 * 1024u64;
        let part_size = Uploader::calculate_part_size(file_size, 128, 4);

        let total_parts = (file_size as usize).div_ceil(part_size);
        assert!(total_parts <= 10000);
    }

    #[test]
    fn test_calculate_part_size_small_file() {
        // Very small file (1MB) should still work
        let part_size = Uploader::calculate_part_size(
            1024 * 1024, // 1MB
            128,
            4,
        );

        // Should use minimum 5MB (larger than file, but that's ok)
        assert_eq!(part_size, 5 * 1024 * 1024);
    }

    // --- SHA256 Checksum Tests ---

    #[test]
    fn test_calculate_checksum_empty() {
        let checksum = Uploader::calculate_checksum(&[]);

        // SHA256 of empty string is known
        assert_eq!(checksum, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
    }

    #[test]
    fn test_calculate_checksum_known_value() {
        let data = b"Hello, World!";
        let checksum = Uploader::calculate_checksum(data);

        // SHA256 of "Hello, World!" is known
        // dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f
        assert_eq!(checksum, "3/1gIbsr1bCvZ2KQgJ7DpTGR3YHH9wpLKGiKNiGCmG8=");
    }

    #[test]
    fn test_calculate_checksum_different_inputs() {
        let checksum1 = Uploader::calculate_checksum(b"data1");
        let checksum2 = Uploader::calculate_checksum(b"data2");

        assert_ne!(
            checksum1, checksum2,
            "Different inputs should produce different checksums"
        );
    }

    #[test]
    fn test_calculate_checksum_deterministic() {
        let data = b"test data for checksumming";
        let checksum1 = Uploader::calculate_checksum(data);
        let checksum2 = Uploader::calculate_checksum(data);

        assert_eq!(
            checksum1, checksum2,
            "Same input should produce same checksum"
        );
    }

    #[test]
    fn test_calculate_checksum_large_data() {
        // Test with a larger chunk (simulating part of a file)
        let data = vec![0xAB; 5 * 1024 * 1024]; // 5MB of 0xAB
        let checksum = Uploader::calculate_checksum(&data);

        assert!(!checksum.is_empty());
        assert!(checksum.len() > 40); // Base64 of SHA256 is 44 chars
    }

    // --- Composite Checksum Tests ---

    #[test]
    fn test_calculate_composite_checksum_single_part() {
        // For single-part upload, composite is SHA256 of that one hash
        let hash1 = vec![0x01, 0x02, 0x03, 0x04];
        let composite = Uploader::calculate_composite_checksum(vec![hash1]);

        assert!(composite.ends_with("-1"), "Should end with part count");
        assert!(composite.len() > 5);
    }

    #[test]
    fn test_calculate_composite_checksum_multiple_parts() {
        let hash1 = vec![0x01; 32]; // Simulated SHA256 hash
        let hash2 = vec![0x02; 32];
        let hash3 = vec![0x03; 32];

        let composite = Uploader::calculate_composite_checksum(vec![hash1, hash2, hash3]);

        assert!(
            composite.ends_with("-3"),
            "Should end with part count: {}",
            composite
        );

        let parts: Vec<&str> = composite.split('-').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1], "3");
    }

    #[test]
    fn test_calculate_composite_checksum_format() {
        let hash1 = vec![0xFF; 32];
        let composite = Uploader::calculate_composite_checksum(vec![hash1]);

        // Should be in format: base64-count
        assert!(composite.contains('-'));
        let parts: Vec<&str> = composite.split('-').collect();
        assert_eq!(parts.len(), 2);

        // First part should be valid base64
        assert!(!parts[0].is_empty());
        // Second part should be numeric
        assert!(parts[1].parse::<usize>().is_ok());
    }

    #[test]
    fn test_calculate_composite_checksum_order_matters() {
        let hash1 = vec![0x01; 32];
        let hash2 = vec![0x02; 32];

        let composite1 = Uploader::calculate_composite_checksum(vec![hash1.clone(), hash2.clone()]);
        let composite2 = Uploader::calculate_composite_checksum(vec![hash2, hash1]);

        // Order of parts matters for checksum
        assert_ne!(
            composite1, composite2,
            "Part order should affect composite checksum"
        );
    }

    #[test]
    fn test_calculate_composite_checksum_matches_s3_format() {
        // S3 multipart checksum format is: base64(SHA256(hash1+hash2+...))-partcount
        let hash1 = vec![0xAB; 32];
        let hash2 = vec![0xCD; 32];

        let composite = Uploader::calculate_composite_checksum(vec![hash1, hash2]);

        // Verify format
        let parts: Vec<&str> = composite.split('-').collect();
        assert_eq!(parts.len(), 2, "Should have exactly one dash");
        assert_eq!(parts[1], "2", "Should have correct part count");

        // Base64 should be 44 characters for SHA256
        assert_eq!(parts[0].len(), 44, "Base64 of SHA256 should be 44 chars");
    }

    // --- Integration-like Tests (testing patterns without S3) ---

    #[test]
    fn test_part_size_for_various_file_sizes() {
        let test_cases = vec![
            (1024 * 1024, 128, 4, 5 * 1024 * 1024),          // 1MB file
            (100 * 1024 * 1024, 128, 4, 25 * 1024 * 1024),   // 100MB file (parallelism)
            (1024 * 1024 * 1024, 128, 4, 128 * 1024 * 1024), // 1GB file (config)
            (10 * 1024 * 1024 * 1024, 128, 4, 128 * 1024 * 1024), // 10GB file
        ];

        for (file_size, config_mb, concurrency, expected_min) in test_cases {
            let part_size = Uploader::calculate_part_size(file_size, config_mb, concurrency);
            assert!(
                part_size >= expected_min,
                "File size {} should have part size >= {}, got {}",
                file_size,
                expected_min,
                part_size
            );

            // Verify 10k parts constraint
            let total_parts = (file_size as usize).div_ceil(part_size);
            assert!(
                total_parts <= 10000,
                "File size {} created {} parts (exceeds 10k)",
                file_size,
                total_parts
            );
        }
    }

    #[test]
    fn test_checksum_workflow_single_part() {
        // Simulate single-part upload workflow
        let data = b"Small file content";

        // Calculate checksum for the part
        let part_checksum = Uploader::calculate_checksum(data);
        assert!(!part_checksum.is_empty());

        // For composite, we need the raw hash bytes
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash_bytes = hasher.finalize().to_vec();

        // Calculate composite
        let composite = Uploader::calculate_composite_checksum(vec![hash_bytes]);
        assert!(composite.ends_with("-1"));
    }

    #[test]
    fn test_checksum_workflow_multipart() {
        // Simulate 3-part upload
        let part1 = b"Part 1 data";
        let part2 = b"Part 2 data";
        let part3 = b"Part 3 data";

        // Calculate checksums for each part
        let mut hashes = Vec::new();
        for part in &[part1, part2, part3] {
            let mut hasher = Sha256::new();
            hasher.update(part);
            hashes.push(hasher.finalize().to_vec());
        }

        // Calculate composite
        let composite = Uploader::calculate_composite_checksum(hashes);
        assert!(composite.ends_with("-3"));

        // Verify format
        let parts: Vec<&str> = composite.split('-').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1], "3");
    }
}
