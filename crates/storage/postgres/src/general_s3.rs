use std::collections::BTreeSet;
use std::io::Read;
use std::time::Duration;

use chrono::{DateTime, Utc};
use lethe_core::domain::BlobRef;
use lethe_storage_api::{BlobStore, StorageError, StorageResult, blob_ref_sha256};
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::PostgresPersistence;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const SERVICE: &str = "s3";
const BLOB_ADMISSION_LOCK_KEY: &str = "lethe:blob-reference-admission";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3TransportPolicy {
    RequiredTls,
    TestHttp,
}

#[derive(Clone)]
pub struct S3BlobStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub transport_policy: S3TransportPolicy,
    pub timeout: Duration,
    pub max_object_bytes: usize,
    pub orphan_min_age: Duration,
}

pub struct S3BlobStore {
    client: Client,
    endpoint: reqwest::Url,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    path_style: bool,
    max_object_bytes: usize,
    orphan_min_age: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct S3Object {
    pub object_key: String,
    pub byte_count: usize,
    pub last_modified: DateTime<Utc>,
}

impl S3BlobStore {
    pub fn connect(config: S3BlobStoreConfig) -> StorageResult<Self> {
        validate_config(&config)?;
        let endpoint = reqwest::Url::parse(&config.endpoint)
            .map_err(|error| StorageError::Invariant(format!("invalid S3 endpoint: {error}")))?;
        let client = Client::builder()
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| StorageError::Backend(format!("building S3 HTTP client: {error}")))?;
        let store = Self {
            client,
            endpoint,
            region: config.region,
            bucket: config.bucket,
            access_key: config.access_key,
            secret_key: config.secret_key,
            path_style: config.path_style,
            max_object_bytes: config.max_object_bytes,
            orphan_min_age: config.orphan_min_age,
        };
        store.require_bucket()?;
        Ok(store)
    }

    pub fn max_object_bytes(&self) -> usize {
        self.max_object_bytes
    }

    pub fn orphan_min_age(&self) -> Duration {
        self.orphan_min_age
    }

    pub fn object_key(blob_ref: &BlobRef) -> StorageResult<String> {
        let _ = blob_ref_sha256(blob_ref)?;
        Ok(format!(
            "sha256/{}",
            blob_ref
                .as_str()
                .strip_prefix("blob:sha256:")
                .ok_or_else(|| StorageError::Invariant(
                    "validated blob prefix vanished".to_owned()
                ))?
        ))
    }

    pub fn verify(&self, blob_ref: &BlobRef) -> StorageResult<Option<usize>> {
        self.get_blob(blob_ref).and_then(|value| {
            value
                .map(|bytes| {
                    if bytes.len() > self.max_object_bytes {
                        Err(StorageError::Invariant(format!(
                            "stored S3 object {} exceeds configured maximum {}",
                            blob_ref, self.max_object_bytes
                        )))
                    } else {
                        Ok(bytes.len())
                    }
                })
                .transpose()
        })
    }

    pub(crate) fn deep_probe(&self) -> StorageResult<()> {
        let mut nonce = [0_u8; 32];
        SystemRandom::new().fill(&mut nonce).map_err(|_| {
            StorageError::Backend("operating system randomness is unavailable".to_owned())
        })?;
        let mut payload = b"lethe-s3-deep-probe-v1:".to_vec();
        payload.extend_from_slice(&nonce);
        let blob_ref = self.put_blob(&payload, self.max_object_bytes)?;
        let verified = self.get_blob(&blob_ref);
        let verified = match verified {
            Ok(Some(bytes)) if bytes == payload => Ok(()),
            Ok(Some(_)) => Err(StorageError::Invariant(format!(
                "S3 deep probe bytes differ at {blob_ref}"
            ))),
            Ok(None) => Err(StorageError::Invariant(format!(
                "S3 deep probe object disappeared at {blob_ref}"
            ))),
            Err(error) => Err(error),
        };
        if let Err(error) = verified {
            return match self.delete_object(&blob_ref) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(StorageError::Backend(format!(
                    "S3 deep probe failed ({error}) and cleanup failed ({cleanup})"
                ))),
            };
        }
        self.delete_object(&blob_ref)?;
        if self.get_blob(&blob_ref)?.is_some() {
            return Err(StorageError::Invariant(format!(
                "S3 deep probe DELETE left object visible at {blob_ref}"
            )));
        }
        Ok(())
    }

    pub(crate) fn list_objects(&self) -> StorageResult<Vec<S3Object>> {
        let mut continuation = None;
        let mut objects = Vec::new();
        loop {
            let mut url = self.bucket_url()?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("list-type", "2");
                query.append_pair("max-keys", "1000");
                query.append_pair("prefix", "sha256/");
                if let Some(token) = continuation.as_deref() {
                    query.append_pair("continuation-token", token);
                }
            }
            let response = self.send_signed("GET", url, EMPTY_SHA256, None)?;
            if !response.status().is_success() {
                return Err(http_error("S3 ListObjectsV2", response));
            }
            let xml = bounded_response_text(response, 8 * 1024 * 1024)?;
            let page: ListBucketResult = quick_xml::de::from_str(&xml).map_err(|error| {
                StorageError::Invariant(format!("S3 ListObjectsV2 XML is invalid: {error}"))
            })?;
            for object in page.contents {
                let blob_ref = blob_ref_from_object_key(&object.key)?;
                let expected_key = Self::object_key(&blob_ref)?;
                if object.key != expected_key {
                    return Err(StorageError::Invariant(format!(
                        "S3 returned non-canonical object key {:?}",
                        object.key
                    )));
                }
                let byte_count = usize::try_from(object.size).map_err(|_| {
                    StorageError::Invariant("S3 object size exceeds usize".to_owned())
                })?;
                let last_modified = DateTime::parse_from_rfc3339(&object.last_modified)
                    .map_err(|error| {
                        StorageError::Invariant(format!(
                            "S3 object LastModified is invalid: {error}"
                        ))
                    })?
                    .with_timezone(&Utc);
                objects.push(S3Object {
                    object_key: object.key,
                    byte_count,
                    last_modified,
                });
            }
            if !page.is_truncated {
                break;
            }
            continuation = Some(page.next_continuation_token.ok_or_else(|| {
                StorageError::Invariant(
                    "truncated S3 ListObjectsV2 page omitted continuation token".to_owned(),
                )
            })?);
        }
        objects.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        Ok(objects)
    }

    pub(crate) fn delete_object(&self, blob_ref: &BlobRef) -> StorageResult<()> {
        let key = Self::object_key(blob_ref)?;
        let url = self.object_url(&key)?;
        let response = self.send_signed("DELETE", url, EMPTY_SHA256, None)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(http_error("S3 DELETE", response))
        }
    }

    fn require_bucket(&self) -> StorageResult<()> {
        let url = self.bucket_url()?;
        let response = self.send_signed("GET", url, EMPTY_SHA256, None)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(http_error("S3 bucket admission", response))
        }
    }

    fn object_url(&self, object_key: &str) -> StorageResult<reqwest::Url> {
        if object_key != percent_safe_object_key(object_key)? {
            return Err(StorageError::Invariant(format!(
                "S3 object key is not canonical: {object_key}"
            )));
        }
        let mut url = self.bucket_url()?;
        let base_path = url.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}/{object_key}"));
        Ok(url)
    }

    fn bucket_url(&self) -> StorageResult<reqwest::Url> {
        let mut url = self.endpoint.clone();
        if self.path_style {
            let base_path = url.path().trim_end_matches('/');
            url.set_path(&format!("{base_path}/{}/", self.bucket));
        } else {
            let host = url
                .host_str()
                .ok_or_else(|| StorageError::Invariant("S3 endpoint has no host".to_owned()))?;
            url.set_host(Some(&format!("{}.{}", self.bucket, host)))
                .map_err(|_| {
                    StorageError::Invariant("S3 virtual-host bucket is invalid".to_owned())
                })?;
        }
        Ok(url)
    }

    fn send_signed(
        &self,
        method: &str,
        url: reqwest::Url,
        payload_sha256: &str,
        body: Option<Vec<u8>>,
    ) -> StorageResult<reqwest::blocking::Response> {
        self.send_signed_at(method, url, payload_sha256, body, Utc::now())
    }

    fn send_signed_at(
        &self,
        method: &str,
        url: reqwest::Url,
        payload_sha256: &str,
        body: Option<Vec<u8>>,
        now: DateTime<Utc>,
    ) -> StorageResult<reqwest::blocking::Response> {
        let headers = signing_headers(
            method,
            &url,
            payload_sha256,
            &self.region,
            &self.access_key,
            &self.secret_key,
            now,
        )?;
        let mut request = match method {
            "GET" => self.client.get(url),
            "HEAD" => self.client.head(url),
            "PUT" => self.client.put(url),
            "DELETE" => self.client.delete(url),
            _ => {
                return Err(StorageError::Invariant(format!(
                    "unsupported S3 method {method}"
                )));
            }
        };
        request = apply_headers(request, &headers);
        if let Some(body) = body {
            request = request.body(body);
        }
        request
            .send()
            .map_err(|error| StorageError::Backend(format!("S3 {method} request failed: {error}")))
    }

    fn get_required(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        let Some(expected_length) = self.head_required(blob_ref)? else {
            return Ok(None);
        };
        let key = Self::object_key(blob_ref)?;
        let url = self.object_url(&key)?;
        let response = self.send_signed("GET", url, EMPTY_SHA256, None)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(http_error("S3 GET", response));
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > self.max_object_bytes as u64) {
            return Err(StorageError::Invariant(format!(
                "S3 object {} declares {} bytes above configured maximum {}",
                blob_ref,
                content_length.unwrap_or_default(),
                self.max_object_bytes
            )));
        }
        let read_limit = u64::try_from(self.max_object_bytes)
            .map_err(|_| StorageError::Invariant("S3 byte limit exceeds u64".to_owned()))?
            .checked_add(1)
            .ok_or_else(|| StorageError::Invariant("S3 byte limit overflow".to_owned()))?;
        let mut bytes = Vec::new();
        response
            .take(read_limit)
            .read_to_end(&mut bytes)
            .map_err(|error| StorageError::Backend(format!("reading S3 object: {error}")))?;
        if bytes.len() > self.max_object_bytes {
            return Err(StorageError::Invariant(format!(
                "S3 object {} exceeds configured maximum {}",
                blob_ref, self.max_object_bytes
            )));
        }
        if bytes.len() != expected_length {
            return Err(StorageError::Invariant(format!(
                "S3 HEAD/GET length differs for {blob_ref}: {expected_length} != {}",
                bytes.len()
            )));
        }
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != blob_ref_sha256(blob_ref)? {
            return Err(StorageError::Invariant(format!(
                "S3 object digest differs from {blob_ref}"
            )));
        }
        Ok(Some(bytes))
    }

    fn head_required(&self, blob_ref: &BlobRef) -> StorageResult<Option<usize>> {
        let key = Self::object_key(blob_ref)?;
        let url = self.object_url(&key)?;
        let response = self.send_signed("HEAD", url, EMPTY_SHA256, None)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(http_error("S3 HEAD", response));
        }
        let length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .ok_or_else(|| {
                StorageError::Invariant(format!("S3 HEAD omitted Content-Length for {blob_ref}"))
            })?
            .to_str()
            .map_err(|error| {
                StorageError::Invariant(format!(
                    "S3 HEAD Content-Length is not ASCII for {blob_ref}: {error}"
                ))
            })?
            .parse::<usize>()
            .map_err(|error| {
                StorageError::Invariant(format!(
                    "S3 HEAD Content-Length is invalid for {blob_ref}: {error}"
                ))
            })?;
        if length > self.max_object_bytes {
            return Err(StorageError::Invariant(format!(
                "S3 object {} declares {} bytes above configured maximum {}",
                blob_ref, length, self.max_object_bytes
            )));
        }
        Ok(Some(length))
    }
}

impl BlobStore for S3BlobStore {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef> {
        let limit = max_bytes.min(self.max_object_bytes);
        if data.len() > limit {
            return Err(StorageError::Invariant(format!(
                "blob contains {} bytes above maximum {limit}",
                data.len()
            )));
        }
        let digest = hex::encode(Sha256::digest(data));
        let blob_ref = BlobRef::new(format!("blob:sha256:{digest}"));
        if let Some(existing) = self.get_required(&blob_ref)? {
            if existing == data {
                return Ok(blob_ref);
            }
            return Err(StorageError::Invariant(format!(
                "existing S3 object differs at {blob_ref}"
            )));
        }
        let key = Self::object_key(&blob_ref)?;
        let url = self.object_url(&key)?;
        let response = self.send_signed("PUT", url, &digest, Some(data.to_vec()))?;
        if !response.status().is_success() {
            return Err(http_error("S3 PUT", response));
        }
        let stored = self.get_required(&blob_ref)?.ok_or_else(|| {
            StorageError::Invariant(format!("S3 PUT did not create object {blob_ref}"))
        })?;
        if stored != data {
            return Err(StorageError::Invariant(format!(
                "S3 PUT verification differs at {blob_ref}"
            )));
        }
        Ok(blob_ref)
    }

    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>> {
        let limit = max_bytes.min(self.max_object_bytes);
        for bytes in data {
            if bytes.len() > limit {
                return Err(StorageError::Invariant(format!(
                    "blob contains {} bytes above maximum {limit}",
                    bytes.len()
                )));
            }
        }
        data.iter()
            .map(|bytes| self.put_blob(bytes, limit))
            .collect()
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        self.get_required(blob_ref)
    }
}

impl BlobStore for PostgresPersistence {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef> {
        let mut refs = self.put_blobs(&[data], max_bytes)?;
        refs.pop().ok_or_else(|| {
            StorageError::Invariant("single S3 blob put returned no reference".to_owned())
        })
    }

    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>> {
        let store = self.admitted_blob_store()?;
        let mut writer = self.writer()?;
        lock_blob_admission_session(&mut writer)?;
        let result = put_blobs_with_metadata(&mut writer, store, data, max_bytes);
        let unlock = unlock_blob_admission_session(&mut writer);
        match (result, unlock) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        let Some(expected_bytes) = self.blob_metadata(blob_ref)? else {
            return Ok(None);
        };
        let bytes = self
            .admitted_blob_store()?
            .get_blob(blob_ref)?
            .ok_or_else(|| {
                StorageError::Invariant(format!(
                    "blob metadata references missing S3 object {blob_ref}"
                ))
            })?;
        if bytes.len() != expected_bytes {
            return Err(StorageError::Invariant(format!(
                "blob metadata byte count differs for {blob_ref}: {expected_bytes} != {}",
                bytes.len()
            )));
        }
        Ok(Some(bytes))
    }
}

fn put_blobs_with_metadata(
    writer: &mut postgres::Client,
    store: &S3BlobStore,
    data: &[&[u8]],
    max_bytes: usize,
) -> StorageResult<Vec<BlobRef>> {
    let refs = store.put_blobs(data, max_bytes)?;
    {
        let mut transaction = writer.transaction().map_err(backend)?;
        for (blob_ref, bytes) in refs.iter().zip(data.iter()) {
            let object_key = S3BlobStore::object_key(blob_ref)?;
            let byte_count = i64::try_from(bytes.len()).map_err(|_| {
                StorageError::Invariant("blob byte count exceeds PostgreSQL BIGINT".to_owned())
            })?;
            transaction
                .execute(
                    "INSERT INTO blob_objects (
                        blob_ref, object_key, byte_count
                     ) VALUES ($1, $2, $3)
                     ON CONFLICT (blob_ref) DO NOTHING",
                    &[
                        &blob_ref.as_str() as &(dyn postgres::types::ToSql + Sync),
                        &object_key,
                        &byte_count,
                    ],
                )
                .map_err(backend)?;
            let row = transaction
                .query_one(
                    "SELECT object_key, byte_count FROM blob_objects
                     WHERE blob_ref = $1",
                    &[&blob_ref.as_str()],
                )
                .map_err(backend)?;
            let stored_key: String = row.get(0);
            let stored_bytes: i64 = row.get(1);
            if stored_key != object_key || stored_bytes != byte_count {
                return Err(StorageError::Invariant(format!(
                    "blob metadata collision for {blob_ref}"
                )));
            }
        }
        transaction.commit().map_err(backend)?;
    }
    Ok(refs)
}

impl PostgresPersistence {
    pub(crate) fn verify_blob_references_admitted(
        &self,
        blob_refs: &[BlobRef],
    ) -> StorageResult<()> {
        let mut unique = BTreeSet::new();
        for blob_ref in blob_refs {
            if !unique.insert(blob_ref.as_str()) {
                continue;
            }
            if self.get_blob(blob_ref)?.is_none() {
                return Err(StorageError::Invariant(format!(
                    "referenced blob was not admitted through PostgreSQL metadata: {blob_ref}"
                )));
            }
        }
        Ok(())
    }

    fn blob_metadata(&self, blob_ref: &BlobRef) -> StorageResult<Option<usize>> {
        let expected_key = S3BlobStore::object_key(blob_ref)?;
        let mut reader = self.reader()?;
        let row = reader
            .query_opt(
                "SELECT object_key, byte_count FROM blob_objects
                 WHERE blob_ref = $1",
                &[&blob_ref.as_str()],
            )
            .map_err(backend)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let object_key: String = row.get(0);
        if object_key != expected_key {
            return Err(StorageError::Invariant(format!(
                "blob metadata object key differs for {blob_ref}"
            )));
        }
        let byte_count: i64 = row.get(1);
        usize::try_from(byte_count)
            .map(Some)
            .map_err(|_| StorageError::Invariant("blob byte count is invalid".to_owned()))
    }

    pub(crate) fn referenced_blob_refs(&self) -> StorageResult<BTreeSet<String>> {
        let mut reader = self.reader()?;
        referenced_blob_refs_with_client(&mut *reader)
    }
}

pub(super) fn lock_blob_admission(
    transaction: &mut postgres::Transaction<'_>,
) -> StorageResult<()> {
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&BLOB_ADMISSION_LOCK_KEY],
        )
        .map_err(backend)?;
    Ok(())
}

pub(super) fn lock_blob_admission_session(client: &mut postgres::Client) -> StorageResult<()> {
    client
        .query_one(
            "SELECT pg_advisory_lock(hashtextextended($1, 0))",
            &[&BLOB_ADMISSION_LOCK_KEY],
        )
        .map_err(backend)?;
    Ok(())
}

pub(super) fn unlock_blob_admission_session(client: &mut postgres::Client) -> StorageResult<()> {
    let unlocked: bool = client
        .query_one(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            &[&BLOB_ADMISSION_LOCK_KEY],
        )
        .map_err(backend)?
        .get(0);
    if unlocked {
        Ok(())
    } else {
        Err(StorageError::Invariant(
            "PostgreSQL blob admission session lock was not held".to_owned(),
        ))
    }
}

pub(super) fn referenced_blob_refs_with_client(
    client: &mut impl postgres::GenericClient,
) -> StorageResult<BTreeSet<String>> {
    let mut refs = BTreeSet::new();
    for row in client
        .query("SELECT observation_json::text FROM observations", &[])
        .map_err(backend)?
    {
        let json: String = row.get(0);
        let observation: lethe_core::domain::Observation =
            serde_json::from_str(&json).map_err(|error| {
                StorageError::Invariant(format!(
                    "stored observation JSON violates the domain schema: {error}"
                ))
            })?;
        refs.extend(
            observation
                .attachments
                .into_iter()
                .map(|blob_ref| blob_ref.as_str().to_owned()),
        );
    }
    for row in client
        .query("SELECT supplemental_json::text FROM supplementals", &[])
        .map_err(backend)?
    {
        let json: String = row.get(0);
        let supplemental: lethe_core::domain::SupplementalRecord = serde_json::from_str(&json)
            .map_err(|error| {
                StorageError::Invariant(format!(
                    "stored supplemental JSON violates the domain schema: {error}"
                ))
            })?;
        refs.extend(
            supplemental
                .derived_from
                .blobs
                .into_iter()
                .map(|blob_ref| blob_ref.as_str().to_owned()),
        );
        collect_json_blob_refs(&supplemental.payload, &mut refs);
    }
    for row in client
        .query(
            "SELECT DISTINCT blob_ref FROM projection_visible_blob_refs",
            &[],
        )
        .map_err(backend)?
    {
        refs.insert(row.get(0));
    }
    for value in &refs {
        let _ = blob_ref_sha256(&BlobRef::new(value.clone()))?;
    }
    Ok(refs)
}

fn collect_json_blob_refs(value: &serde_json::Value, refs: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(value) if value.starts_with("blob:sha256:") => {
            refs.insert(value.clone());
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_blob_refs(value, refs);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_json_blob_refs(value, refs);
            }
        }
        _ => {}
    }
}

#[derive(Debug)]
struct SigningHeaders {
    host: String,
    amz_date: String,
    payload_sha256: String,
    authorization: String,
}

fn signing_headers(
    method: &str,
    url: &reqwest::Url,
    payload_sha256: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    now: DateTime<Utc>,
) -> StorageResult<SigningHeaders> {
    if !matches!(method, "GET" | "HEAD" | "PUT" | "DELETE") {
        return Err(StorageError::Invariant(format!(
            "unsupported S3 signing method {method}"
        )));
    }
    validate_lower_hex("S3 payload SHA-256", payload_sha256, 64)?;
    let host = canonical_host(url)?;
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let canonical_uri = canonical_uri(url.path())?;
    let canonical_query = canonical_query(url);
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_sha256}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_sha256}"
    );
    let scope = format!("{date}/{region}/{SERVICE}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes());
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    Ok(SigningHeaders {
        host,
        amz_date,
        payload_sha256: payload_sha256.to_owned(),
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
    })
}

fn apply_headers(request: RequestBuilder, headers: &SigningHeaders) -> RequestBuilder {
    request
        .header("host", &headers.host)
        .header("x-amz-content-sha256", &headers.payload_sha256)
        .header("x-amz-date", &headers.amz_date)
        .header("authorization", &headers.authorization)
}

fn canonical_host(url: &reqwest::Url) -> StorageResult<String> {
    let host = url
        .host_str()
        .ok_or_else(|| StorageError::Invariant("S3 URL has no host".to_owned()))?;
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    })
}

fn canonical_uri(path: &str) -> StorageResult<String> {
    if !path.starts_with('/') {
        return Err(StorageError::Invariant(
            "S3 canonical URI must start with /".to_owned(),
        ));
    }
    Ok(path
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect())
}

fn canonical_query(url: &reqwest::Url) -> String {
    let mut pairs = url
        .query_pairs()
        .map(|(key, value)| {
            (
                aws_percent_encode(key.as_bytes(), true),
                aws_percent_encode(value.as_bytes(), true),
            )
        })
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_percent_encode(value: &[u8], encode_slash: bool) -> String {
    value
        .iter()
        .map(|byte| {
            if byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b'~')
                || (!encode_slash && *byte == b'/')
            {
                char::from(*byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn percent_safe_object_key(value: &str) -> StorageResult<String> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || value.contains("..")
    {
        return Err(StorageError::Invariant(format!(
            "unsafe S3 object key {value:?}"
        )));
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
    {
        Ok(value.to_owned())
    } else {
        Err(StorageError::Invariant(format!(
            "unsafe S3 object key {value:?}"
        )))
    }
}

fn validate_config(config: &S3BlobStoreConfig) -> StorageResult<()> {
    let endpoint = reqwest::Url::parse(&config.endpoint)
        .map_err(|error| StorageError::Invariant(format!("invalid S3 endpoint: {error}")))?;
    if !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !matches!(endpoint.path(), "" | "/")
    {
        return Err(StorageError::Invariant(
            "S3 endpoint must contain only scheme, host, and optional port".to_owned(),
        ));
    }
    let host = endpoint
        .host_str()
        .ok_or_else(|| StorageError::Invariant("S3 endpoint has no host".to_owned()))?;
    match config.transport_policy {
        S3TransportPolicy::RequiredTls if endpoint.scheme() != "https" => {
            return Err(StorageError::Invariant(
                "S3 endpoint must use HTTPS under required TLS policy".to_owned(),
            ));
        }
        S3TransportPolicy::TestHttp
            if endpoint.scheme() != "http"
                || !matches!(host, "localhost" | "127.0.0.1" | "::1" | "minio") =>
        {
            return Err(StorageError::Invariant(
                "test HTTP S3 endpoint must be localhost, loopback, or minio".to_owned(),
            ));
        }
        _ => {}
    }
    non_blank("S3 region", &config.region)?;
    validate_bucket(&config.bucket)?;
    non_blank("S3 access key", &config.access_key)?;
    non_blank("S3 secret key", &config.secret_key)?;
    if config.timeout.is_zero() || config.max_object_bytes == 0 || config.orphan_min_age.is_zero() {
        return Err(StorageError::Invariant(
            "S3 timeout, maximum object bytes, and orphan minimum age must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_bucket(bucket: &str) -> StorageResult<()> {
    let valid = (3..=63).contains(&bucket.len())
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
        && bucket
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(StorageError::Invariant(
            "S3 bucket must be a 3-63 character lowercase DNS label".to_owned(),
        ))
    }
}

fn validate_lower_hex(field: &str, value: &str, length: usize) -> StorageResult<()> {
    if value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(StorageError::Invariant(format!(
            "{field} must contain {length} lowercase hexadecimal characters"
        )))
    }
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), value)
        .as_ref()
        .to_vec()
}

fn http_error(operation: &str, response: reqwest::blocking::Response) -> StorageError {
    let status = response.status();
    let mut detail = String::new();
    let _ = response.take(4096).read_to_string(&mut detail);
    let detail = detail.trim();
    if detail.is_empty() {
        StorageError::Backend(format!("{operation} failed with HTTP {status}"))
    } else {
        StorageError::Backend(format!("{operation} failed with HTTP {status}: {detail}"))
    }
}

fn bounded_response_text(
    response: reqwest::blocking::Response,
    max_bytes: usize,
) -> StorageResult<String> {
    let read_limit = u64::try_from(max_bytes)
        .map_err(|_| StorageError::Invariant("response byte limit exceeds u64".to_owned()))?
        .checked_add(1)
        .ok_or_else(|| StorageError::Invariant("response byte limit overflow".to_owned()))?;
    let mut bytes = Vec::new();
    response
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| StorageError::Backend(format!("reading S3 response: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(StorageError::Invariant(format!(
            "S3 response exceeds maximum {max_bytes} bytes"
        )));
    }
    String::from_utf8(bytes)
        .map_err(|error| StorageError::Invariant(format!("S3 response is not UTF-8: {error}")))
}

fn blob_ref_from_object_key(object_key: &str) -> StorageResult<BlobRef> {
    let digest = object_key.strip_prefix("sha256/").ok_or_else(|| {
        StorageError::Invariant(format!(
            "S3 object key has unexpected prefix: {object_key:?}"
        ))
    })?;
    let blob_ref = BlobRef::new(format!("blob:sha256:{digest}"));
    let _ = blob_ref_sha256(&blob_ref)?;
    Ok(blob_ref)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<ListBucketObject>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketObject {
    key: String,
    last_modified: String,
    size: u64,
}

fn non_blank(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!(
            "{field} must not be blank"
        )))
    } else {
        Ok(())
    }
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn sigv4_fixture_is_stable() {
        let url = reqwest::Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let headers = signing_headers(
            "GET",
            &url,
            EMPTY_SHA256,
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(
            headers.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=df548e2ce037944d03f3e68682813b093763996d597cf890ca3d9037fd231eb4"
        );
    }

    #[test]
    fn unsafe_s3_boundaries_fail_before_io() {
        let base = S3BlobStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "ask-bot-test".to_owned(),
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            path_style: true,
            transport_policy: S3TransportPolicy::TestHttp,
            timeout: Duration::from_secs(1),
            max_object_bytes: 1024,
            orphan_min_age: Duration::from_secs(1),
        };
        validate_config(&base).unwrap();
        for endpoint in [
            "http://example.com:9000",
            "http://user@example.com:9000",
            "http://127.0.0.1:9000/path",
            "http://127.0.0.1:9000?query=x",
        ] {
            let mut invalid = base.clone();
            invalid.endpoint = endpoint.to_owned();
            assert!(validate_config(&invalid).is_err(), "{endpoint}");
        }
        assert!(percent_safe_object_key("../secret").is_err());
        assert!(percent_safe_object_key("sha256/%2f").is_err());
    }
}
