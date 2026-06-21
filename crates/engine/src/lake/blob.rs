//! M03 Observation Lake — Content-Addressable Blob Store
//!
//! MVP: in-memory HashMap keyed by SHA-256 hex digest.

use sha2::{Digest, Sha256};
use std::collections::HashMap;

use lethe_core::domain::BlobRef;
use lethe_storage_api::BlobStorePort;

/// A simple content-addressable store for binary attachments.
#[derive(Debug, Default)]
pub struct BlobStore {
    blobs: HashMap<String, Vec<u8>>,
}

impl BlobStorePort for BlobStore {
    fn put_blob(&mut self, data: &[u8]) -> BlobRef {
        self.put(data)
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> Option<&[u8]> {
        self.get(blob_ref)
    }
}

impl BlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store bytes and return a `BlobRef` (`blob:sha256:<hex>`).
    /// Idempotent: storing the same content twice is a no-op.
    pub fn put(&mut self, data: &[u8]) -> BlobRef {
        let hash = hex::encode(Sha256::digest(data));
        self.blobs
            .entry(hash.clone())
            .or_insert_with(|| data.to_vec());
        BlobRef::new(format!("blob:sha256:{hash}"))
    }

    /// Retrieve stored bytes by BlobRef.
    pub fn get(&self, blob_ref: &BlobRef) -> Option<&[u8]> {
        let hash = blob_ref.as_str().strip_prefix("blob:sha256:")?;
        self.blobs.get(hash).map(|v| v.as_slice())
    }

    pub fn contains(&self, blob_ref: &BlobRef) -> bool {
        blob_ref
            .as_str()
            .strip_prefix("blob:sha256:")
            .is_some_and(|h| self.blobs.contains_key(h))
    }

    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get_round_trip() {
        let mut store = BlobStore::new();
        let data = b"hello world";
        let blob_ref = store.put(data);
        assert!(blob_ref.as_str().starts_with("blob:sha256:"));
        assert_eq!(store.get(&blob_ref), Some(data.as_slice()));
    }

    #[test]
    fn duplicate_content_deduplicates() {
        let mut store = BlobStore::new();
        let r1 = store.put(b"same");
        let r2 = store.put(b"same");
        assert_eq!(r1, r2);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn missing_blob_returns_none() {
        let store = BlobStore::new();
        let missing = BlobRef::new("blob:sha256:0000000000000000");
        assert!(store.get(&missing).is_none());
    }

    #[test]
    fn blob_store_port_conformance() {
        let mut store = BlobStore::new();
        lethe_storage_api::conformance::blob_store_round_trip(&mut store);
    }
}
