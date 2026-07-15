//! Persistent search materialization for the access-controlled Corpus Projection.

mod index;
mod read;
mod schema;
mod search;
mod source;

pub use index::{IndexError, IndexRoot, MIN_WRITER_HEAP_BYTES, OpenedIndex, PersistentCorpusIndex};
pub use read::CodingSessionEdge;
pub use schema::{
    INDEX_FORMAT_VERSION, IndexCommitMetadata, IndexSchema, asc_sort_key, desc_sort_key,
};
pub use source::CorpusIndexSource;
