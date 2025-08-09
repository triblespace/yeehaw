//! Indexing and merging data.
//!
//! Contains code to create and merge segments.
//! `IndexWriter` is the main entry point for that, which created from
//! [`Index::writer`](crate::Index::writer).

pub(crate) mod path_to_unordered_id;

pub(crate) mod doc_id_mapping;
mod flat_map_with_buffer;
pub(crate) mod index_writer;
pub(crate) mod index_writer_status;
mod log_merge_policy;
mod merge_operation;
pub(crate) mod merge_policy;
pub(crate) mod merger;
pub(crate) mod operation;
pub(crate) mod prepared_commit;
mod segment_entry;
mod segment_manager;
mod segment_register;
pub(crate) mod segment_serializer;
pub(crate) mod segment_updater;
pub(crate) mod segment_writer;
pub(crate) mod single_segment_index_writer;

use crossbeam_channel as channel;
use smallvec::SmallVec;

pub use self::index_writer::{IndexWriter, IndexWriterOptions};
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
use self::operation::AddOperation;
pub use self::operation::UserOperation;
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub(crate) use self::segment_serializer::SegmentSerializer;
pub use self::segment_updater::{merge_filtered_segments, merge_indices};
pub use self::segment_writer::SegmentWriter;
pub use self::single_segment_index_writer::SingleSegmentIndexWriter;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;

// Batch of documents.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous doc_ids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type AddBatch<D> = SmallVec<[AddOperation<D>; 4]>;
type AddBatchSender<D> = channel::Sender<AddBatch<D>>;
type AddBatchReceiver<D> = channel::Receiver<AddBatch<D>>;

#[cfg(test)]
mod tests {}
