use std::ops::Range;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use smallvec::smallvec;

use super::operation::{AddOperation, UserOperation};
use super::segment_updater::SegmentUpdater;
use super::{AddBatch, AddBatchReceiver, AddBatchSender, PreparedCommit};
use crate::directory::{DirectoryLock, GarbageCollectionResult};
use crate::error::TantivyError;
use crate::index::{Index, Segment, SegmentId, SegmentMeta};
use crate::indexer::index_writer_status::IndexWriterStatus;
use crate::indexer::{MergePolicy, SegmentEntry, SegmentWriter};
use crate::schema::document::Document;
use crate::schema::TantivyDocument;
use crate::{FutureResult, Opstamp};

// Size of the margin for the `memory_arena`. A segment is closed when the remaining memory
// in the `memory_arena` goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: usize = 1_000_000;

// We impose the memory per thread to be at least 15 MB, as the baseline consumption is 12MB.
pub const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
pub const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

// We impose the number of index writer threads to be at most this.
pub const MAX_NUM_THREAD: usize = 8;

// Add document will block if the number of docs waiting in the queue to be indexed
// reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

fn error_in_index_worker_thread(context: &str) -> TantivyError {
    TantivyError::ErrorInThread(format!(
        "{context}. A worker thread encountered an error (io::Error most likely) or panicked."
    ))
}

#[derive(Clone, bon::Builder)]
/// A builder for creating a new [IndexWriter] for an index.
pub struct IndexWriterOptions {
    #[builder(default = MEMORY_BUDGET_NUM_BYTES_MIN)]
    /// The memory budget per indexer thread.
    ///
    /// When an indexer thread has buffered this much data in memory
    /// it will flush the segment to disk (although this is not searchable until commit is called.)
    memory_budget_per_thread: usize,
    #[builder(default = 1)]
    /// The number of indexer worker threads to use.
    num_worker_threads: usize,
    #[builder(default = 4)]
    /// Defines the number of merger threads to use.
    num_merge_threads: usize,
}

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independent [`Segment`], via
/// a `SegmentWriter` object.
pub struct IndexWriter<D: Document = TantivyDocument> {
    // the lock is just used to bind the
    // lifetime of the lock with that of the IndexWriter.
    _directory_lock: Option<DirectoryLock>,

    index: Index,

    options: IndexWriterOptions,

    workers_join_handle: Vec<JoinHandle<crate::Result<()>>>,

    index_writer_status: IndexWriterStatus<D>,
    operation_sender: AddBatchSender<D>,

    segment_updater: SegmentUpdater,

    worker_id: usize,

    committed_opstamp: Opstamp,
}

fn index_documents<D: Document>(
    memory_budget: usize,
    segment: Segment,
    grouped_document_iterator: &mut dyn Iterator<Item = AddBatch<D>>,
    segment_updater: &SegmentUpdater,
) -> crate::Result<()> {
    let mut segment_writer = SegmentWriter::for_segment(memory_budget, segment.clone())?;
    for document_group in grouped_document_iterator {
        for doc in document_group {
            segment_writer.add_document(doc)?;
        }
        let mem_usage = segment_writer.mem_usage();
        if mem_usage >= memory_budget - MARGIN_IN_BYTES {
            info!(
                "Buffer limit reached, flushing segment with maxdoc={}.",
                segment_writer.max_doc()
            );
            break;
        }
    }

    if !segment_updater.is_alive() {
        return Ok(());
    }

    let max_doc = segment_writer.max_doc();

    // this is ensured by the call to peek before starting
    // the worker thread.
    assert!(max_doc > 0);

    segment_writer.finalize()?;

    let segment_with_max_doc = segment.with_max_doc(max_doc);

    let meta = segment_with_max_doc.meta().clone();
    // update segment_updater inventory to remove tempstore
    let segment_entry = SegmentEntry::new(meta);
    segment_updater.schedule_add_segment(segment_entry).wait()?;
    Ok(())
}

impl<D: Document> IndexWriter<D> {
    /// Create a new index writer. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// `num_threads` specifies the number of indexing workers that
    /// should work at the same time.
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub(crate) fn new(
        index: &Index,
        options: IndexWriterOptions,
        directory_lock: DirectoryLock,
    ) -> crate::Result<Self> {
        if options.memory_budget_per_thread < MEMORY_BUDGET_NUM_BYTES_MIN {
            let err_msg = format!(
                "The memory arena in bytes per thread needs to be at least \
                 {MEMORY_BUDGET_NUM_BYTES_MIN}."
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        if options.memory_budget_per_thread >= MEMORY_BUDGET_NUM_BYTES_MAX {
            let err_msg = format!(
                "The memory arena in bytes per thread cannot exceed {MEMORY_BUDGET_NUM_BYTES_MAX}"
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        if options.num_worker_threads == 0 {
            let err_msg = "At least one worker thread is required, got 0".to_string();
            return Err(TantivyError::InvalidArgument(err_msg));
        }

        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);

        let segment_updater = SegmentUpdater::create(index.clone(), options.num_merge_threads)?;

        let mut index_writer = Self {
            _directory_lock: Some(directory_lock),

            options: options.clone(),
            index: index.clone(),
            index_writer_status: IndexWriterStatus::from(document_receiver),
            operation_sender: document_sender,

            segment_updater,

            workers_join_handle: vec![],

            committed_opstamp: 0,

            worker_id: 0,
        };
        index_writer.start_workers()?;
        Ok(index_writer)
    }

    fn drop_sender(&mut self) {
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        self.operation_sender = sender;
    }

    /// Accessor to the index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// If there are some merging threads, blocks until they all finish their work and
    /// then drop the `IndexWriter`.
    pub fn wait_merging_threads(mut self) -> crate::Result<()> {
        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        self.drop_sender();

        let former_workers_handles = std::mem::take(&mut self.workers_join_handle);
        for join_handle in former_workers_handles {
            join_handle
                .join()
                .map_err(|_| error_in_index_worker_thread("Worker thread panicked."))?
                .map_err(|_| error_in_index_worker_thread("Worker thread failed."))?;
        }

        let result = self
            .segment_updater
            .wait_merging_thread()
            .map_err(|_| error_in_index_worker_thread("Failed to join merging thread."));

        if let Err(ref e) = result {
            error!("Some merging thread failed {e:?}");
        }

        result
    }

    #[doc(hidden)]
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> crate::Result<()> {
        let segment_entry = SegmentEntry::new(segment_meta);
        self.segment_updater
            .schedule_add_segment(segment_entry)
            .wait()
    }

    /// Creates a new segment.
    ///
    /// This method is useful only for users trying to do complex
    /// operations, like converting an index format to another.
    ///
    /// It is safe to start writing file associated with the new `Segment`.
    /// These will not be garbage collected as long as an instance object of
    /// `SegmentMeta` object associated with the new `Segment` is "alive".
    pub fn new_segment(&self) -> Segment {
        self.index.new_segment()
    }

    fn operation_receiver(&self) -> crate::Result<AddBatchReceiver<D>> {
        self.index_writer_status
            .operation_receiver()
            .ok_or_else(|| {
                crate::TantivyError::ErrorInThread(
                    "The index writer was killed. It can happen if an indexing worker encountered \
                     an Io error for instance."
                        .to_string(),
                )
            })
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    fn add_indexing_worker(&mut self) -> crate::Result<()> {
        let document_receiver_clone = self.operation_receiver()?;
        let index_writer_bomb = self.index_writer_status.create_bomb();

        let segment_updater = self.segment_updater.clone();

        let mem_budget = self.options.memory_budget_per_thread;
        let index = self.index.clone();
        let join_handle: JoinHandle<crate::Result<()>> = thread::Builder::new()
            .name(format!("thrd-tantivy-index{}", self.worker_id))
            .spawn(move || {
                loop {
                    let mut document_iterator = document_receiver_clone
                        .clone()
                        .into_iter()
                        .filter(|batch| !batch.is_empty())
                        .peekable();

                    // The peeking here is to avoid creating a new segment's files
                    // if no document are available.
                    //
                    // This is a valid guarantee as the peeked document now belongs to
                    // our local iterator.
                    if document_iterator.peek().is_none() {
                        // No more documents.
                        // It happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        index_writer_bomb.defuse();
                        return Ok(());
                    }

                    index_documents(
                        mem_budget,
                        index.new_segment(),
                        &mut document_iterator,
                        &segment_updater,
                    )?;
                }
            })?;
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }

    /// Accessor to the merge policy.
    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.segment_updater.get_merge_policy()
    }

    /// Setter for the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        self.segment_updater.set_merge_policy(merge_policy);
    }

    fn start_workers(&mut self) -> crate::Result<()> {
        for _ in 0..self.options.num_worker_threads {
            self.add_indexing_worker()?;
        }
        Ok(())
    }

    /// Detects and removes the files that are not used by the index anymore.
    pub fn garbage_collect_files(&self) -> FutureResult<GarbageCollectionResult> {
        self.segment_updater.schedule_garbage_collect()
    }

    /// Deletes all documents from the index
    ///
    /// Merges a given list of segments.
    ///
    /// If all segments are empty no new segment will be created.
    ///
    /// `segment_ids` is required to be non-empty.
    pub fn merge(&mut self, segment_ids: &[SegmentId]) -> FutureResult<Option<SegmentMeta>> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        let segment_updater = self.segment_updater.clone();
        segment_updater.start_merge(merge_operation)
    }

    /// Closes the current document channel send.
    /// and replace all the channels by new ones.
    ///
    /// The current workers will keep on indexing
    /// the pending document and stop
    /// when no documents are remaining.
    ///
    /// Returns the former segment_ready channel.
    fn recreate_document_channel(&mut self) {
        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);
        self.operation_sender = document_sender;
        self.index_writer_status = IndexWriterStatus::from(document_receiver);
    }

    /// Rollback to the last commit
    ///
    /// This cancels all of the updates that
    /// happened after the last commit.
    /// After calling rollback, the index is in the same
    /// state as it was after the last commit.
    ///
    /// The opstamp at the last commit is returned.
    pub fn rollback(&mut self) -> crate::Result<Opstamp> {
        info!("Rolling back to opstamp {}", self.committed_opstamp);
        // marks the segment updater as killed. From now on, all
        // segment updates will be ignored.
        self.segment_updater.kill();
        let document_receiver_res = self.operation_receiver();

        // take the directory lock to create a new index_writer.
        let directory_lock = self
            ._directory_lock
            .take()
            .expect("The IndexWriter does not have any lock. This is a bug, please report.");

        let new_index_writer = IndexWriter::new(&self.index, self.options.clone(), directory_lock)?;

        // the current `self` is dropped right away because of this call.
        //
        // This will drop the document queue, and the thread
        // should terminate.
        *self = new_index_writer;

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        //
        // This will reach an end as the only document_sender
        // was dropped with the index_writer.
        if let Ok(document_receiver) = document_receiver_res {
            for _ in document_receiver {}
        }

        Ok(self.committed_opstamp)
    }

    /// Prepares a commit.
    ///
    /// Calling `prepare_commit()` will cut the indexing
    /// queue. All pending documents will be sent to the
    /// indexing workers. They will then terminate, regardless
    /// of the size of their current segment and flush their
    /// work on disk.
    ///
    /// Once a commit is "prepared", you can either
    /// call
    /// * `.commit()`: to accept this commit
    /// * `.abort()`: to cancel this commit.
    ///
    /// In the current implementation, [`PreparedCommit`] borrows
    /// the [`IndexWriter`] mutably so we are guaranteed that no new
    /// document can be added as long as it is committed or is
    /// dropped.
    ///
    /// It is also possible to add a payload to the `commit`
    /// using this API.
    /// See [`PreparedCommit::set_payload()`].
    pub fn prepare_commit(&mut self) -> crate::Result<PreparedCommit<D>> {
        // Here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next commit have been
        // pushed too, because add_document can only happen
        // on this thread.
        //
        // This will move uncommitted segments to the state of
        // committed segments.
        info!("Preparing commit");

        // this will drop the current document channel
        // and recreate a new one.
        self.recreate_document_channel();

        let former_workers_join_handle = std::mem::take(&mut self.workers_join_handle);

        for worker_handle in former_workers_join_handle {
            let indexing_worker_result = worker_handle
                .join()
                .map_err(|e| TantivyError::ErrorInThread(format!("{e:?}")))?;
            indexing_worker_result?;
            self.add_indexing_worker()?;
        }

        let prepared_commit = PreparedCommit::new(self);
        info!("Prepared commit 0");
        Ok(prepared_commit)
    }

    /// Commits all of the pending changes
    ///
    /// A call to commit blocks.
    /// After it returns, all of the document that
    /// were added since the last commit are published
    /// and persisted.
    ///
    /// In case of a crash or an hardware failure (as
    /// long as the hard disk is spared), it will be possible
    /// to resume indexing from this point.
    ///
    /// Commit returns the `opstamp` of the last document
    /// that made it in the commit.
    pub fn commit(&mut self) -> crate::Result<Opstamp> {
        self.prepare_commit()?.commit()
    }

    pub(crate) fn segment_updater(&self) -> &SegmentUpdater {
        &self.segment_updater
    }

    /// Delete all documents containing a given term.
    ///
    /// Delete operation only affects documents that
    /// were added in previous commits, and documents
    /// that were added previously in the same commit.
    ///
    /// Like adds, the deletion itself will be visible
    /// only after calling `commit()`.
    /// Returns the opstamp of the last successful commit.
    ///
    /// This is, for instance, the opstamp the index will
    /// rollback to if there is a failure like a power surge.
    ///
    /// This is also the opstamp of the commit that is currently
    /// available for searchers.
    pub fn commit_opstamp(&self) -> Opstamp {
        self.committed_opstamp
    }

    /// Adds a document.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// The opstamp is an increasing `u64` that can
    /// be used by the client to align commits with its own
    /// document queue.
    pub fn add_document(&self, document: D) -> crate::Result<Opstamp> {
        self.send_add_documents_batch(smallvec![AddOperation {
            opstamp: 0,
            document
        }])?;
        Ok(0)
    }

    /// Gets a range of stamps.
    ///
    /// TODO integrate commit handle.
    fn get_batch_opstamps(&self, count: Opstamp) -> (Opstamp, Range<Opstamp>) {
        (0, 0..count)
    }

    /// Runs a group of document operations ensuring that the operations are
    /// assigned contiguous u64 opstamps and that add operations of the same
    /// group are flushed into the same segment.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// Each operation of the given `user_operations` will receive an in-order,
    /// contiguous u64 opstamp. The entire batch itself is also given an
    /// opstamp that is 1 greater than the last given operation. This
    /// `batch_opstamp` is the return value of `run`. An empty group of
    /// `user_operations`, an empty `Vec<UserOperation>`, still receives
    /// a valid opstamp even though no changes were _actually_ made to the index.
    ///
    /// Like adds (see `IndexWriter.add_document`), the changes made by calling
    /// `run` will be visible to readers only after calling `commit()`.
    pub fn run<I>(&self, user_operations: I) -> crate::Result<Opstamp>
    where
        I: IntoIterator<Item = UserOperation<D>>,
        I::IntoIter: ExactSizeIterator,
    {
        let user_operations_it = user_operations.into_iter();
        let count = user_operations_it.len() as u64;
        if count == 0 {
            return Ok(0);
        }
        let (_batch_opstamp, stamps) = self.get_batch_opstamps(count);

        let mut adds = AddBatch::default();

        for (user_op, _opstamp) in user_operations_it.zip(stamps) {
            if let UserOperation::Add(document) = user_op {
                let add_operation = AddOperation {
                    opstamp: 0,
                    document,
                };
                adds.push(add_operation);
            }
        }
        self.send_add_documents_batch(adds)?;
        Ok(0)
    }

    fn send_add_documents_batch(&self, add_ops: AddBatch<D>) -> crate::Result<()> {
        if self.index_writer_status.is_alive() && self.operation_sender.send(add_ops).is_ok() {
            Ok(())
        } else {
            Err(error_in_index_worker_thread("An index writer was killed."))
        }
    }
}

impl<D: Document> Drop for IndexWriter<D> {
    fn drop(&mut self) {
        self.segment_updater.kill();
        self.drop_sender();
        for work in self.workers_join_handle.drain(..) {
            let _ = work.join();
        }
    }
}

#[cfg(test)]
mod tests {}
