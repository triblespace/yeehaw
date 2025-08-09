use std::any::Any;
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use rayon::{ThreadPool, ThreadPoolBuilder};

use super::segment_manager::SegmentManager;
use crate::core::META_FILEPATH;
use crate::directory::{Directory, DirectoryClone, GarbageCollectionResult};
use crate::fastfield::AliveBitSet;
use crate::index::{Index, IndexMeta, IndexSettings, Segment, SegmentId, SegmentMeta};
use crate::indexer::merge_operation::MergeOperationInventory;
use crate::indexer::merger::IndexMerger;
use crate::indexer::segment_manager::SegmentsStatus;
use crate::indexer::{
    DefaultMergePolicy, MergeCandidate, MergeOperation, MergePolicy, SegmentEntry,
    SegmentSerializer,
};
use crate::{FutureResult, Opstamp, TantivyError};

const PANIC_CAUGHT: &str = "Panic caught in merge thread";

/// Save the index meta file.
/// This operation is atomic:
/// Either
/// - it fails, in which case an error is returned, and the `meta.json` remains untouched,
/// - it success, and `meta.json` is written and flushed.
///
/// This method is not part of tantivy's public API
pub(crate) fn save_metas(metas: &IndexMeta, directory: &dyn Directory) -> crate::Result<()> {
    info!("save metas");
    let mut buffer = serde_json::to_vec_pretty(metas)?;
    // Just adding a new line at the end of the buffer.
    writeln!(&mut buffer)?;
    crate::fail_point!("save_metas", |msg| Err(crate::TantivyError::from(
        std::io::Error::new(
            std::io::ErrorKind::Other,
            msg.unwrap_or_else(|| "Undefined".to_string())
        )
    )));
    directory.sync_directory()?;
    directory.atomic_write(&META_FILEPATH, &buffer[..])?;
    debug!("Saved metas {:?}", serde_json::to_string_pretty(&metas));
    Ok(())
}

// The segment update runner is in charge of processing all
//  of the `SegmentUpdate`s.
//
// All this processing happens on a single thread
// consuming a common queue.
//
// We voluntarily pass a merge_operation ref to guarantee that
// the merge_operation is alive during the process
#[derive(Clone)]
pub(crate) struct SegmentUpdater(Arc<InnerSegmentUpdater>);

impl Deref for SegmentUpdater {
    type Target = InnerSegmentUpdater;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn garbage_collect_files(
    segment_updater: SegmentUpdater,
) -> crate::Result<GarbageCollectionResult> {
    info!("Running garbage collection");
    let mut index = segment_updater.index.clone();
    index
        .directory_mut()
        .garbage_collect(move || segment_updater.list_files())
}

/// Merges a list of segments the list of segment givens in the `segment_entries`.
/// This function happens in the calling thread and is computationally expensive.
fn merge(index: &Index, segment_entries: Vec<SegmentEntry>) -> crate::Result<Option<SegmentEntry>> {
    let num_docs = segment_entries
        .iter()
        .map(|segment| segment.meta().num_docs() as u64)
        .sum::<u64>();
    if num_docs == 0 {
        return Ok(None);
    }

    let merged_segment = index.new_segment();

    let segments: Vec<Segment> = segment_entries
        .iter()
        .map(|segment_entry| index.segment(segment_entry.meta().clone()))
        .collect();

    // An IndexMerger is like a "view" of our merged segments.
    let merger: IndexMerger = IndexMerger::open(index.schema(), &segments[..])?;

    // ... we just serialize this index merger in our new segment to merge the segments.
    let segment_serializer = SegmentSerializer::for_segment(merged_segment.clone())?;

    let num_docs = merger.write(segment_serializer)?;
    let merged_segment_id = merged_segment.id();
    let segment_meta = index.new_segment_meta(merged_segment_id, num_docs);
    Ok(Some(SegmentEntry::new(segment_meta)))
}

/// Advanced: Merges a list of segments from different indices in a new index.
///
/// Returns `TantivyError` if the indices list is empty or their
/// schemas don't match.
///
/// `output_directory`: is assumed to be empty.
///
/// # Warning
/// This function does NOT check or take the `IndexWriter` is running. It is not
/// meant to work if you have an `IndexWriter` running for the origin indices, or
/// the destination `Index`.
#[doc(hidden)]
pub fn merge_indices<T: Into<Box<dyn Directory>>>(
    indices: &[Index],
    output_directory: T,
) -> crate::Result<Index> {
    if indices.is_empty() {
        // If there are no indices to merge, there is no need to do anything.
        return Err(crate::TantivyError::InvalidArgument(
            "No indices given to merge".to_string(),
        ));
    }

    let target_settings = indices[0].settings().clone();

    // let's check that all of the indices have the same index settings
    if indices
        .iter()
        .skip(1)
        .any(|index| index.settings() != &target_settings)
    {
        return Err(crate::TantivyError::InvalidArgument(
            "Attempt to merge indices with different index_settings".to_string(),
        ));
    }

    let mut segments: Vec<Segment> = Vec::new();
    for index in indices {
        segments.extend(index.searchable_segments()?);
    }

    let non_filter = segments.iter().map(|_| None).collect::<Vec<_>>();
    merge_filtered_segments(&segments, target_settings, non_filter, output_directory)
}

/// Advanced: Merges a list of segments from different indices in a new index.
/// Additional you can provide a delete bitset for each segment to ignore doc_ids.
///
/// Returns `TantivyError` if the indices list is empty or their
/// schemas don't match.
///
/// `output_directory`: is assumed to be empty.
///
/// # Warning
/// This function does NOT check or take the `IndexWriter` is running. It is not
/// meant to work if you have an `IndexWriter` running for the origin indices, or
/// the destination `Index`.
#[doc(hidden)]
pub fn merge_filtered_segments<T: Into<Box<dyn Directory>>>(
    segments: &[Segment],
    target_settings: IndexSettings,
    filter_doc_ids: Vec<Option<AliveBitSet>>,
    output_directory: T,
) -> crate::Result<Index> {
    if segments.is_empty() {
        // If there are no indices to merge, there is no need to do anything.
        return Err(crate::TantivyError::InvalidArgument(
            "No segments given to merge".to_string(),
        ));
    }

    let target_schema = segments[0].schema();

    // let's check that all of the indices have the same schema
    if segments
        .iter()
        .skip(1)
        .any(|index| index.schema() != target_schema)
    {
        return Err(crate::TantivyError::InvalidArgument(
            "Attempt to merge different schema indices".to_string(),
        ));
    }

    let mut merged_index = Index::create(
        output_directory,
        target_schema.clone(),
        target_settings.clone(),
    )?;
    let merged_segment = merged_index.new_segment();
    let merged_segment_id = merged_segment.id();
    let merger: IndexMerger =
        IndexMerger::open_with_custom_alive_set(merged_index.schema(), segments, filter_doc_ids)?;
    let segment_serializer = SegmentSerializer::for_segment(merged_segment)?;
    let num_docs = merger.write(segment_serializer)?;

    let segment_meta = merged_index.new_segment_meta(merged_segment_id, num_docs);

    let stats = format!(
        "Segments Merge: [{}]",
        segments
            .iter()
            .fold(String::new(), |sum, current| format!(
                "{sum}{} ",
                current.meta().id().uuid_string()
            ))
            .trim_end()
    );

    let index_meta = IndexMeta {
        index_settings: target_settings, // index_settings of all segments should be the same
        segments: vec![segment_meta],
        schema: target_schema,
        payload: Some(stats),
    };

    // save the meta.json
    save_metas(&index_meta, merged_index.directory_mut())?;

    Ok(merged_index)
}

pub(crate) struct InnerSegmentUpdater {
    // we keep a copy of the current active IndexMeta to
    // avoid loading the file every time we need it in the
    // `SegmentUpdater`.
    //
    // This should be up to date as all update happen through
    // the unique active `SegmentUpdater`.
    active_index_meta: RwLock<Arc<IndexMeta>>,
    pool: ThreadPool,
    merge_thread_pool: ThreadPool,

    index: Index,
    segment_manager: SegmentManager,
    merge_policy: RwLock<Arc<dyn MergePolicy>>,
    killed: AtomicBool,
    merge_operations: MergeOperationInventory,
}

impl SegmentUpdater {
    pub fn create(index: Index, num_merge_threads: usize) -> crate::Result<SegmentUpdater> {
        let segments = index.searchable_segment_metas()?;
        let segment_manager = SegmentManager::from_segments(segments);
        let pool = ThreadPoolBuilder::new()
            .thread_name(|_| "segment_updater".to_string())
            .num_threads(1)
            .build()
            .map_err(|_| {
                crate::TantivyError::SystemError(
                    "Failed to spawn segment updater thread".to_string(),
                )
            })?;
        let merge_thread_pool = ThreadPoolBuilder::new()
            .thread_name(|i| format!("merge_thread_{i}"))
            .num_threads(num_merge_threads)
            .panic_handler(move |panic| {
                // We don't print the panic content itself,
                // it is already printed during the unwinding
                if let Some(message) = panic.downcast_ref::<&str>() {
                    if *message != PANIC_CAUGHT {
                        error!("uncaught merge panic")
                    }
                }
            })
            .build()
            .map_err(|_| {
                crate::TantivyError::SystemError(
                    "Failed to spawn segment merging thread".to_string(),
                )
            })?;
        let index_meta = index.load_metas()?;
        Ok(SegmentUpdater(Arc::new(InnerSegmentUpdater {
            active_index_meta: RwLock::new(Arc::new(index_meta)),
            pool,
            merge_thread_pool,
            index,
            segment_manager,
            merge_policy: RwLock::new(Arc::new(DefaultMergePolicy::default())),
            killed: AtomicBool::new(false),
            merge_operations: Default::default(),
        })))
    }

    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.merge_policy.read().unwrap().clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        let arc_merge_policy = Arc::from(merge_policy);
        *self.merge_policy.write().unwrap() = arc_merge_policy;
    }

    fn schedule_task<T: 'static + Send, F: FnOnce() -> crate::Result<T> + 'static + Send>(
        &self,
        task: F,
    ) -> FutureResult<T> {
        if !self.is_alive() {
            return crate::TantivyError::SystemError("Segment updater killed".to_string()).into();
        }
        let (scheduled_result, sender) = FutureResult::create(
            "A segment_updater future did not succeed. This should never happen.",
        );
        self.pool.spawn(|| {
            let task_result = task();
            let _ = sender.send(task_result);
        });
        scheduled_result
    }

    pub fn schedule_add_segment(&self, segment_entry: SegmentEntry) -> FutureResult<()> {
        let segment_updater = self.clone();
        self.schedule_task(move || {
            segment_updater.segment_manager.add_segment(segment_entry);
            segment_updater.consider_merge_options();
            Ok(())
        })
    }

    /// Orders `SegmentManager` to remove all segments
    pub(crate) fn remove_all_segments(&self) {
        self.segment_manager.remove_all_segments();
    }

    pub fn kill(&mut self) {
        self.killed.store(true, Ordering::Release);
    }

    pub fn is_alive(&self) -> bool {
        !self.killed.load(Ordering::Acquire)
    }

    pub fn save_metas(&self, commit_message: Option<String>) -> crate::Result<()> {
        if self.is_alive() {
            let index = &self.index;
            let directory = index.directory();
            let mut committed_segment_metas = self.segment_manager.committed_segment_metas();

            // We sort segment_readers by number of documents.
            // This is an heuristic to make multithreading more efficient.
            //
            // This is not done at the searcher level because I had a strange
            // use case in which I was dealing with a large static index,
            // dispatched over 5 SSD drives.
            //
            // A `UnionDirectory` makes it possible to read from these
            // 5 different drives and creates a meta.json on the fly.
            // In order to optimize the throughput, it creates a lasagna of segments
            // from the different drives.
            //
            // Segment 1 from disk 1, Segment 1 from disk 2, etc.
            committed_segment_metas.sort_by_key(|segment_meta| -(segment_meta.max_doc() as i32));
            let index_meta = IndexMeta {
                index_settings: index.settings().clone(),
                segments: committed_segment_metas,
                schema: index.schema(),
                payload: commit_message,
            };
            // TODO add context to the error.
            save_metas(&index_meta, directory.box_clone().borrow_mut())?;
            self.store_meta(&index_meta);
        }
        Ok(())
    }

    pub fn schedule_garbage_collect(&self) -> FutureResult<GarbageCollectionResult> {
        let self_clone = self.clone();
        self.schedule_task(move || garbage_collect_files(self_clone))
    }

    /// List the files that are useful to the index.
    ///
    /// This does not include lock files, or files that are obsolete
    /// but have not yet been deleted by the garbage collector.
    fn list_files(&self) -> HashSet<PathBuf> {
        let mut files: HashSet<PathBuf> = self
            .index
            .list_all_segment_metas()
            .into_iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();
        files.insert(META_FILEPATH.to_path_buf());
        files
    }

    pub(crate) fn schedule_commit(
        &self,
        _opstamp: Opstamp,
        payload: Option<String>,
    ) -> FutureResult<Opstamp> {
        let segment_updater: SegmentUpdater = self.clone();
        self.schedule_task(move || {
            let segment_entries = segment_updater.segment_manager.segment_entries();
            segment_updater.segment_manager.commit(segment_entries);
            segment_updater.save_metas(payload)?;
            let _ = garbage_collect_files(segment_updater.clone());
            segment_updater.consider_merge_options();
            Ok(0)
        })
    }

    fn store_meta(&self, index_meta: &IndexMeta) {
        *self.active_index_meta.write().unwrap() = Arc::new(index_meta.clone());
    }

    fn load_meta(&self) -> Arc<IndexMeta> {
        self.active_index_meta.read().unwrap().clone()
    }

    pub(crate) fn make_merge_operation(&self, segment_ids: &[SegmentId]) -> MergeOperation {
        MergeOperation::new(&self.merge_operations, 0, segment_ids.to_vec())
    }

    // Starts a merge operation. This function will block until the merge operation is effectively
    // started. Note that it does not wait for the merge to terminate.
    // The calling thread should not be block for a long time, as this only involve waiting for the
    // `SegmentUpdater` queue which in turns only contains lightweight operations.
    //
    // The merge itself happens on a different thread.
    //
    // When successful, this function returns a `Future` for a `Result<SegmentMeta>` that represents
    // the actual outcome of the merge operation.
    //
    // It returns an error if for some reason the merge operation could not be started.
    //
    // At this point an error is not necessarily the sign of a malfunction.
    // (e.g. A rollback could have happened, between the instant when the merge operation was
    // suggested and the moment when it ended up being executed.)
    //
    // `segment_ids` is required to be non-empty.
    pub fn start_merge(
        &self,
        merge_operation: MergeOperation,
    ) -> FutureResult<Option<SegmentMeta>> {
        assert!(
            !merge_operation.segment_ids().is_empty(),
            "Segment_ids cannot be empty."
        );

        let segment_updater = self.clone();
        let segment_entries: Vec<SegmentEntry> = match self
            .segment_manager
            .start_merge(merge_operation.segment_ids())
        {
            Ok(segment_entries) => segment_entries,
            Err(err) => {
                warn!(
                    "Starting the merge failed for the following reason. This is not fatal. {err}"
                );
                return err.into();
            }
        };

        info!("Starting merge  - {:?}", merge_operation.segment_ids());

        let (scheduled_result, merging_future_send) =
            FutureResult::create("Merge operation failed.");

        self.merge_thread_pool.spawn(move || {
            // The fact that `merge_operation` is moved here is important.
            // Its lifetime is used to track how many merging thread are currently running,
            // as well as which segment is currently in merge and therefore should not be
            // candidate for another merge.
            let merge_panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                merge(&segment_updater.index, segment_entries)
            }));
            let merge_res = match merge_panic_res {
                Ok(merge_res) => merge_res,
                Err(panic_err) => {
                    let panic_str = if let Some(msg) = panic_err.downcast_ref::<&str>() {
                        *msg
                    } else if let Some(msg) = panic_err.downcast_ref::<String>() {
                        msg.as_str()
                    } else {
                        "UNKNOWN"
                    };
                    let _send_result = merging_future_send.send(Err(TantivyError::SystemError(
                        format!("Merge thread panicked: {panic_str}"),
                    )));
                    // Resume unwinding because we forced unwind safety with
                    // `std::panic::AssertUnwindSafe` Use a specific message so
                    // the panic_handler can double check that we properly caught the panic.
                    let boxed_panic_message: Box<dyn Any + Send> = Box::new(PANIC_CAUGHT);
                    std::panic::resume_unwind(boxed_panic_message);
                }
            };
            match merge_res {
                Ok(after_merge_segment_entry) => {
                    let res = segment_updater.end_merge(merge_operation, after_merge_segment_entry);
                    let _send_result = merging_future_send.send(res);
                }
                Err(merge_error) => {
                    warn!(
                        "Merge of {:?} was cancelled: {:?}",
                        merge_operation.segment_ids().to_vec(),
                        merge_error
                    );
                    if cfg!(test) {
                        panic!("{merge_error:?}");
                    }
                    let _send_result = merging_future_send.send(Err(merge_error));
                }
            }
        });

        scheduled_result
    }

    pub(crate) fn get_mergeable_segments(&self) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
        let merge_segment_ids: HashSet<SegmentId> = self.merge_operations.segment_in_merge();
        self.segment_manager
            .get_mergeable_segments(&merge_segment_ids)
    }

    fn consider_merge_options(&self) {
        let (committed_segments, uncommitted_segments) = self.get_mergeable_segments();

        // Committed segments cannot be merged with uncommitted_segments.
        // We therefore consider merges using these two sets of segments independently.
        let merge_policy = self.get_merge_policy();

        let current_opstamp = 0; // TODO integrate commit handle
        let mut merge_candidates: Vec<MergeOperation> = merge_policy
            .compute_merge_candidates(&uncommitted_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.merge_operations, current_opstamp, merge_candidate.0)
            })
            .collect();

        let committed_merge_candidates = merge_policy
            .compute_merge_candidates(&committed_segments)
            .into_iter()
            .map(|merge_candidate: MergeCandidate| {
                MergeOperation::new(&self.merge_operations, 0, merge_candidate.0)
            });
        merge_candidates.extend(committed_merge_candidates);

        for merge_operation in merge_candidates {
            // If a merge cannot be started this is not a fatal error.
            // We do log a warning in `start_merge`.
            drop(self.start_merge(merge_operation));
        }
    }

    /// Queues a `end_merge` in the segment updater and blocks until it is successfully processed.
    fn end_merge(
        &self,
        merge_operation: MergeOperation,
        after_merge_segment_entry: Option<SegmentEntry>,
    ) -> crate::Result<Option<SegmentMeta>> {
        let segment_updater = self.clone();
        let after_merge_segment_meta = after_merge_segment_entry
            .as_ref()
            .map(|after_merge_segment_entry| after_merge_segment_entry.meta().clone());
        self.schedule_task(move || {
            info!(
                "End merge {:?}",
                after_merge_segment_entry.as_ref().map(|entry| entry.meta())
            );
            {
                let previous_metas = segment_updater.load_meta();
                let segments_status = segment_updater
                    .segment_manager
                    .end_merge(merge_operation.segment_ids(), after_merge_segment_entry)?;

                if segments_status == SegmentsStatus::Committed {
                    segment_updater.save_metas(previous_metas.payload.clone())?;
                }

                segment_updater.consider_merge_options();
            } // we drop all possible handle to a now useless `SegmentMeta`.

            let _ = garbage_collect_files(segment_updater);
            Ok(())
        })
        .wait()?;
        Ok(after_merge_segment_meta)
    }

    /// Wait for current merging threads.
    ///
    /// Upon termination of the current merging threads,
    /// merge opportunity may appear.
    ///
    /// We keep waiting until the merge policy judges that
    /// no opportunity is available.
    ///
    /// Note that it is not required to call this
    /// method in your application.
    /// Terminating your application without letting
    /// merge terminate is perfectly safe.
    ///
    /// Obsolete files will eventually be cleaned up
    /// by the directory garbage collector.
    pub fn wait_merging_thread(&self) -> crate::Result<()> {
        self.merge_operations.wait_until_empty();
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
