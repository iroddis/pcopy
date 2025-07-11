use anyhow::Result;
use async_channel::unbounded;
use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::fs as async_fs;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "pcopy")]
#[command(about = "A parallel file copy tool mimicking rsync -ar for local copies")]
struct Args {
    #[arg(short, long, default_value = "10")]
    parallelism: usize,

    source: PathBuf,
    destination: PathBuf,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    progress: bool,

    #[arg(long)]
    verbose: bool,

    #[arg(long)]
    debug: bool,
}

#[derive(Debug, Clone)]
struct ProgressCounter {
    discovered: Arc<AtomicU64>,
    flagged_for_copy: Arc<AtomicU64>,
    copied: Arc<AtomicU64>,
    bytes_copied: Arc<AtomicU64>,
    start_time: Instant,
}

impl ProgressCounter {
    fn new() -> Self {
        Self {
            discovered: Arc::new(AtomicU64::new(0)),
            flagged_for_copy: Arc::new(AtomicU64::new(0)),
            copied: Arc::new(AtomicU64::new(0)),
            bytes_copied: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    fn increment_discovered(&self) {
        self.discovered.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_flagged(&self) {
        self.flagged_for_copy.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_copied(&self, bytes: u64) {
        self.copied.fetch_add(1, Ordering::Relaxed);
        self.bytes_copied.fetch_add(bytes, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (u64, u64, u64, u64, f64) {
        let discovered = self.discovered.load(Ordering::Relaxed);
        let flagged = self.flagged_for_copy.load(Ordering::Relaxed);
        let copied = self.copied.load(Ordering::Relaxed);
        let bytes = self.bytes_copied.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        };
        (discovered, flagged, copied, bytes, speed)
    }
}

async fn progress_display_task(progress: ProgressCounter) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    loop {
        interval.tick().await;
        let (discovered, flagged, copied, bytes, speed) = progress.get_stats();

        // Format bytes nicely
        let bytes_str = format_bytes(bytes);
        let speed_str = format_bytes(speed as u64);

        // Clear line and print progress
        print!("\r\x1b[2K"); // Clear current line
        print!(
            "Discovered: {} | Flagged: {} | Copied: {} | Size: {} | Speed: {}/s",
            discovered, flagged, copied, bytes_str, speed_str
        );
        use std::io::{self, Write};
        io::stdout().flush().unwrap();

        // If we're done (no more discovery happening), we can exit
        // For now, we'll just continue indefinitely
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", size as u64, UNITS[unit_idx])
    } else {
        format!("{:.1} {}", size, UNITS[unit_idx])
    }
}

#[derive(Debug, Clone)]
struct FileTask {
    source_path: PathBuf,
    dest_path: PathBuf,
    source_metadata: std::fs::Metadata,
    dest_metadata: Option<std::fs::Metadata>,
    symlink_target: Option<PathBuf>,
}

impl FileTask {
    // If the destination exists, then only consider the size. Otherwise flag for copy.
    // Metadata will get set as a separate step.
    fn needs_copy(&self) -> bool {
        if let Some(dm) = &self.dest_metadata {
            let sm = &self.source_metadata;
            dm.len() != sm.len()
        } else {
            true
        }
    }

    // Transform symlink target path for absolute links
    fn transform_symlink_target(&self, source_root: &Path, dest_root: &Path) -> Option<PathBuf> {
        if let Some(target) = &self.symlink_target {
            if target.is_absolute() {
                // For absolute symlinks, check if they point within the source tree
                // First try to canonicalize, but if that fails, try direct prefix matching
                if let Ok(canonical_target) = target.canonicalize() {
                    if let Ok(relative_to_source) = canonical_target.strip_prefix(source_root) {
                        // The symlink points within the source tree, transform it
                        return Some(dest_root.join(relative_to_source));
                    }
                } else {
                    // If canonicalization fails, try direct prefix matching
                    if let Ok(relative_to_source) = target.strip_prefix(source_root) {
                        return Some(dest_root.join(relative_to_source));
                    }
                }
                // If it doesn't point within source tree, keep it as-is
                Some(target.clone())
            } else {
                // Relative symlinks are preserved as-is
                Some(target.clone())
            }
        } else {
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if !args.source.exists() {
        anyhow::bail!("Source path does not exist: {}", args.source.display());
    }

    let log_level = if args.verbose {
        Level::INFO
    } else if args.debug {
        Level::DEBUG
    } else {
        Level::WARN
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting pcopy with parallelism: {}", args.parallelism);
    info!("Source: {}", args.source.display());
    info!("Destination: {}", args.destination.display());

    let (discovery_tx, discovery_rx) = unbounded::<FileTask>();
    let (comparison_tx, comparison_rx) = unbounded::<FileTask>();
    let (copy_tx, copy_rx) = unbounded::<FileTask>();

    let source = args.source.clone();
    let destination = args.destination.clone();
    let parallelism = args.parallelism;
    let dry_run = args.dry_run;
    let show_progress = args.progress;

    let progress_counter = if show_progress {
        Some(ProgressCounter::new())
    } else {
        None
    };

    let discovery_handle = {
        let progress = progress_counter.clone();
        tokio::spawn(
            async move { discover_files(source, destination, discovery_tx, progress).await },
        )
    };

    let comparison_handle = {
        let progress = progress_counter.clone();
        tokio::spawn(
            async move { populate_dest_metadata(discovery_rx, comparison_tx, progress).await },
        )
    };

    let copy_handles: Vec<_> = (0..parallelism)
        .map(|_| {
            (
                comparison_rx.clone(),
                copy_tx.clone(),
                args.source.clone(),
                args.destination.clone(),
                progress_counter.clone(),
            )
        })
        .map(|(comp, copy, src, dest, progress)| {
            tokio::spawn(async move { copy_files(comp, copy, dry_run, src, dest, progress).await })
        })
        .collect();

    let md_handles: Vec<_> = (0..parallelism)
        .map(|_| copy_rx.clone())
        .map(|c_rx| tokio::spawn(async move { adjust_metadata(c_rx, dry_run).await }))
        .collect();

    //let metadata_handle = tokio::spawn(async move { adjust_metadata(copy_rx, dry_run).await });

    // Start progress display if requested
    let progress_handle = if let Some(progress) = progress_counter.clone() {
        Some(tokio::spawn(async move {
            progress_display_task(progress).await
        }))
    } else {
        None
    };

    info!("Waiting on file discovery");
    discovery_handle.await??;
    info!("Waiting on comparison");
    comparison_handle.await??;
    info!("Waiting on copier");
    for ch in copy_handles {
        ch.await??;
    }
    copy_tx.close();
    //copy_handle.await??;
    info!("Waiting on metadata updater");
    //metadata_handle.await??;
    for ch in md_handles {
        ch.await??;
    }
    // md_handles.iter().map(|ch| ch.await??);

    // Clean up progress display
    if let Some(handle) = progress_handle {
        handle.abort();
        if show_progress {
            // Print final newline to clear progress line
            println!();
        }
    }

    info!("pcopy completed successfully");
    Ok(())
}

async fn discover_files(
    source: PathBuf,
    destination: PathBuf,
    tx: async_channel::Sender<FileTask>,
    progress: Option<ProgressCounter>,
) -> Result<()> {
    /*
    if let Some(parent) = destination.parent() {
        async_fs::create_dir_all(parent).await?;
    }
    */

    info!("Starting file discovery");
    let mut dirs = vec![source.clone()];
    while let Some(current) = dirs.pop() {
        let mut entries = async_fs::read_dir(current).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;

            let relative_path = path.strip_prefix(&source)?;
            let dest_path = destination.join(relative_path);

            if metadata.is_dir() {
                dirs.push(path.clone());
            }

            // Check if this is a symlink and read its target
            let symlink_target = if metadata.is_symlink() {
                std::fs::read_link(&path).ok()
            } else {
                None
            };

            if let Some(ref progress) = progress {
                progress.increment_discovered();
            }

            if tx
                .try_send(FileTask {
                    source_path: path.clone(),
                    dest_path: dest_path.clone(),
                    source_metadata: metadata,
                    dest_metadata: None,
                    symlink_target,
                })
                .is_err()
            {
                break;
            }
        }
    }
    info!("File discovery completed");
    Ok(())
}

async fn populate_dest_metadata(
    rx: async_channel::Receiver<FileTask>,
    tx: async_channel::Sender<FileTask>,
    progress: Option<ProgressCounter>,
) -> Result<()> {
    while let Ok(mut task) = rx.recv().await {
        if let Ok(dest_metadata) = async_fs::metadata(&task.dest_path).await {
            task.dest_metadata = Some(dest_metadata);
        }

        // Check if this task needs copying and update progress
        if task.needs_copy() {
            if let Some(ref progress) = progress {
                progress.increment_flagged();
            }
        }

        if tx.send(task).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn copy_files(
    rx: async_channel::Receiver<FileTask>,
    tx: async_channel::Sender<FileTask>,
    dry_run: bool,
    source_root: PathBuf,
    dest_root: PathBuf,
    progress: Option<ProgressCounter>,
) -> Result<()> {
    use std::fs;

    while let Ok(mut task) = rx.recv().await {
        if task.source_metadata.is_dir() {
            if !dry_run {
                async_fs::create_dir_all(&task.dest_path).await?;
            }
            // Count directory creation as copying
            if let Some(ref progress) = progress {
                progress.increment_copied(0);
            }
        } else if task.source_metadata.is_symlink() {
            // Handle symlinks
            if let Some(parent) = task.dest_path.parent() {
                if !dry_run {
                    async_fs::create_dir_all(parent).await?;
                }
            }

            if !dry_run {
                // Remove existing symlink if it exists
                if task.dest_path.exists() || task.dest_path.is_symlink() {
                    fs::remove_file(&task.dest_path)?;
                }

                if let Some(transformed_target) =
                    task.transform_symlink_target(&source_root, &dest_root)
                {
                    info!(
                        "creating symlink {:?} -> {:?}",
                        &task.dest_path, transformed_target
                    );

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::symlink;
                        symlink(&transformed_target, &task.dest_path)?;
                    }

                    #[cfg(windows)]
                    {
                        use std::os::windows::fs::{symlink_dir, symlink_file};
                        // On Windows, we need to know if the target is a directory or file
                        // For simplicity, we'll try to determine this from the original source
                        if task.source_path.is_dir() {
                            symlink_dir(&transformed_target, &task.dest_path)?;
                        } else {
                            symlink_file(&transformed_target, &task.dest_path)?;
                        }
                    }
                }

                // Update the destination metadata
                if let Ok(dm) = fs::symlink_metadata(&task.dest_path) {
                    task.dest_metadata = Some(dm);
                }

                // Count symlink creation as copying
                if let Some(ref progress) = progress {
                    progress.increment_copied(0);
                }
            }
        } else {
            if let Some(parent) = task.dest_path.parent() {
                if !dry_run {
                    async_fs::create_dir_all(parent).await?;
                }
            }

            if !dry_run {
                if task.needs_copy() {
                    info!("copying {:?}", &task.dest_path);
                    async_fs::copy(&task.source_path, &task.dest_path).await?;

                    // Count file copying with size
                    if let Some(ref progress) = progress {
                        progress.increment_copied(task.source_metadata.len());
                    }
                }
                // Update the destination metadata
                if let Ok(dm) = fs::metadata(&task.dest_path) {
                    task.dest_metadata = Some(dm);
                }
            } else if task.needs_copy() {
                // In dry run mode, still count what would be copied
                if let Some(ref progress) = progress {
                    progress.increment_copied(task.source_metadata.len());
                }
            }
        }

        if tx.send(task).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn adjust_metadata(rx: async_channel::Receiver<FileTask>, dry_run: bool) -> Result<()> {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, chown};

    while let Ok(task) = rx.recv().await {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut changed = false;

            let sm = &task.source_metadata;
            if let Some(dm) = task.dest_metadata {
                // Set permissions if necessary
                if sm.mode() != dm.mode() {
                    let permissions = fs::Permissions::from_mode(sm.mode());
                    if !dry_run {
                        async_fs::set_permissions(&task.dest_path, permissions).await?;
                    }
                    changed = true;
                }

                // Set mtime
                if let Ok(smtime) = sm.modified() {
                    if let Ok(dmtime) = dm.modified() {
                        if smtime != dmtime {
                            let mtime = filetime::FileTime::from_system_time(smtime);
                            if !dry_run {
                                filetime::set_file_mtime(&task.dest_path, mtime)?;
                            }
                            changed = true;
                        }
                    }
                }

                // set user
                if sm.uid() != dm.uid() {
                    if !dry_run {
                        chown(&task.dest_path, Some(sm.uid()), Some(sm.gid()))?;
                    }
                    changed = true;
                }

                if changed {
                    // info!("set attrs: {:?}", &task.dest_path);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn create_test_file_task(
        source_path: PathBuf,
        dest_path: PathBuf,
        symlink_target: Option<PathBuf>,
    ) -> FileTask {
        let metadata = if source_path.is_symlink() {
            fs::symlink_metadata(&source_path).unwrap()
        } else {
            fs::metadata(&source_path).unwrap()
        };
        FileTask {
            source_path,
            dest_path,
            source_metadata: metadata,
            dest_metadata: None,
            symlink_target,
        }
    }

    #[test]
    fn test_needs_copy_new_file() {
        let temp_dir = TempDir::new().unwrap();
        let source_file = temp_dir.path().join("source.txt");
        fs::write(&source_file, "test content").unwrap();

        let task = create_test_file_task(source_file, temp_dir.path().join("dest.txt"), None);

        assert!(task.needs_copy());
    }

    #[test]
    fn test_needs_copy_same_size() {
        let temp_dir = TempDir::new().unwrap();
        let source_file = temp_dir.path().join("source.txt");
        let dest_file = temp_dir.path().join("dest.txt");

        fs::write(&source_file, "test content").unwrap();
        fs::write(&dest_file, "test content").unwrap();

        let dest_metadata = fs::metadata(&dest_file).unwrap();
        let mut task = create_test_file_task(source_file, dest_file, None);
        task.dest_metadata = Some(dest_metadata);

        assert!(!task.needs_copy());
    }

    #[test]
    fn test_needs_copy_different_size() {
        let temp_dir = TempDir::new().unwrap();
        let source_file = temp_dir.path().join("source.txt");
        let dest_file = temp_dir.path().join("dest.txt");

        fs::write(&source_file, "test content longer").unwrap();
        fs::write(&dest_file, "test content").unwrap();

        let dest_metadata = fs::metadata(&dest_file).unwrap();
        let mut task = create_test_file_task(source_file, dest_file, None);
        task.dest_metadata = Some(dest_metadata);

        assert!(task.needs_copy());
    }

    #[test]
    fn test_symlink_transform_relative_unchanged() {
        let temp_dir = TempDir::new().unwrap();
        let source_file = temp_dir.path().join("source.txt");
        let target_file = temp_dir.path().join("target.txt");

        fs::write(&target_file, "target content").unwrap();
        symlink("./target.txt", &source_file).unwrap();

        let task = create_test_file_task(
            source_file,
            temp_dir.path().join("dest.txt"),
            Some(PathBuf::from("./target.txt")),
        );

        let source_root = temp_dir.path().join("source");
        let dest_root = temp_dir.path().join("dest");

        let transformed = task.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(transformed, Some(PathBuf::from("./target.txt")));
    }

    #[test]
    fn test_symlink_transform_absolute_within_source() {
        let temp_dir = TempDir::new().unwrap();
        let source_root = temp_dir.path().join("source");
        let dest_root = temp_dir.path().join("dest");

        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&dest_root).unwrap();

        let source_root = source_root.canonicalize().unwrap();

        let target_file = source_root.join("subdir").join("target.txt");
        fs::create_dir_all(target_file.parent().unwrap()).unwrap();
        fs::write(&target_file, "target content").unwrap();

        let source_file = source_root.join("source.txt");
        symlink(&target_file, &source_file).unwrap();

        let task = create_test_file_task(
            source_file,
            dest_root.join("dest.txt"),
            Some(target_file.clone()),
        );

        let transformed = task.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(
            transformed,
            Some(dest_root.join("subdir").join("target.txt"))
        );
    }

    #[test]
    fn test_symlink_transform_absolute_outside_source() {
        let temp_dir = TempDir::new().unwrap();
        let source_root = temp_dir.path().join("source");
        let dest_root = temp_dir.path().join("dest");

        fs::create_dir_all(&source_root).unwrap();

        let external_target = PathBuf::from("/etc/passwd");
        let source_file = source_root.join("source.txt");

        // Create a dummy file since we can't actually symlink to /etc/passwd in tests
        fs::write(&source_file, "dummy").unwrap();

        let task = create_test_file_task(
            source_file,
            dest_root.join("dest.txt"),
            Some(external_target.clone()),
        );

        let transformed = task.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(transformed, Some(external_target));
    }

    #[test]
    fn test_symlink_transform_none_target() {
        let temp_dir = TempDir::new().unwrap();
        let source_file = temp_dir.path().join("source.txt");
        fs::write(&source_file, "regular file").unwrap();

        let task = create_test_file_task(source_file, temp_dir.path().join("dest.txt"), None);

        let source_root = temp_dir.path().join("source");
        let dest_root = temp_dir.path().join("dest");

        let transformed = task.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(transformed, None);
    }

    #[tokio::test]
    async fn test_discover_files_with_symlinks() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");

        fs::create_dir_all(&source_dir).unwrap();

        // Create a regular file
        let regular_file = source_dir.join("regular.txt");
        fs::write(&regular_file, "regular content").unwrap();

        // Create a relative symlink
        let rel_symlink = source_dir.join("rel_link.txt");
        symlink("./regular.txt", &rel_symlink).unwrap();

        // Create a target file for absolute symlink
        let abs_target = source_dir.join("abs_target.txt");
        fs::write(&abs_target, "abs target content").unwrap();

        // Create absolute symlink
        let abs_symlink = source_dir.join("abs_link.txt");
        symlink(&abs_target, &abs_symlink).unwrap();

        let (tx, rx) = unbounded::<FileTask>();

        let discover_result = discover_files(source_dir, dest_dir, tx, None).await;
        assert!(discover_result.is_ok());

        let mut tasks = Vec::new();
        while let Ok(task) = rx.try_recv() {
            tasks.push(task);
        }

        // Should have 4 tasks: regular file, rel symlink, abs target, abs symlink
        assert_eq!(tasks.len(), 4);

        // Check that symlinks have their targets recorded
        let symlink_tasks: Vec<_> = tasks
            .iter()
            .filter(|t| t.symlink_target.is_some())
            .collect();
        assert_eq!(symlink_tasks.len(), 2);

        // Check relative symlink
        let rel_task = tasks
            .iter()
            .find(|t| t.source_path.file_name().unwrap() == "rel_link.txt")
            .unwrap();
        assert_eq!(
            rel_task.symlink_target,
            Some(PathBuf::from("./regular.txt"))
        );

        // Check absolute symlink
        let abs_task = tasks
            .iter()
            .find(|t| t.source_path.file_name().unwrap() == "abs_link.txt")
            .unwrap();
        assert!(abs_task.symlink_target.is_some());
        assert!(abs_task.symlink_target.as_ref().unwrap().is_absolute());
    }

    #[tokio::test]
    async fn test_copy_files_with_symlinks() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");

        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&dest_dir).unwrap();

        // Create target file
        let target_file = source_dir.join("target.txt");
        fs::write(&target_file, "target content").unwrap();

        // Create symlink
        let symlink_file = source_dir.join("link.txt");
        symlink("./target.txt", &symlink_file).unwrap();

        // Create FileTask for the symlink
        let symlink_metadata = fs::symlink_metadata(&symlink_file).unwrap();
        let task = FileTask {
            source_path: symlink_file,
            dest_path: dest_dir.join("link.txt"),
            source_metadata: symlink_metadata,
            dest_metadata: None,
            symlink_target: Some(PathBuf::from("./target.txt")),
        };

        let (task_tx, task_rx) = unbounded::<FileTask>();
        let (copy_tx, copy_rx) = unbounded::<FileTask>();

        task_tx.send(task).await.unwrap();
        task_tx.close();

        let copy_result =
            copy_files(task_rx, copy_tx, false, source_dir, dest_dir.clone(), None).await;
        assert!(copy_result.is_ok());

        // Verify symlink was created
        let dest_link = dest_dir.join("link.txt");
        assert!(dest_link.is_symlink());

        // Verify symlink target is correct
        let link_target = fs::read_link(&dest_link).unwrap();
        assert_eq!(link_target, PathBuf::from("./target.txt"));

        // Verify a task was sent to the next stage
        let copied_task = copy_rx.try_recv().unwrap();
        assert!(copied_task.dest_metadata.is_some());
    }

    #[tokio::test]
    async fn test_copy_files_dry_run_symlinks() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");

        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&dest_dir).unwrap();

        // Create symlink
        let symlink_file = source_dir.join("link.txt");
        symlink("./target.txt", &symlink_file).unwrap();

        let symlink_metadata = fs::symlink_metadata(&symlink_file).unwrap();
        let task = FileTask {
            source_path: symlink_file,
            dest_path: dest_dir.join("link.txt"),
            source_metadata: symlink_metadata,
            dest_metadata: None,
            symlink_target: Some(PathBuf::from("./target.txt")),
        };

        let (task_tx, task_rx) = unbounded::<FileTask>();
        let (copy_tx, copy_rx) = unbounded::<FileTask>();

        task_tx.send(task).await.unwrap();
        task_tx.close();

        let copy_result =
            copy_files(task_rx, copy_tx, true, source_dir, dest_dir.clone(), None).await;
        assert!(copy_result.is_ok());

        // Verify symlink was NOT created in dry run
        let dest_link = dest_dir.join("link.txt");
        assert!(!dest_link.exists());

        // Verify task was still sent to next stage
        let copied_task = copy_rx.try_recv().unwrap();
        assert!(copied_task.dest_metadata.is_none()); // No metadata in dry run
    }

    #[test]
    fn test_symlink_edge_cases() {
        let temp_dir = TempDir::new().unwrap();
        let source_root = temp_dir.path().join("source");
        let dest_root = temp_dir.path().join("dest");

        // Test with empty symlink target
        let source_file = temp_dir.path().join("source.txt");
        fs::write(&source_file, "dummy").unwrap();

        let task = create_test_file_task(
            source_file,
            temp_dir.path().join("dest.txt"),
            Some(PathBuf::from("")),
        );

        let transformed = task.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(transformed, Some(PathBuf::from("")));

        // Test with complex relative path
        let complex_rel = PathBuf::from("../../../some/deep/path");
        let source_file2 = temp_dir.path().join("source2.txt");
        fs::write(&source_file2, "dummy").unwrap();

        let task2 = create_test_file_task(
            source_file2,
            temp_dir.path().join("dest2.txt"),
            Some(complex_rel.clone()),
        );

        let transformed2 = task2.transform_symlink_target(&source_root, &dest_root);
        assert_eq!(transformed2, Some(complex_rel));
    }

    #[test]
    fn test_progress_counter() {
        let progress = ProgressCounter::new();

        // Test initial state
        let (discovered, flagged, copied, bytes, _speed) = progress.get_stats();
        assert_eq!(discovered, 0);
        assert_eq!(flagged, 0);
        assert_eq!(copied, 0);
        assert_eq!(bytes, 0);

        // Test increments
        progress.increment_discovered();
        progress.increment_discovered();
        progress.increment_flagged();
        progress.increment_copied(100);
        progress.increment_copied(200);

        let (discovered, flagged, copied, bytes, _speed) = progress.get_stats();
        assert_eq!(discovered, 2);
        assert_eq!(flagged, 1);
        assert_eq!(copied, 2);
        assert_eq!(bytes, 300);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 1024), "1.0 TB");
    }

    #[tokio::test]
    async fn test_progress_integration() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");

        fs::create_dir_all(&source_dir).unwrap();

        // Create test files
        let file1 = source_dir.join("file1.txt");
        let file2 = source_dir.join("file2.txt");
        fs::write(&file1, "content1").unwrap();
        fs::write(&file2, "content2").unwrap();

        let progress = ProgressCounter::new();
        let (tx, rx) = unbounded::<FileTask>();

        // Test discovery with progress
        let discover_result =
            discover_files(source_dir, dest_dir, tx, Some(progress.clone())).await;
        assert!(discover_result.is_ok());

        // Check that files were discovered
        let (discovered, _flagged, _copied, _bytes, _speed) = progress.get_stats();
        assert_eq!(discovered, 2);

        // Test flagging with progress
        let (comp_tx, _comp_rx) = unbounded::<FileTask>();
        let populate_result = populate_dest_metadata(rx, comp_tx, Some(progress.clone())).await;
        assert!(populate_result.is_ok());

        // Check that files were flagged for copying (since dest doesn't exist)
        let (_discovered, flagged, _copied, _bytes, _speed) = progress.get_stats();
        assert_eq!(flagged, 2);
    }
}
