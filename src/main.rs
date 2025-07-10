use anyhow::Result;
use async_channel::unbounded;
use clap::Parser;
use std::path::{Path, PathBuf};
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
    fn transform_symlink_target(
        &self,
        source_root: &Path,
        dest_root: &Path,
    ) -> Option<PathBuf> {
        if let Some(target) = &self.symlink_target {
            if target.is_absolute() {
                // For absolute symlinks, check if they point within the source tree
                if let Ok(canonical_target) = target.canonicalize() {
                    if let Ok(relative_to_source) = canonical_target.strip_prefix(source_root) {
                        // The symlink points within the source tree, transform it
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
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    if !args.source.exists() {
        anyhow::bail!("Source path does not exist: {}", args.source.display());
    }

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

    let discovery_handle =
        tokio::spawn(async move { discover_files(source, destination, discovery_tx).await });

    let comparison_handle =
        tokio::spawn(async move { populate_dest_metadata(discovery_rx, comparison_tx).await });

    let copy_handles: Vec<_> = (0..parallelism)
        .map(|_| {
            (
                comparison_rx.clone(),
                copy_tx.clone(),
                args.source.clone(),
                args.destination.clone(),
            )
        })
        .map(|(comp, copy, src, dest)| {
            tokio::spawn(async move { copy_files(comp, copy, dry_run, src, dest).await })
        })
        .collect();

    let md_handles: Vec<_> = (0..parallelism)
        .map(|_| copy_rx.clone())
        .map(|c_rx| tokio::spawn(async move { adjust_metadata(c_rx, dry_run).await }))
        .collect();

    //let metadata_handle = tokio::spawn(async move { adjust_metadata(copy_rx, dry_run).await });

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

    info!("pcopy completed successfully");
    Ok(())
}

async fn discover_files(
    source: PathBuf,
    destination: PathBuf,
    tx: async_channel::Sender<FileTask>,
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
) -> Result<()> {
    while let Ok(mut task) = rx.recv().await {
        if let Ok(dest_metadata) = async_fs::metadata(&task.dest_path).await {
            task.dest_metadata = Some(dest_metadata);
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
) -> Result<()> {
    use std::fs;

    while let Ok(mut task) = rx.recv().await {
        if task.source_metadata.is_dir() {
            if !dry_run {
                async_fs::create_dir_all(&task.dest_path).await?;
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
                }
                // Update the destination metadata
                if let Ok(dm) = fs::metadata(&task.dest_path) {
                    task.dest_metadata = Some(dm);
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
