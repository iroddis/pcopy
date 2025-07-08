use anyhow::Result;
use async_channel::unbounded;
use clap::Parser;
use std::path::PathBuf;
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
        .map(|_| (comparison_rx.clone(), copy_tx.clone()))
        .map(|(comp, copy)| tokio::spawn(async move { copy_files(comp, copy, dry_run).await }))
        .collect();

    let metadata_handle = tokio::spawn(async move { adjust_metadata(copy_rx, dry_run).await });

    discovery_handle.await??;
    comparison_handle.await??;
    for ch in copy_handles {
        ch.await??;
    }
    //copy_handle.await??;
    metadata_handle.await??;

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
            if tx
                .try_send(FileTask {
                    source_path: path.clone(),
                    dest_path: dest_path.clone(),
                    source_metadata: metadata,
                    dest_metadata: None,
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
) -> Result<()> {
    use std::fs;

    while let Ok(mut task) = rx.recv().await {
        if task.source_metadata.is_dir() {
            if !dry_run {
                async_fs::create_dir_all(&task.dest_path).await?;
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
                task.dest_metadata = Some(fs::metadata(&task.dest_path).unwrap());
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
        if dry_run {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut changed = false;

            let sm = &task.source_metadata;
            let dm = task.dest_metadata.as_ref().unwrap();

            // Set permissions if necessary
            if sm.mode() != dm.mode() {
                let permissions = fs::Permissions::from_mode(sm.mode());
                async_fs::set_permissions(&task.dest_path, permissions).await?;
                changed = true;
            }

            // Set mtime
            if let Ok(smtime) = sm.modified() {
                if let Ok(dmtime) = dm.modified() {
                    if smtime != dmtime {
                        let mtime = filetime::FileTime::from_system_time(smtime);
                        filetime::set_file_mtime(&task.dest_path, mtime)?;
                        changed = true;
                    }
                }
            }

            // set user
            if sm.uid() != dm.uid() {
                chown(&task.dest_path, Some(sm.uid()), Some(sm.gid()))?;
                changed = true;
            }

            if changed {
                info!("set attrs: {:?}", &task.dest_path);
            }
        }
    }
    Ok(())
}
