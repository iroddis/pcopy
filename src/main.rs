use anyhow::Result;
use async_channel::unbounded;
use clap::Parser;
use std::path::PathBuf;
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
}

#[derive(Debug, Clone)]
struct FileTask {
    source_path: PathBuf,
    dest_path: PathBuf,
    metadata: std::fs::Metadata,
}

#[derive(Debug, Clone)]
struct CopyTask {
    source_path: PathBuf,
    dest_path: PathBuf,
    metadata: std::fs::Metadata,
}

#[derive(Debug, Clone)]
struct MetadataTask {
    dest_path: PathBuf,
    metadata: std::fs::Metadata,
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
    let (comparison_tx, comparison_rx) = unbounded::<CopyTask>();
    let (copy_tx, copy_rx) = unbounded::<MetadataTask>();

    let source = args.source.clone();
    let destination = args.destination.clone();
    let parallelism = args.parallelism;

    let discovery_handle =
        tokio::spawn(async move { discover_files(source, destination, discovery_tx).await });

    let comparison_handle =
        tokio::spawn(async move { compare_files(discovery_rx, comparison_tx).await });

    let copy_handles: Vec<_> = (0..parallelism)
        .map(|_| (comparison_rx.clone(), copy_tx.clone()))
        .map(|(comp, copy)| tokio::spawn(async move { copy_files(comp, copy).await }))
        .collect();

    let metadata_handle = tokio::spawn(async move { adjust_metadata(copy_rx).await });

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
    use std::fs;
    use tokio::fs as async_fs;

    fn walk_dir(
        source: &PathBuf,
        destination: &PathBuf,
        current: &PathBuf,
        tx: &async_channel::Sender<FileTask>,
    ) -> Result<()> {
        let entries = fs::read_dir(current)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;

            let relative_path = path.strip_prefix(source)?;
            let dest_path = destination.join(relative_path);

            if metadata.is_dir() {
                if tx
                    .try_send(FileTask {
                        source_path: path.clone(),
                        dest_path: dest_path.clone(),
                        metadata,
                    })
                    .is_err()
                {
                    break;
                }
                walk_dir(source, destination, &path, tx)?;
            } else if tx
                .try_send(FileTask {
                    source_path: path,
                    dest_path,
                    metadata,
                })
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }

    if let Some(parent) = destination.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    info!("Starting file discovery");
    walk_dir(&source, &destination, &source, &tx)?;
    info!("File discovery completed");
    Ok(())
}

async fn compare_files(
    rx: async_channel::Receiver<FileTask>,
    tx: async_channel::Sender<CopyTask>,
) -> Result<()> {
    use std::fs;
    use tokio::fs as async_fs;

    while let Ok(task) = rx.recv().await {
        if task.metadata.is_dir() {
            async_fs::create_dir_all(&task.dest_path).await?;

            if tx
                .send(CopyTask {
                    source_path: task.source_path,
                    dest_path: task.dest_path,
                    metadata: task.metadata,
                })
                .await
                .is_err()
            {
                break;
            }
        } else {
            let needs_copy = if let Ok(dest_metadata) = fs::metadata(&task.dest_path) {
                task.metadata.len() != dest_metadata.len()
                    || task.metadata.modified()? > dest_metadata.modified()?
            } else {
                true
            };

            if needs_copy
                && tx
                    .send(CopyTask {
                        source_path: task.source_path,
                        dest_path: task.dest_path,
                        metadata: task.metadata,
                    })
                    .await
                    .is_err()
            {
                break;
            }
        }
    }
    Ok(())
}

async fn copy_files(
    rx: async_channel::Receiver<CopyTask>,
    tx: async_channel::Sender<MetadataTask>,
) -> Result<()> {
    use tokio::fs as async_fs;

    while let Ok(task) = rx.recv().await {
        if !task.metadata.is_dir() {
            if let Some(parent) = task.dest_path.parent() {
                async_fs::create_dir_all(parent).await?;
            }

            async_fs::copy(&task.source_path, &task.dest_path).await?;
        }

        if tx
            .send(MetadataTask {
                dest_path: task.dest_path,
                metadata: task.metadata,
            })
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn adjust_metadata(rx: async_channel::Receiver<MetadataTask>) -> Result<()> {
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    while let Ok(task) = rx.recv().await {
        if let Ok(_dest_metadata) = fs::metadata(&task.dest_path) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                let permissions = fs::Permissions::from_mode(task.metadata.mode());
                fs::set_permissions(&task.dest_path, permissions)?;

                if let Ok(mtime) = task.metadata.modified() {
                    if let Ok(atime) = task.metadata.accessed() {
                        let mtime = filetime::FileTime::from_system_time(mtime);
                        let atime = filetime::FileTime::from_system_time(atime);
                        filetime::set_file_times(&task.dest_path, atime, mtime)?;
                    }
                }
            }
        }
    }
    Ok(())
}
