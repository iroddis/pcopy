# pcopy

pcopy is a rust async project that mimics the function of `rsync -ar` for local copies
only. It's biggest feature is parallelized file discovery, comparisons, copying, and
attribute adjustment.

An example usage is:

```bash
# Copy files using 4 readers, 4 comparitors, 4 copiers, and 4 attribute adjustments.
pcopy -p 4 /path/to/src /path/to/dest
```

# implementation

pcopy uses tokio and mpsc channels to:

1. discover files and directories in the source
2. run comparisons in the destination
3. copy files
4. adjust metadata (mtime, owner, group, and permissions)
