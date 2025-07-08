# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pcopy is a Rust async project that mimics the function of `rsync -ar` for local copies only. Its biggest feature is parallelized file discovery, comparisons, copying, and attribute adjustment.

## Core Architecture

The project uses tokio and mpsc channels to create a parallel pipeline:

1. **File Discovery**: Scan source directory structure
2. **Comparison**: Compare files between source and destination
3. **Copying**: Copy files that need updating
4. **Metadata Adjustment**: Set mtime, owner, group, and permissions

The parallelization is controlled by the `-p` parameter which sets the number of concurrent workers for each stage.

## Development Commands

### Build and Run
```bash
cargo build
cargo run -- -p 4 /path/to/src /path/to/dest
```

### Testing
```bash
cargo test
```

### Linting and Formatting
```bash
cargo clippy
cargo fmt
```

## Current State

The project is in early development with only a basic "Hello, world!" main function implemented. The architecture described in the README needs to be built from scratch.

## Key Implementation Notes

- Use tokio for async runtime
- Implement mpsc channels for inter-stage communication
- Support parallelization parameter (-p) for controlling worker count
- Handle file metadata (mtime, permissions, ownership)
- Focus on local file operations only (no network functionality)