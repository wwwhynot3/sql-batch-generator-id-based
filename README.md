# SQL Batch Slicer (ID-Based)

A lightweight CLI that slices one SQL statement into multiple ID-range batches.

It is designed for safer large-scale UPDATE/DELETE/SELECT operations by generating one statement per ID range and writing all batches to a file.

## Features

- SQL AST parsing with `sqlparser` (no regex-based SQL rewriting).
- Supports `SELECT`, `UPDATE`, and `DELETE` statements.
- Appends `BETWEEN start_id AND end_id` condition into existing `WHERE` or creates one when missing.
- Custom primary key input (default `id`).
- If primary key is unqualified (for example `id`), it is prefixed with the main table alias when an alias exists (for example `u.id`).
- If primary key is already qualified (for example `users.id`), it is kept as-is.
- Supports selectable SQL dialects: `generic`, `mysql`, `postgres`, `sqlite`, `mssql`, `snowflake`, `duckdb`.
- Supports both interactive mode and CLI argument mode.

## Quick Start

### Prerequisites

- Rust + Cargo installed.

### Run (interactive mode)

```bash
cargo run --release
```

When no arguments are provided, the tool enters interactive mode and asks for:

- Start ID / End ID
- Batch size
- Primary key
- SQL dialect
- SQL source (editor or file)
- Output path

### Run (argument mode)

```bash
cargo run --release -- \
	--start-id 1 \
	--end-id 100000 \
	--batch-size 50000 \
	--primary-key id \
	--dialect postgres \
	--sql-file ./input.sql \
	--output id_slice.sql
```

You must provide one of:

- `--sql "..."`
- `--sql-file <path>`

## CLI Arguments

- `-s, --start-id <i128>`: Start ID (required in argument mode)
- `-e, --end-id <i128>`: End ID (required in argument mode)
- `-b, --batch-size <usize>`: Batch size (default `50000`)
- `-q, --sql <string>`: Raw SQL text
- `-f, --sql-file <path>`: SQL file path
- `-o, --output <path>`: Output file (default `id_slice.sql`)
- `-k, --primary-key <string>`: Primary key column (default `id`)
- `-d, --dialect <dialect>`: SQL dialect (default `generic`)

## Example

Input SQL:

```sql
UPDATE users u SET active = 0 WHERE status = 'old';
```

Parameters:

- `start_id = 1`
- `end_id = 100`
- `batch_size = 50`
- `primary_key = id`

Generated output:

```sql
UPDATE users AS u SET active = 0 WHERE u.id BETWEEN 1 AND 50 AND (status = 'old');
UPDATE users AS u SET active = 0 WHERE u.id BETWEEN 51 AND 100 AND (status = 'old');
```

## Validation

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

## GitHub Actions (CI/CD)

This project includes automation under `.github/workflows`:

- `ci.yml`: Runs format check, clippy, tests, and release-target build checks (including Linux ARM64 cross-build) on push/PR.
- `release.yml`: Builds release binaries and packages for:
	- Windows amd64 (`x86_64-pc-windows-msvc`)
	- macOS x64 (`x86_64-apple-darwin`)
	- macOS arm64 (`aarch64-apple-darwin`)
	- Linux amd64 (`x86_64-unknown-linux-gnu`)
	- Linux arm64 (`aarch64-unknown-linux-gnu`)
	Then publishes GitHub Release assets and a `SHA256SUMS.txt` checksum file.
- `security.yml`: Runs `cargo audit` on push/PR + weekly schedule, and dependency review on pull requests.

### How to create a release (for beginners)

1. Update version in `Cargo.toml` if needed.
2. Commit and push your changes.
3. Create and push a version tag (`v*` or `release*`):

```bash
git tag v0.1.0
git push origin v0.1.0
```

After the tag is pushed, GitHub Actions will automatically:

- build binaries,
- package artifacts,
- create a GitHub Release,
- upload packaged files to that release.

### Optional setup

- Enable **Dependabot** in repository settings (config already provided in `.github/dependabot.yml`) to keep Cargo/GitHub Actions dependencies updated automatically.

## License

MIT
