# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs`: CLI entrypoint and core slicing logic (`IdSlicer`, SQL template parsing, file output).
- `Cargo.toml` / `Cargo.lock`: Rust package metadata and dependency lockfile.
- `id_slice.sql`: Generated output file for batched SQL statements.
- `target/`: Build artifacts (do not edit or commit manually).
- `README.md`: User-facing overview and usage examples.

Keep new logic in small functions or structs near related code. If code grows, split reusable logic into modules under `src/` (for example, `src/parser.rs` and `src/slicer.rs`).

## Build, Test, and Development Commands
- `cargo run --release`: Run the interactive slicer with optimized performance.
- `cargo build --release`: Build a release binary without running it.
- `cargo test`: Run unit/integration tests.
- `cargo fmt --all`: Format code using Rust conventions.
- `cargo clippy --all-targets -- -D warnings`: Lint strictly and fail on warnings.

For quick manual verification, run `cargo run --release`, input sample SQL, then inspect `id_slice.sql`.

## Coding Style & Naming Conventions
- Follow standard Rust formatting (`rustfmt`) and lint guidance (`clippy`).
- Use `snake_case` for functions/variables and `CamelCase` for structs/enums.
- Prefer descriptive names (`batch_size`, `start_id`) over abbreviations.
- Keep error handling explicit with `anyhow::Result` and contextual messages (`.context(...)`).
- Keep PRs focused; avoid unrelated refactors.

## Testing Guidelines
- Place unit tests in `src/main.rs` under `#[cfg(test)]`, or move logic to modules and test there.
- Add integration tests in `tests/` when behavior spans multiple components.
- Cover edge cases: `start_id > end_id`, `batch_size == 0`, SQL with/without `WHERE`, and trailing `ORDER BY`/`LIMIT`.

## Commit & Pull Request Guidelines
- Existing commits are short, imperative, and descriptive (for example, `Initialize README with project description and usage`).
- Recommended format: `<scope>: <what changed>` (example: `parser: handle LIMIT stripping`).
- PRs should include a summary, rationale, verification steps, and sample input/output when SQL generation changes.
- Link related issues and keep each PR scoped to one concern.

## Security & Configuration Tips
- Review generated SQL before executing in production.
- The ID column is currently hardcoded as `id`; adjust logic carefully if your schema differs.
