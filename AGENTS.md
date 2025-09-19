# Repository Guidelines

## Project Structure & Module Organization
`src/main.rs` orchestrates the CLI, argument parsing, and rendering pipeline. Adjacent modules contain focused concerns: `args` for Clap inputs, `slurp` for the streaming pass, `prefetch_reader` and `parser/` for low-level hprof decoding, and `rendered_result` plus `result_recorder` for output serialization. Shared helpers live in `utils.rs`, with error types in `errors.rs`. Sample dumps for local runs live in `test-heap-dumps/`; keep new fixtures small and document them in the directory README when added.

## Build, Test, and Development Commands
- `cargo fmt --all` — enforce formatting exactly as CI expects.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — run the lint gate; fix or silence before committing.
- `cargo build --release` — produce an optimized binary, the right target for profiling or packaging.
- `cargo run -- --inputFile test-heap-dumps/hprof-64.bin` — smoke-test the CLI with bundled data; add `--json` when validating serializers.
- `cargo test --all -- --nocapture` — execute unit tests; add `--test-threads=1` if order matters.

## Coding Style & Naming Conventions
Rustfmt manages layout (4-space indents, 100-column width); never hand-tune formatting. Use idiomatic Rust naming: modules and functions in `snake_case`, types in `PascalCase`, constants in `SCREAMING_SNAKE_CASE`. Keep modules aligned with runtime responsibilities (`parser/record_parser.rs`, etc.) so the streaming flow remains traceable. Favor doc comments on public APIs and return structured errors through `HprofSlurpError` instead of `panic!`.

## Testing Guidelines
Place unit tests beside the code under `#[cfg(test)]` blocks (`src/parser/file_header_parser.rs`, `src/slurp.rs`). Name them after observable behavior (e.g., `should_parse_utf8_record`). When binary fixtures are required, store them in `test-heap-dumps/` and refer to them via relative paths so `cargo test` stays hermetic. Extend coverage around parsing edge cases and JSON rendering, and note any known gaps at the top of the test module.

## Commit & Pull Request Guidelines
Write imperative, present-tense commits (`Add record stream guard`) and keep scopes narrow; squash noisy fixups before review. Reference issues or discussions in the commit body when relevant, and call out new CLI flags, metrics, or JSON fields. PR descriptions should cover motivation, testing performed (`cargo test`, manual runs), and include screenshots or JSON snippets when output changes. Confirm the CI command set passes locally ahead of review to keep the pipeline green.

## Operational Notes
Heap dumps can contain sensitive identifiers; never commit real production data. For multi-gigabyte reproductions, trim or anonymize dumps and explain the process in the PR description.
