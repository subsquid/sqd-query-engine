.PHONY: spec-check spec-check-strict spec-test test test-data fmt lint

# MG-6 — the specification's own integrity gate. Runs in well under a second.
# CI runs exactly this; see .github/workflows/spec.yml.
spec-check:
	python3 spec/check.py spec

# The checker's own tests. Every rule gets a mutation that must trip it, because
# a check that silently does nothing and a check that passes print the same thing.
spec-test:
	python3 spec/test_check.py

# What is *reported* and what *fails* are separate thresholds: print everything
# down to notices, fail on anything at warning or worse. Useful before touching
# the spec; not what CI blocks on.
spec-check-strict:
	python3 spec/check.py spec --severity info --gate warning

# MG-3's portable suite. Tests that require external data are explicitly ignored,
# so their absence is visible in the test summary rather than reported as a pass.
test:
	cargo test

# The data-backed suite. The guards turn missing inputs into failures, and
# `--ignored` selects the tests omitted from the portable gate.
test-data:
	SQD_REQUIRE_CHUNKS=1 SQD_REQUIRE_FIXTURES=1 cargo test -- --ignored

# MG-8, over the engine and its tests. `benches/` and `examples/` are outside
# both static gates: they are not the engine, and bringing them under the
# formatter is a change of its own.
# rustfmt follows `mod` declarations, so the library root covers all of `src/`.
# Integration tests are separate crates, so each is named. Expanded by `make`
# rather than by `git ls-files`: a gate that quietly checks fewer files where git
# is unavailable is worse than one that fails.
ENGINE_SOURCES = src/lib.rs src/bin/generate_fixtures.rs $(wildcard tests/*.rs)

fmt:
	rustfmt --check --edition 2021 $(ENGINE_SOURCES)

lint:
	cargo clippy --lib --tests -- -D warnings
