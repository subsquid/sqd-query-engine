.PHONY: spec-check spec-check-strict spec-test test fmt lint

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

test:
	cargo test

fmt:
	cargo fmt --check

lint:
	cargo clippy --all-targets -- -D warnings
