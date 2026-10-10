.PHONY: spec-check spec-check-strict spec-test test test-pr test-nightly test-data \
        fetch-cases test-cases fmt lint

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
# A superset of `test-pr`: CT-9 costs a tenth of a second, so there is nothing to
# gain by holding it back, and running it here is worth more than the tidiness of
# the split.
test:
	cargo test --workspace

# MG-3 and MG-4 own disjoint sets of classes, so each gate needs a command that
# selects its own. The class prefix is the module name, which is why every class
# must have at least one test in its own directory to be selectable at all.
NIGHTLY_CLASSES = ct7_ ct8_ ct9_

test-pr:
	cargo test --workspace --lib
	cargo test --test conformance -- $(addprefix --skip ,$(NIGHTLY_CLASSES))

# MG-4's classes. CT-7 needs the reference engine and CT-8 external chunks, so
# this needs both inputs; no job supplies them yet, which is why MG-4 is advisory.
test-nightly:
	SQD_REQUIRE_CHUNKS=1 SQD_REQUIRE_FIXTURES=1 cargo test --features legacy-query \
	  --test conformance -- $(NIGHTLY_CLASSES) --include-ignored

# The data-backed suite. The guards turn missing inputs into failures, and
# `--ignored` selects the tests omitted from the portable gate.
test-data:
	SQD_REQUIRE_CHUNKS=1 SQD_REQUIRE_FIXTURES=1 cargo test --workspace -- --ignored

# Real-chunk cases, see tests/cases/README.md. The chunks live in a cache outside
# the repo, and the test reads the same variable.
export SQD_CHUNK_CACHE ?= $(HOME)/.cache/sqd-chunks

# Downloads each chunk a case names and the cache lacks. The copy lands in a
# `.part` directory first, so an interrupted one never looks complete. The AWS
# CLI must be configured for the chunk store.
fetch-cases:
	@for ref in $$(find tests/cases -name case.yaml \
	    -exec sed -n 's|^chunk: *s3://\([^ ]*\).*|\1|p' {} + | sort -u); do \
	  dest="$(SQD_CHUNK_CACHE)/$$ref"; \
	  test -d "$$dest" && continue; \
	  echo "fetching s3://$$ref"; \
	  aws s3 cp --recursive --only-show-errors "s3://$$ref/" "$$dest.part/" || exit 1; \
	  test -d "$$dest.part" || { echo "s3://$$ref holds no files"; exit 1; }; \
	  mv "$$dest.part" "$$dest" || exit 1; \
	done

# The guard turns a chunk missing from the cache into a failure, and
# `legacy-query` adds the live comparison with the reference engine.
test-cases:
	SQD_REQUIRE_CASES=1 cargo test --features legacy-query --test cases -- --ignored

# MG-8, over the engine, its crates and its tests. `benches/` and `examples/` are outside
# both static gates: they are not the engine, and bringing them under the
# formatter is a change of its own.
# rustfmt follows `mod` declarations, so the library root covers all of `src/`,
# each crate root under `crates/` covers that crate, and the conformance crate's
# root covers all of its classes. Integration tests
# are separate crates, so each root is named. Expanded by `make` rather than by
# `git ls-files`: a gate that quietly checks fewer files where git is unavailable
# is worse than one that fails.
ENGINE_SOURCES = src/lib.rs src/bin/generate_fixtures.rs \
                 $(wildcard crates/*/src/lib.rs) \
                 tests/conformance/main.rs $(wildcard tests/*.rs)

fmt:
	rustfmt --check --edition 2021 $(ENGINE_SOURCES)

lint:
	cargo clippy --workspace --lib --tests -- -D warnings
