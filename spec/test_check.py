#!/usr/bin/env python3
"""Self-test for `check.py`.

Every check here was, at some point, a check that silently did nothing: a
lookbehind that made its pattern unreachable, a heading whose absence returned
early, an exit code computed after the display filter. None of that is visible
from reading the output, because the output of a broken check and a satisfied
one are the same. So each rule gets a mutation that must trip it.

    python3 spec/test_check.py

Copies the suite to a temporary directory, mutates one file, and asserts on the
findings. Stdlib only; no network, no fixtures.
"""

from __future__ import annotations

import io
import json
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check  # noqa: E402

SPEC = Path(__file__).resolve().parent
CASES = []


def case(fn):
    CASES.append(fn)
    return fn


def run(root: Path, *args):
    """(exit code, findings) for one invocation against `root`."""
    buf, err = io.StringIO(), io.StringIO()
    with redirect_stdout(buf), redirect_stderr(err):
        code = check.main([str(root), "--format", "json", "--severity", "info", *args])
    text = buf.getvalue().strip()
    findings = json.loads(text) if text.startswith("[") else []
    return code, findings


def checks_in(findings):
    return {f["check"] for f in findings}


class Suite:
    """A throwaway copy of the spec, with git history, that tests may mutate."""

    def __init__(self, stack):
        self.root = Path(stack.enter_context(tempfile.TemporaryDirectory())) / "spec"
        shutil.copytree(SPEC, self.root)
        for junk in ("__pycache__", ".speccheck-ignore"):
            shutil.rmtree(self.root / junk, ignore_errors=True)
            (self.root / junk).unlink(missing_ok=True)
        repo = self.root.parent
        shutil.copy(SPEC.parent / "Cargo.toml", repo / "Cargo.toml")
        # The coverage-tag checks read the implementation tree, so the throwaway
        # copy carries one — but only the parts they open. `tests/fixtures` is an
        # externally supplied tree of some gigabytes whose form is per-machine:
        # here it happens to be a symlink, and a checkout that materialises it
        # would copy it once per case, before the gate has run at all.
        skip = shutil.ignore_patterns("fixtures")
        for tree in ("src", "tests"):
            source = SPEC.parent / tree
            if source.is_dir():
                shutil.copytree(source, repo / tree, symlinks=True, ignore=skip)
        env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
        for argv in (["init", "-q"], ["add", "-A"], ["commit", "-qm", "spec"]):
            subprocess.run(["git", *argv], cwd=repo, check=True,
                           capture_output=True, env={**env, "PATH": "/usr/bin:/bin"})

    def read(self, name):
        return (self.root / name).read_text(encoding="utf-8")

    def write(self, name, text):
        (self.root / name).write_text(text, encoding="utf-8")

    def edit(self, name, old, new, count=1):
        body = self.read(name)
        assert body.count(old) >= count, f"{name}: nothing to replace: {old[:60]!r}"
        self.write(name, body.replace(old, new, count))

    def append(self, name, text):
        self.write(name, self.read(name) + text)

    # The coverage-tag checks read Rust, which lives beside the spec rather than
    # inside it, so those mutations need their own pair.
    def rust_edit(self, name, old, new, count=1):
        path = self.root.parent / name
        body = path.read_text(encoding="utf-8")
        assert body.count(old) >= count, f"{name}: nothing to replace: {old[:60]!r}"
        path.write_text(body.replace(old, new, count), encoding="utf-8")

    def rust_append(self, name, text):
        path = self.root.parent / name
        path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")


# --- the suite as committed -------------------------------------------------

@case
def test_the_suite_itself_is_clean(s):
    """The gate CI runs, on the tree as it stands."""
    code, findings = run(s.root)
    gating = [f for f in findings if f["severity"] in ("error", "warning")]
    assert not gating, gating
    assert code == 0


# --- crashes ----------------------------------------------------------------

@case
def test_a_collapsed_pipe_run_does_not_crash_the_run(s):
    """`strip('|')` eats a run of pipes, so a short row passed the pipe count."""
    doc = "08-conformance.md"
    for anchor, row in (("| [INV-D2]", "| INV-D2 |||"),
                        ("| **MG-8**", "| **MG-9** |||||")):
        body = s.read(doc)
        s.edit(doc, anchor, row + "\n" + anchor)
        code, findings = run(s.root)
        assert findings, f"{row}: no findings at all — did the run abort?"
        assert {"trace-row-malformed", "gate-no-capability"} & checks_in(findings), \
            f"{row}: {checks_in(findings)}"
        s.write(doc, body)


# --- links and anchors ------------------------------------------------------

@case
def test_a_dotslash_link_resolves(s):
    s.append("02-request.md", "\nSee [invariants](./07-invariants.md).\n")
    assert "link-missing-file" not in checks_in(run(s.root)[1])


@case
def test_a_broken_link_still_fails(s):
    s.append("02-request.md", "\nSee [nothing](./no-such-file.md).\n")
    assert "link-missing-file" in checks_in(run(s.root)[1])


@case
def test_github_duplicate_anchor_suffixes_resolve(s):
    """`### Filters` appears once per dataset; the second is `#filters-1`."""
    s.append("02-request.md", "\nSee [solana filters](03-catalog.md#filters-1).\n")
    assert "link-missing-anchor" not in checks_in(run(s.root)[1])


@case
def test_an_underscore_survives_into_the_anchor(s):
    s.append("02-request.md",
             "\n## The block_number column\n\n"
             "See [it](02-request.md#the-block_number-column).\n")
    assert "link-missing-anchor" not in checks_in(run(s.root)[1])


@case
def test_an_anchor_naming_no_heading_still_fails(s):
    s.append("02-request.md", "\nSee [nope](07-invariants.md#no-such-heading).\n")
    assert "link-missing-anchor" in checks_in(run(s.root)[1])


# --- fenced examples --------------------------------------------------------

@case
def test_a_fenced_example_is_not_a_reference(s):
    s.append("02-request.md",
             '\n```json\n{"note": "see [x](nope.md), INV-D99, P-NOPE"}\n```\n')
    found = checks_in(run(s.root)[1])
    assert not ({"link-missing-file", "id-undefined", "param-undefined"} & found), found


@case
def test_a_citation_inside_a_fence_does_not_satisfy_id_orphan(s):
    """The leak that matters more: a fence must not silence dead-weight checks."""
    s.edit("08-conformance.md", "| **CT-9** — [fuzz]",
           "| **CT-10** — nothing | Nothing | no | no |\n| **CT-9** — [fuzz]")
    s.append("02-request.md", "\n```json\n{\"note\": \"CT-10\"}\n```\n")
    assert "id-orphan" in checks_in(run(s.root)[1])


# --- the matrix -------------------------------------------------------------

@case
def test_flipping_a_status_invalidates_the_stated_totals(s):
    s.edit("08-conformance.md",
           "| [INV-D2](07-invariants.md#inv-d2) | CT-1 | **C** |",
           "| [INV-D2](07-invariants.md#inv-d2) | CT-1 | **U** |")
    found = checks_in(run(s.root)[1])
    assert "trace-totals-stale" in found, found
    assert "param-observed-stale" in found, found


@case
def test_a_hand_edited_coverage_baseline_is_caught(s):
    """MG-1 ratchets on this number; it may not be loosened by typing."""
    s.edit("09-parameters.md", "0.58 (49 of 85)", "0.99 (84 of 85)")
    assert "param-observed-stale" in checks_in(run(s.root)[1])


@case
def test_a_row_with_no_class_is_caught(s):
    s.edit("08-conformance.md",
           "| [INV-D2](07-invariants.md#inv-d2) | CT-1 |",
           "| [INV-D2](07-invariants.md#inv-d2) |  |")
    assert "trace-class-missing" in checks_in(run(s.root)[1])


@case
def test_two_rows_for_one_invariant_are_caught(s):
    s.edit("08-conformance.md", "| [INV-D3]",
           "| [INV-D2](07-invariants.md#inv-d2) | CT-1 | **U** | dup |\n| [INV-D3]")
    assert "trace-duplicate" in checks_in(run(s.root)[1])


# --- gates and capabilities -------------------------------------------------

@case
def test_a_renamed_bound_heading_is_loud(s):
    """A case-only rename leaves the slug intact, so no link breaks."""
    s.edit("08-conformance.md", "## 8.13 Harness capability register",
           "## 8.13 Harness Capability register")
    assert "section-missing" in checks_in(run(s.root)[1])


@case
def test_a_capability_cited_only_from_the_build_order_is_not_orphaned(s):
    s.edit("08-conformance.md", "| **HC-12**",
           "| **HC-13** Cited from the build order alone | CT-1 | **U** | |\n| **HC-12**")
    s.edit("08-conformance.md", "3. **Coverage reporting**",
           "3. HC-13, named only here.\n4. **Coverage reporting**")
    assert "hc-orphan" not in checks_in(run(s.root)[1])


@case
def test_a_capability_nothing_cites_is_orphaned(s):
    s.edit("08-conformance.md", "| **HC-12**",
           "| **HC-13** Nobody asked for this | CT-1 | **U** | |\n| **HC-12**")
    assert "hc-orphan" in checks_in(run(s.root)[1])


@case
def test_a_blocking_gate_on_an_unbuilt_capability_is_caught(s):
    """HC-9 is at U, so MG-2 may not claim to block on it."""
    s.edit("08-conformance.md",
           "| per-PR | HC-9 | advisory until HC-9 exists |",
           "| per-PR | HC-9 | **blocking** |")
    assert "gate-blocking-unbuilt" in checks_in(run(s.root)[1])


# --- normative shape --------------------------------------------------------

@case
def test_a_source_filename_in_prose_is_leakage(s):
    """A stem ends in a word character, so a shared lookbehind killed these."""
    s.append("02-request.md",
             "\nThe module plan.rs does the work, described in evm.yaml, "
             "listed by Cargo.toml.\n")
    found = [f for f in run(s.root)[1] if f["check"] == "impl-leakage"]
    named = {re.search(r"`([^`]+)`", f["message"]).group(1) for f in found}
    assert {"plan.rs", "evm.yaml", "Cargo.toml"} <= named, named


@case
def test_a_source_path_in_prose_is_leakage(s):
    s.append("02-request.md", "\nHandled in src/query/plan.rs today.\n")
    assert "impl-leakage" in checks_in(run(s.root)[1])


@case
def test_every_leak_on_a_line_is_reported(s):
    """`.search` reported the first only, so fixing one exposed the next."""
    s.append("02-request.md", "\nSee plan.rs and evm.yaml on one line.\n")
    found = [f for f in run(s.root)[1] if f["check"] == "impl-leakage"]
    assert len(found) >= 2, found


@case
def test_an_unrouted_warning_marker_is_caught(s):
    """Was circular: a ⚠ row excused itself for naming its own parameter."""
    s.edit("09-parameters.md",
           "| `P-DEFAULT-COLUMN-WEIGHT` | Weight of a column the catalog gives no "
           "weight (§5.4) | 32 | 32 |",
           "| `P-DEFAULT-COLUMN-WEIGHT` | Weight of a column the catalog gives no "
           "weight (§5.4) | 32 | ⚠ 64 |")
    assert "warn-unratified" in checks_in(run(s.root)[1])


@case
def test_the_word_unratified_does_not_satisfy_the_check(s):
    """`ratif` matched *unratified*, so saying it excused saying nothing."""
    s.edit("09-parameters.md",
           "| `P-DEFAULT-COLUMN-WEIGHT` | Weight of a column the catalog gives no "
           "weight (§5.4) | 32 | 32 |",
           "| `P-DEFAULT-COLUMN-WEIGHT` | This value is unratified | 32 | ⚠ 64 |")
    assert "warn-unratified" in checks_in(run(s.root)[1])


# --- freshness --------------------------------------------------------------

@case
def test_the_matrix_is_stale_when_the_contract_moves_without_it(s):
    s.append("01-data-model.md", "\nA later change.\n")
    repo = s.root.parent
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "PATH": "/usr/bin:/bin",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
           "GIT_COMMITTER_DATE": "2030-01-01T00:00:00Z"}
    subprocess.run(["git", "commit", "-qam", "later"], cwd=repo, check=True,
                   capture_output=True, env=env)
    assert "stale-matrix" in checks_in(run(s.root)[1])


@case
def test_a_mutable_document_with_no_date_is_caught(s):
    s.edit("09-parameters.md", "Observed as of **2026-09-03**.", "")
    assert "date-inconsistent" in checks_in(run(s.root)[1])


# --- suppression and exit codes ---------------------------------------------

@case
def test_a_wildcard_suppression_is_a_usage_error(s):
    s.write(".speccheck-ignore", "* | * | .\n")
    code, _ = run(s.root)
    assert code == 2, code


@case
def test_a_malformed_suppression_is_a_usage_error(s):
    s.write(".speccheck-ignore", "id-orphan | only-two-fields\n")
    assert run(s.root)[0] == 2


@case
def test_a_suppressed_run_never_reads_clean(s):
    s.append("02-request.md", "\nSee [nothing](./no-such-file.md).\n")
    s.write(".speccheck-ignore", "link-missing-file | *.md | does not exist\n")
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = check.main([str(s.root), "--severity", "error"])
    assert "clean" not in buf.getvalue(), buf.getvalue()
    assert "1 suppressed" in buf.getvalue(), buf.getvalue()
    assert code == 0


@case
def test_severity_does_not_decide_what_gates(s):
    """`--strict --severity error` counted warnings after hiding them."""
    s.edit("09-parameters.md", "Observed as of **2026-09-03**.", "")
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = check.main([str(s.root), "--strict", "--severity", "error"])
    assert code == 1, buf.getvalue()


@case
def test_an_info_finding_can_gate(s):
    """No flag combination could fail on one, including the 44 real ones."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = check.main([str(s.root), "--gate", "info", "--only", "inv-missing-check"])
    assert code == 1, buf.getvalue()


@case
def test_only_an_info_check_does_not_announce_clean(s):
    buf = io.StringIO()
    with redirect_stdout(buf):
        check.main([str(s.root), "--only", "inv-missing-check"])
    assert "clean" not in buf.getvalue(), buf.getvalue()


# --- coverage tags ----------------------------------------------------------

@case
def test_a_tag_naming_an_unknown_invariant_is_caught(s):
    s.rust_edit("tests/conformance/ct2_request/bounds.rs",
                "/// Covers CT-2 · INV-Q10", "/// Covers CT-2 · INV-Q99")
    assert "tag-unknown-id" in checks_in(run(s.root)[1])


@case
def test_a_tag_filed_under_the_wrong_class_is_caught(s):
    """The matrix files INV-Q10 under CT-2; a test may not disagree quietly."""
    s.rust_edit("tests/conformance/ct2_request/bounds.rs",
                "/// Covers CT-2 · INV-Q10", "/// Covers CT-5 · INV-Q10")
    assert "tag-class-mismatch" in checks_in(run(s.root)[1])


@case
def test_a_tag_on_something_that_is_not_a_test_is_caught(s):
    """A tagged helper claims coverage its own body never asserts."""
    s.rust_append("tests/conformance/ct1_catalog.rs",
                  "\n/// Covers CT-1 · INV-D1\nfn not_a_test() {}\n")
    assert "tag-orphan" in checks_in(run(s.root)[1])


@case
def test_a_test_the_matrix_names_but_nothing_tags_is_caught(s):
    s.rust_edit("src/metadata/loader.rs", "    /// Covers CT-1 · INV-D10\n", "")
    assert "tag-missing" in checks_in(run(s.root)[1])


@case
def test_a_tagged_test_under_an_unchecked_row_is_caught(s):
    """INV-R11 is at U. A tag saying otherwise means the matrix understates it."""
    s.rust_append("tests/conformance/ct4_relations.rs",
                  "\n/// Covers CT-4 · INV-R11\n#[test]\nfn idempotence() {}\n")
    assert "tag-status-understated" in checks_in(run(s.root)[1])


@case
def test_a_missing_implementation_tree_is_not_silence(s):
    """A check that reads nothing and says nothing is the failure it looks for."""
    shutil.rmtree(s.root.parent / "src")
    shutil.rmtree(s.root.parent / "tests")
    code, findings = run(s.root)
    assert "tag-tree-absent" in checks_in(findings)
    # And it has to *gate*. Registered at warning, it reported the failure and
    # exited 0, which is what a spec-only checkout would have seen.
    assert code == 1, "a tag layer reading nothing must fail the gate CI runs"


@case
def test_half_an_implementation_tree_is_not_silence(s):
    """The dangerous shape: the rules still run, over half the tags."""
    shutil.rmtree(s.root.parent / "src")
    code, findings = run(s.root)
    assert "tag-tree-absent" in checks_in(findings)
    assert code == 1
    # Nothing may be reported *about* the tags that survive: with src/ gone,
    # every invariant tagged only there reads as backed by prose alone.
    assert "tag-unbacked" not in checks_in(findings)


@case
def test_a_near_miss_tag_is_not_dropped(s):
    """`·` is not on a keyboard. A tag typed with a hyphen must not vanish."""
    s.rust_edit("tests/conformance/ct2_request/fork.rs",
                "/// Covers CT-2 · INV-E5", "/// Covers CT-2 - INV-E5")
    assert "tag-malformed" in checks_in(run(s.root)[1])


@case
def test_a_tag_on_an_async_test_is_not_an_orphan(s):
    """`"#[test]" in "#[tokio::test]"` is False, so every async test was one."""
    s.rust_append("tests/conformance/ct4_relations.rs",
                  "\n/// Covers CT-4 · INV-R1\n#[tokio::test]\n"
                  "async fn an_async_test() {}\n")
    assert "tag-orphan" not in checks_in(run(s.root)[1])


@case
def test_an_attribute_above_the_doc_comment_is_still_a_test(s):
    """Attributes are as legal above a doc comment as below it."""
    s.rust_append("tests/conformance/ct4_relations.rs",
                  "\n#[test]\n/// Covers CT-4 · INV-R1\nfn attribute_first() {}\n")
    assert "tag-orphan" not in checks_in(run(s.root)[1])


@case
def test_a_matrix_note_naming_a_deleted_test_is_caught(s):
    """The direction that mattered: a row still claiming evidence that is gone."""
    s.rust_edit("src/metadata/loader.rs",
                "fn test_validate_rejects_duplicate_names(", "fn renamed_away(")
    assert "note-stale" in checks_in(run(s.root)[1])


@case
def test_a_tag_retargeted_at_another_invariant_is_caught(s):
    """A tag proves coverage of the invariant it names, not of any invariant."""
    s.rust_edit("src/metadata/loader.rs",
                "/// Covers CT-1 · INV-D10", "/// Covers CT-1 · INV-D2")
    assert "tag-missing" in checks_in(run(s.root)[1])


@case
def test_a_test_filed_under_a_class_it_does_not_claim_is_caught(s):
    s.rust_append("tests/conformance/ct4_relations.rs",
                  "\n/// Covers CT-1 · INV-D1\n#[test]\nfn misfiled() {}\n")
    assert "tag-class-misfiled" in checks_in(run(s.root)[1])


@case
def test_a_tag_on_a_macro_generated_test_is_not_an_orphan(s):
    """113 fixture tests are composed by `paste!` and spelled nowhere."""
    assert "tag-orphan" not in checks_in(run(s.root)[1])
    names = check.test_fns(s.root.parent)
    assert "solana_include_all_blocks" in names, "paste-generated tests are invisible"


@case
def test_a_stale_evidence_count_is_caught(s):
    s.edit("08-conformance.md", "are backed by a tagged test", "are backed by no tagged test")
    assert "trace-evidence-stale" in checks_in(run(s.root)[1])


@case
def test_a_stale_portable_gate_count_is_caught(s):
    s.edit("08-conformance.md", "are backed only by tests marked",
           "of them are backed only by tests marked")
    assert "trace-evidence-stale" in checks_in(run(s.root)[1])


@case
def test_a_matrix_heading_rename_does_not_fabricate_tag_findings(s):
    """`s.matrix` was assigned after the early return, so every tag went unknown."""
    s.edit("08-conformance.md", "## 8.11 Traceability matrix", "## 8.11 traceability matrix")
    checks = checks_in(run(s.root)[1])
    assert "trace-missing" in checks
    assert "tag-unknown-id" not in checks, "one real finding under 48 invented ones"


@case
def test_a_covered_row_backed_only_by_prose_is_reported(s):
    """Not an error — the count is what MG-1 ratchets — but never invisible.

    Asserted on a row this test makes prose-only, rather than on the tree as it
    stands: written the second way it goes red on the day §8.11 reaches the goal
    it sets, which is the one outcome a self-test must not punish.
    """
    s.rust_edit("src/metadata/loader.rs", "    /// Covers CT-1 · INV-D3\n", "")
    found = [f for f in run(s.root)[1] if f["check"] == "tag-unbacked"]
    assert any("INV-D3" in f["message"] for f in found), found


@case
def test_a_row_backed_only_by_ignored_tests_is_reported(s):
    """A status no job the gate runs could falsify is a status worth seeing."""
    found = [f for f in run(s.root)[1] if f["check"] == "tag-data-backed-only"]
    assert found, "no row rests on ignored tests alone; drop the check or the claim"


def main() -> int:
    import contextlib
    failures = []
    for fn in CASES:
        with contextlib.ExitStack() as stack:
            suite = Suite(stack)
            try:
                fn(suite)
            except AssertionError as exc:
                failures.append((fn.__name__, str(exc) or "assertion failed"))
            except Exception as exc:  # a check that crashes is a failure too
                failures.append((fn.__name__, f"{type(exc).__name__}: {exc}"))
    for name, why in failures:
        print(f"FAIL {name}: {why}")
    print(f"\n{len(CASES) - len(failures)}/{len(CASES)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
