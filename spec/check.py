#!/usr/bin/env python3
"""Quality gate for the specification suite in this directory.

A spec is a graph: invariants cite each other, the traceability matrix cites
invariants and test classes, merge gates cite parameters and capabilities,
decisions are cited from normative text. Nobody holds that graph in their head,
so it rots silently — an ID is renamed, a matrix row is forgotten, a decision is
amended in one place only. None of it is visible when reading. All of it is
mechanically detectable.

    python3 spec/check.py spec              # human-readable, gates on errors
    python3 spec/check.py spec --list-checks
    python3 spec/check.py spec --format github --severity warning

Exit 0 clean, 1 findings, 2 bad usage. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Suite conventions. These mirror the "Conventions" section of spec/README.md;
# when that section changes, change this block with it.
# ---------------------------------------------------------------------------

INVARIANTS_DOC = "07-invariants.md"
CONFORMANCE_DOC = "08-conformance.md"
PARAMETERS_DOC = "09-parameters.md"
GAPS_DOC = "GAPS.md"
README_DOC = "README.md"
DECISIONS_DIR = "decisions"

# Prefix -> the one document that defines it.
HOME = {
    "INV": INVARIANTS_DOC,
    "CT": CONFORMANCE_DOC,
    "MG": CONFORMANCE_DOC,
    "HC": CONFORMANCE_DOC,
    "ADR": DECISIONS_DIR,
}

# Invariant bands. A hole between two numbers of the same band is a removed
# invariant; a hole across bands is the convention working.
INV_BANDS = ("D", "Q", "P", "R", "B", "O", "E", "X")

# Documents that track current state and may change without the contract changing.
MUTABLE = {CONFORMANCE_DOC, PARAMETERS_DOC, GAPS_DOC}

# Documents whose text is normative prose. Everything else is exempt from the
# implementation-leakage and bare-constant checks.
NORMATIVE = {
    "01-data-model.md",
    "02-request.md",
    "03-catalog.md",
    "04-evaluation.md",
    "05-response.md",
    "06-errors.md",
    INVARIANTS_DOC,
    PARAMETERS_DOC,
}

# 03 enumerates the catalog's actual contents — column weights, bloom widths,
# hex digit counts — so concrete numbers there are the point. 09 is the registry.
NO_BARE_CONSTANTS = NORMATIVE - {"03-catalog.md", PARAMETERS_DOC}

# Section headings inside the conformance doc that the checks bind to.
MATRIX_HEADING = "8.11 Traceability matrix"
GATES_HEADING = "8.12 Merge gates"
HARNESS_HEADING = "8.13 Harness capability register"

ADR_STATUSES = ("Proposed", "Accepted", "Superseded")

# `**Totals: 28 C, 34 P, 23 U** of 85`, and `P-COV-PROPERTY`'s observed cell.
TOTALS_RE = re.compile(r"\*\*Totals:\s*(\d+)\s*C,\s*(\d+)\s*P,\s*(\d+)\s*U\*\*\s*of\s*(\d+)")
COV_OBSERVED_RE = re.compile(r"(\d\.\d+)\s*\((\d+)\s+of\s+(\d+)\)")

# ---------------------------------------------------------------------------

CHECKS = {
    # reference integrity
    "id-undefined": ("E", "Every ID referenced is defined in its home document"),
    "id-duplicate": ("E", "No ID is defined twice"),
    "param-undefined": ("E", "Every P-* symbol has a row in the parameter registry"),
    "link-missing-file": ("E", "Relative links resolve from the linking file"),
    "link-missing-anchor": ("E", "Anchored links name a real heading"),
    "doc-unlisted": ("E", "Every document is listed in the README document map"),
    "doc-missing": ("E", "Every document-map entry exists"),
    "adr-undefined": ("E", "Every ADR-n referenced has a file"),
    # dead weight
    "id-orphan": ("W", "Every defined ID is cited from somewhere other than its definition"),
    "param-unused": ("W", "Every registry row is cited outside the registry"),
    # traceability
    "trace-missing": ("E", "Every invariant has a row in the traceability matrix"),
    "trace-unknown": ("E", "Every ID named in a matrix row is defined"),
    "trace-bad-status": ("E", "Every matrix row carries a status of C, P or U"),
    "trace-class-unknown": ("E", "Every test class named in a matrix row exists"),
    "trace-class-missing": ("E", "Every matrix row names a test class"),
    "trace-duplicate": ("E", "No invariant has two matrix rows"),
    "trace-row-malformed": ("E", "Every matrix or gate row has the columns its table declares"),
    "trace-totals-stale": ("E", "The stated matrix totals match the rows"),
    "param-observed-stale": ("E", "A derived observed value matches what it is derived from"),
    "gate-blocking-unbuilt": ("E", "No blocking gate rests on an unbuilt capability"),
    "section-missing": ("E", "Every section the checks bind to exists"),
    "gate-threshold-literal": ("E", "Every merge-gate threshold is a P-* symbol, not a literal"),
    "gate-no-capability": ("E", "Every merge gate names a harness capability"),
    "gate-unknown-class": ("E", "Every test class named by a merge gate exists"),
    "hc-orphan": ("W", "Every harness capability is needed by some class or gate"),
    # coverage tags — the matrix read back off the tests it claims
    # A partial tree is the dangerous case: the checks still run, read half the
    # tags, and report the other half's invariants as backed by nothing. So this
    # gates rather than warns — it is the guard on every rule below it.
    "tag-tree-absent": ("E", "The implementation tree is beside the spec, so tags can be read"),
    "tag-unknown-id": ("E", "Every invariant a coverage tag names has a matrix row"),
    "tag-class-mismatch": ("E", "A coverage tag's class is the one the matrix gives that invariant"),
    "tag-class-misfiled": ("E", "A tagged test under tests/conformance/ctN claims CT-n"),
    "tag-orphan": ("E", "Every coverage tag sits on a test"),
    "tag-malformed": ("E", "Every line that reads like a coverage tag parses as one"),
    "tag-missing": ("E", "Every test the matrix names by hand carries its tag for that invariant"),
    "tag-status-understated": ("E", "No invariant marked unchecked has a tagged test"),
    "note-stale": ("E", "Every name a matrix note cites still exists in the tree"),
    "trace-evidence-stale": ("E", "The stated tag-backed and prose-only counts match the rows"),
    "tag-unbacked": ("I", "Every covered invariant is backed by a tagged test, not only by prose"),
    "tag-data-backed-only": ("I", "No covered invariant rests only on tests the portable gate skips"),
    "ct-ungated": ("I", "Every test class is reachable from some merge gate"),
    # decision log
    "adr-bad-status": ("E", "Every ADR carries a parseable Status line"),
    "adr-id-mismatch": ("E", "An ADR's filename and heading agree on its number"),
    "adr-uncited": ("W", "Every ADR is cited from a normative document"),
    # normative shape
    "inv-missing-check": ("I", "Every invariant states how to falsify it in prose"),
    "trace-unsupported-status": ("W", "Every invariant marked covered names what covers it"),
    "param-bare-constant": ("W", "No bare dimensioned literal in normative prose"),
    "impl-leakage": ("W", "No dependency name, source path or file extension in normative prose"),
    "heading-duplicate": ("W", "No two headings in a file share an anchor"),
    "warn-unratified": ("W", "Every ⚠ marker routes to an ADR, a gap or a registry row"),
    "gap-missing-body": ("W", "Every gap in the summary table has a detail section"),
    "id-number-gap": ("I", "No unused number inside a band"),
    # freshness
    "stale-matrix": ("W", "The matrix is no older than the normative documents"),
    "date-inconsistent": ("W", "A mutable document states one as-of date"),
}

SEV_ORDER = {"E": 0, "W": 1, "I": 2}
SEV_NAME = {"E": "error", "W": "warning", "I": "info"}

ID_RE = re.compile(r"\b(INV-[A-Z]\d+|(?:CT|MG|HC|ADR)-\d+)\b")
PARAM_RE = re.compile(r"\bP-[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*\b")
LINK_RE = re.compile(r"\]\(([^)\s]*?)(?:#([^)\s]+))?\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
INV_DEF_RE = re.compile(r"^###\s+(INV-[A-Z]\d+)\s*$")
BOLD_DEF_RE = re.compile(r"^\|\s*\*\*((?:CT|MG|HC)-\d+)\*\*")
DATE_RE = re.compile(r"\b(20\d\d-\d\d-\d\d)\b")

# Dimensioned literals. Deliberately narrow: a number with a unit or a countable
# noun is a tunable; "32-bit" and "2⁴⁰" are facts about a type.
BARE_CONST_RE = re.compile(
    r"(?<![\w.-])\d[\d\s_,]*\s*(?:MiB|KiB|GiB|MB|KB|GB|ms|µs|seconds?|minutes?"
    r"|bytes?|values?|entries|item requests|requests|hashes)\b",
    re.I,
)
# Lines that are notes about tests or rationale, not normative statements.
NON_NORMATIVE_LINE = re.compile(r"^\s*[*_]?(Test|Why|Note|Example)[:*_]")


class Finding:
    __slots__ = ("check", "sev", "path", "line", "message", "repo_relative")

    def __init__(self, check, path, line, message, repo_relative=False):
        self.check = check
        self.sev = CHECKS[check][0]
        self.path = path
        self.line = line
        self.message = message
        # A finding against `src/` or `tests/` names a path relative to the
        # repository, not to the spec directory. Prefixing it with the spec
        # directory names a file that does not exist, and GitHub silently drops
        # an annotation whose file it cannot find — so the only two rules that
        # point at code were the two invisible on the diff.
        self.repo_relative = repo_relative

    def key(self):
        return (SEV_ORDER[self.sev], self.path, self.line, self.check)


def slug(heading: str) -> str:
    """GitHub's heading anchor: strip formatting, lowercase, spaces to hyphens.

    Underscores and non-ASCII word characters survive, as they do on GitHub —
    `## The block_number column` anchors at `#the-block_number-column`. Repeated
    headings take a `-1`, `-2` suffix, applied by `anchors_of`, which alone knows
    the order they appear in.
    """
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[*~]", "", text)
    text = re.sub(r"[^\w\s-]", "", text.lower(), flags=re.UNICODE)
    return text.strip().replace(" ", "-")


def anchors_of(lines):
    """Heading anchors in document order -> {anchor: (line, base, ordinal)}."""
    seen, out = {}, {}
    for lineno, line in enumerate(lines, 1):
        m = HEADING_RE.match(line)
        if not m:
            continue
        base = slug(m.group(2))
        n = seen.get(base, 0)
        seen[base] = n + 1
        out[base if n == 0 else f"{base}-{n}"] = (lineno, base, n)
    return out


def cells_of(line: str):
    """A markdown table row's cells, empty ones included.

    `str.strip("|")` takes a character *set*, so it eats a run of pipes and an
    empty leading cell disappears with it — which is how a row can satisfy a
    pipe count and still be short of the cells the caller indexes.
    """
    row = line.strip()
    if row.startswith("|"):
        row = row[1:]
    if row.endswith("|"):
        row = row[:-1]
    return [c.strip() for c in row.split("|")]


def strip_fences(body: str) -> str:
    """Blank out fenced code, preserving line numbering."""
    out, fenced = [], False
    for line in body.split("\n"):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            out.append("")
        else:
            out.append("" if fenced else line)
    return "\n".join(out)


def enclosing_section(lines, index) -> str:
    """The `##` section containing `index`, heading included."""
    start = 0
    for i in range(index, -1, -1):
        m = HEADING_RE.match(lines[i])
        if m and len(m.group(1)) == 2:
            start = i
            break
    end = len(lines)
    for i in range(index + 1, len(lines)):
        m = HEADING_RE.match(lines[i])
        if m and len(m.group(1)) == 2:
            end = i
            break
    return "\n".join(lines[start:end])


def enclosing_paragraph(lines, index) -> str:
    """The blank-line-delimited block containing `index`."""
    start = index
    while start > 0 and lines[start - 1].strip():
        start -= 1
    end = index + 1
    while end < len(lines) and lines[end].strip():
        end += 1
    return "\n".join(lines[start:end])


def section_of(lines, index) -> str:
    """The nearest preceding `##` heading text, for scoping checks to a section."""
    for i in range(index, -1, -1):
        m = HEADING_RE.match(lines[i])
        if m and len(m.group(1)) == 2:
            return m.group(2).strip()
    return ""


class Suite:
    def __init__(self, root: Path):
        self.root = root
        self.files = sorted(p for p in root.rglob("*.md"))
        self.rel = {p: p.relative_to(root).as_posix() for p in self.files}
        self.text = {self.rel[p]: p.read_text(encoding="utf-8") for p in self.files}
        self.lines = {k: v.split("\n") for k, v in self.text.items()}
        self.nofence = {k: strip_fences(v) for k, v in self.text.items()}
        # Every reference scan reads these. A fenced example is illustration, not
        # a citation: an ID inside one must neither satisfy `id-orphan` nor
        # trip `id-undefined`.
        self.nolines = {k: v.split("\n") for k, v in self.nofence.items()}

    def find_line(self, doc: str, needle: str, start: int = 0) -> int:
        for i in range(start, len(self.lines[doc])):
            if needle in self.lines[doc][i]:
                return i + 1
        return 1

    def section_body(self, doc: str, heading: str):
        """(start, end) line indices of a `##` section, end exclusive."""
        lines = self.lines.get(doc, [])
        start = None
        for i, line in enumerate(lines):
            m = HEADING_RE.match(line)
            if not m or len(m.group(1)) != 2:
                continue
            if start is None and m.group(2).strip() == heading:
                start = i + 1
            elif start is not None:
                return start, i
        return (start, len(lines)) if start is not None else (None, None)


def collect_definitions(s: Suite, out: list):
    """ID -> [(doc, line)]. Invariants by heading, CT/MG/HC by bold table cell."""
    defs = defaultdict(list)

    for lineno, line in enumerate(s.nolines.get(INVARIANTS_DOC, []), 1):
        m = INV_DEF_RE.match(line)
        if m:
            defs[m.group(1)].append((INVARIANTS_DOC, lineno))

    for lineno, line in enumerate(s.nolines.get(CONFORMANCE_DOC, []), 1):
        m = BOLD_DEF_RE.match(line)
        if m:
            defs[m.group(1)].append((CONFORMANCE_DOC, lineno))

    for doc in s.text:
        if not doc.startswith(DECISIONS_DIR + "/"):
            continue
        stem = Path(doc).name
        fm = re.match(r"ADR-(\d+)-", stem)
        hm = re.search(r"^#\s*(ADR-(\d+))\b", s.text[doc], re.M)
        if not fm or not hm:
            out.append(Finding("adr-id-mismatch", doc, 1,
                               "ADR file needs an `ADR-<n>-<slug>.md` name and an "
                               "`# ADR-<n> — Title` heading"))
            continue
        if fm.group(1) != hm.group(2):
            out.append(Finding("adr-id-mismatch", doc, 1,
                               f"filename says ADR-{fm.group(1)}, heading says "
                               f"ADR-{hm.group(2)} — make them agree"))
        defs[hm.group(1)].append((doc, 1))

    for ident, places in defs.items():
        if len(places) > 1:
            where = ", ".join(f"{d}:{ln}" for d, ln in places[1:])
            out.append(Finding("id-duplicate", places[0][0], places[0][1],
                               f"{ident} is defined again at {where} — one ID, one definition"))
    return {k: v[0] for k, v in defs.items()}


def check_references(s: Suite, defs, out: list):
    refs = defaultdict(list)
    for doc, lines in s.nolines.items():
        for lineno, line in enumerate(lines, 1):
            for ident in ID_RE.findall(line):
                refs[ident].append((doc, lineno))

    for ident, places in sorted(refs.items()):
        if ident in defs:
            continue
        doc, lineno = places[0]
        home = HOME[ident.split("-")[0]]
        out.append(Finding("adr-undefined" if ident.startswith("ADR") else "id-undefined",
                           doc, lineno,
                           f"{ident} is referenced but never defined — define it in {home} "
                           f"or fix the reference"))

    for ident, (doc, lineno) in sorted(defs.items()):
        # MG-n is a leaf: a gate is the thing other rules point *at*, and nothing
        # is obliged to point back. HC-n has its own check. Both would otherwise
        # report every gate in the table on a suite that is entirely correct.
        if ident.startswith(("MG-", "HC-")):
            continue
        cited = [(d, ln) for d, ln in refs.get(ident, []) if (d, ln) != (doc, lineno)]
        if not cited:
            out.append(Finding("id-orphan", doc, lineno,
                               f"{ident} is defined but nothing cites it — cite it where it "
                               f"applies, or retire it"))
    return refs


def check_parameters(s: Suite, out: list):
    registry, reg_line = set(), {}
    body = s.nolines.get(PARAMETERS_DOC, [])
    for lineno, line in enumerate(body, 1):
        if line.startswith("|"):
            cells = cells_of(line)
            for prm in PARAM_RE.findall(cells[0] if cells else ""):
                registry.add(prm)
                reg_line.setdefault(prm, lineno)

    used = defaultdict(list)
    for doc, lines in s.nolines.items():
        for lineno, line in enumerate(lines, 1):
            for prm in PARAM_RE.findall(line):
                used[prm].append((doc, lineno))

    for prm, places in sorted(used.items()):
        if prm in registry:
            continue
        doc, lineno = places[0]
        out.append(Finding("param-undefined", doc, lineno,
                           f"{prm} has no row in {PARAMETERS_DOC} — add one with its "
                           f"observed and target values"))

    for prm in sorted(registry):
        outside = [p for p in used.get(prm, []) if p[0] != PARAMETERS_DOC]
        if not outside:
            out.append(Finding("param-unused", PARAMETERS_DOC, reg_line[prm],
                               f"{prm} is registered but used nowhere — cite it where it "
                               f"applies, or drop the row"))
    return registry


def check_links(s: Suite, out: list):
    # A repeated sub-heading under two different `##` sections (`### Selectable
    # fields`, once per dataset) is normal structure: GitHub gives the second one
    # `#selectable-fields-1`, so both are addressable. It is a defect only when
    # something links to the *bare* anchor, which silently means the first.
    anchors = {doc: anchors_of(lines) for doc, lines in s.nolines.items()}
    linked_bare = defaultdict(set)
    for doc, body in s.nofence.items():
        for m in LINK_RE.finditer(body):
            if m.group(2):
                linked_bare[m.group(1) or doc].add(m.group(2))

    for doc, found in anchors.items():
        for anchor, (lineno, base_slug, ordinal) in found.items():
            if ordinal == 0:
                continue
            if base_slug not in linked_bare.get(doc, ()):
                continue
            first = found[base_slug][0]
            out.append(Finding("heading-duplicate", doc, lineno,
                               f"heading anchor `#{base_slug}` is already used at line "
                               f"{first}; a link to the bare anchor resolves there, not "
                               f"here — link `#{anchor}` if this is the one meant"))

    for doc, body in s.nofence.items():
        base = Path(doc).parent
        for m in LINK_RE.finditer(body):
            target, anchor = m.group(1), m.group(2)
            if target and "://" in target:
                continue
            lineno = body[:m.start()].count("\n") + 1
            if target:
                resolved = os.path.normpath(os.path.join(str(base), target))
                resolved = resolved.replace(os.sep, "/")
                if resolved.endswith("/"):
                    resolved = resolved[:-1]
                is_dir = (s.root / resolved).is_dir()
                if not is_dir and resolved not in s.text:
                    out.append(Finding("link-missing-file", doc, lineno,
                                       f"link points at `{target}`, which does not exist"))
                    continue
                if is_dir:
                    continue
            else:
                resolved = doc
            if anchor and anchor not in anchors.get(resolved, {}):
                out.append(Finding("link-missing-anchor", doc, lineno,
                                   f"`{resolved}#{anchor}` names no heading in {resolved}"))


def check_document_map(s: Suite, out: list):
    readme = s.nofence.get(README_DOC, "")
    listed = set()
    for m in LINK_RE.finditer(readme):
        t = m.group(1)
        if t and not t.startswith("http"):
            listed.add(t.rstrip("/"))
    for doc in sorted(s.text):
        if doc == README_DOC or doc.startswith(DECISIONS_DIR + "/"):
            continue
        if doc not in listed:
            out.append(Finding("doc-unlisted", README_DOC, 1,
                               f"{doc} is not in the README document map — a document "
                               f"nobody is pointed at is a document nobody reads"))
    for entry in sorted(listed):
        if entry.endswith(".md") and entry not in s.text:
            out.append(Finding("doc-missing", README_DOC,
                               s.find_line(README_DOC, entry),
                               f"the document map lists `{entry}`, which does not exist"))
    if DECISIONS_DIR not in listed:
        out.append(Finding("doc-unlisted", README_DOC, 1,
                           f"`{DECISIONS_DIR}/` is not in the README document map"))


def check_traceability(s: Suite, defs, out: list):
    # `check_test_tags` reads the tests back against these rows. One parse, so
    # the two cannot drift into disagreeing about what the matrix says — and it
    # is set before the guard below, because a run that returns early otherwise
    # buries one true finding under a fabricated one per tag in the tree.
    s.matrix = {}

    start, end = s.section_body(CONFORMANCE_DOC, MATRIX_HEADING)
    if start is None:
        out.append(Finding("trace-missing", CONFORMANCE_DOC, 1,
                           f"no `## {MATRIX_HEADING}` section — nothing records what is "
                           f"covered"))
        return set()

    lines = s.nolines[CONFORMANCE_DOC]
    classes = {i for i in defs if i.startswith("CT-")}
    covered, seen_at = set(), {}
    counts = {"C": 0, "P": 0, "U": 0}
    for i in range(start, end):
        line = lines[i]
        if not line.startswith("|") or line.startswith("|---"):
            continue
        cells = cells_of(line)
        if not ID_RE.search(cells[0] if cells else ""):
            continue
        if len(cells) < 4:
            out.append(Finding("trace-row-malformed", CONFORMANCE_DOC, i + 1,
                               f"matrix row has {len(cells)} columns; the table declares "
                               f"Invariant, Class, Status and Note"))
            continue
        ids = ID_RE.findall(cells[0])
        for ident in ids:
            covered.add(ident)
            if ident in seen_at:
                out.append(Finding("trace-duplicate", CONFORMANCE_DOC, i + 1,
                                   f"{ident} already has a matrix row at line "
                                   f"{seen_at[ident]} — one invariant, one status"))
            else:
                seen_at[ident] = i + 1
            if ident not in defs:
                out.append(Finding("trace-unknown", CONFORMANCE_DOC, i + 1,
                                   f"matrix row names {ident}, which is not defined"))
        cls = ID_RE.findall(cells[1])
        if not cls:
            out.append(Finding("trace-class-missing", CONFORMANCE_DOC, i + 1,
                               f"matrix row for {ids[0]} names no test class — say which "
                               f"class would check it, or that none does"))
        for c in cls:
            if c not in classes:
                out.append(Finding("trace-class-unknown", CONFORMANCE_DOC, i + 1,
                                   f"matrix row for {ids[0]} names test class {c}, which "
                                   f"is not in the taxonomy"))
        status = re.sub(r"[*\s]", "", cells[2])
        if status not in counts:
            out.append(Finding("trace-bad-status", CONFORMANCE_DOC, i + 1,
                               f"matrix row for {ids[0]} has status `{cells[2]}`; "
                               f"expected C, P or U"))
        else:
            counts[status] += len(ids)
            for ident in ids:
                s.matrix[ident] = (i + 1, set(cls), status, cells[3])
        # "Covered" has to name what covers it, or the matrix is an opinion. A
        # note citing a test, a fixture set or a gap counts; empty prose does not.
        note = cells[3]
        if status == "C" and not re.search(r"`[^`]+`|\bcases\b|\bfixtures?\b|GAP", note):
            out.append(Finding("trace-unsupported-status", CONFORMANCE_DOC, i + 1,
                               f"{ids[0]} is marked covered but its note names no test — "
                               f"say what would fail if it broke, or downgrade to P"))

    for ident, (doc, lineno) in sorted(defs.items()):
        if ident.startswith("INV-") and ident not in covered:
            out.append(Finding("trace-missing", doc, lineno,
                               f"{ident} has no row in the traceability matrix of "
                               f"{CONFORMANCE_DOC} — nobody has decided how to test it"))

    check_matrix_totals(s, counts, out)
    return classes


# --- coverage tags ----------------------------------------------------------
#
# §8.11 is prose, and prose is an opinion. A test carries
#
#     /// Covers CT-2 · INV-Q6, INV-Q7
#
# and these checks read the two against each other: a tag naming a class the
# matrix does not give that invariant is a disagreement, and so is a row at U
# whose invariant something already tests. What neither side can fake is the
# count of rows backed by nothing but a sentence, which is what HC-8 ratchets.

TAG_RE = re.compile(r"^\s*///\s*Covers\s+(CT-\d+)\s+·\s+(INV-[A-Z]\d+(?:\s*,\s*INV-[A-Z]\d+)*)\s*$")
# Anything shaped like a tag. The strict pattern drops what it cannot parse, and
# a dropped tag reads exactly like a test nobody tagged — so an ASCII hyphen for
# the `·`, a trailing sentence, or `//` for `///` has to be a finding of its own.
TAG_LOOSE_RE = re.compile(r"^\s*//[/!]?\s*Covers\b")
FN_RE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
# `paste::paste!` composes a name the source never spells. `paste_tests` expands
# the one shape the suite uses; this matches the definition so a tag above one is
# not read as sitting on nothing.
PASTE_FN_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+"
    r"\[<\s*([A-Za-z_][A-Za-z0-9_]*)\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*>\]"
)
MACRO_DEF_RE = re.compile(r"^\s*macro_rules!\s+([A-Za-z_][A-Za-z0-9_]*)")
# `name!(arg);` and `name!(arg, "extra");` alike — the name is composed from the
# first argument, and a second one for the fixture file changes nothing.
MACRO_CALL_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)!\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*[,)]")
# `#[test]` and the wrappers that stand in for it. Substring matching read
# `#[tokio::test]` as *not* a test and reported every async one as an orphan.
TEST_ATTR_RE = re.compile(r"^\s*#\[\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*test\s*[\]\(]")
IGNORE_ATTR_RE = re.compile(r"^\s*#\[\s*ignore\s*[\]=\(]")
SKIP_RE = re.compile(r"^\s*(///|//!|#\[|\)|\]|$)")
# An attribute is as legal above the doc comment as below it.
ATTR_OR_DOC_RE = re.compile(r"^\s*(///|#\[)")
# A name in a matrix note is a test only if it reads like one. `block_number` and
# `can_skip` are columns and methods, and demanding a tag on those would make the
# check unsatisfiable.
NOTE_NAME_RE = re.compile(r"`([a-z_][a-z0-9_]{6,})`")
WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# `Of the 71 rows at **C** or **P**, 49 are backed by a tagged test and 22 rest
# on prose alone.` — three numbers derived from the matrix, typed by hand.
EVIDENCE_RE = re.compile(
    r"Of the (\d+) rows at \*\*C\*\* or \*\*P\*\*, (\d+) are backed by a tagged test "
    r"and (\d+) rest on prose alone"
)
# `7 of the 49 are backed only by tests marked `#[ignore]`` — the rows a status
# claims but no job the gate runs would falsify.
DATA_BACKED_RE = re.compile(r"(\d+) of the (\d+) are backed only by tests marked")

TREE_ROOTS = ("src", "tests")
CONFORMANCE_DIR = "tests/conformance/"


def rust_files(repo: Path):
    for name in TREE_ROOTS:
        base = repo / name
        if not base.is_dir():
            continue
        # `followlinks` stays off: `tests/fixtures` is a link to an external tree.
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
            for f in sorted(filenames):
                if f.endswith(".rs"):
                    yield Path(dirpath) / f


def rust_sources(repo: Path):
    """[(path relative to the repo, lines)], read once for the whole run.

    Three rules walk this tree, and reading it three times is most of what the
    checker's own test suite spends its time on.
    """
    cached = getattr(rust_sources, "_cache", {})
    key = str(repo)
    if key not in cached:
        cached[key] = [
            (p.relative_to(repo).as_posix(),
             p.read_text(encoding="utf-8", errors="replace").split("\n"))
            for p in rust_files(repo)
        ]
        rust_sources._cache = cached
    return cached[key]


def paste_tests(lines):
    """Test names a `paste::paste!` macro in this file composes.

    One shape, the one the fixture suite uses: a `macro_rules!` whose body is a
    `#[test] fn [<prefix_ $arg>]()`, invoked as `name!(arg);`. Without this the
    113 fixture tests are invisible to every rule below, which is the population
    §8.11's prose-only rows are meant to be moved into.
    """
    prefixes, macro = {}, None
    for line in lines:
        m = MACRO_DEF_RE.match(line)
        if m:
            macro = m.group(1)
            continue
        if macro:
            m = PASTE_FN_RE.match(line)
            if m:
                prefixes[macro] = m.group(1)
                macro = None

    names = set()
    for line in lines:
        m = MACRO_CALL_RE.match(line)
        if m and m.group(1) in prefixes:
            names.add(prefixes[m.group(1)] + m.group(2))
    return names


def tagged_item(lines, i):
    """(name, is a test, is ignored) for the item the tag on line `i` sits on.

    Both directions are walked, because an attribute may sit above the doc
    comment as easily as below it. The forward walk ends at the first line that
    is not part of the item's header: a fixed line budget silently turned a long
    doc comment into a tag on nothing.
    """
    is_test = ignored = False

    j = i - 1
    while j >= 0 and ATTR_OR_DOC_RE.match(lines[j]):
        is_test = is_test or bool(TEST_ATTR_RE.match(lines[j]))
        ignored = ignored or bool(IGNORE_ATTR_RE.match(lines[j]))
        j -= 1

    name = None
    j = i + 1
    while j < len(lines):
        is_test = is_test or bool(TEST_ATTR_RE.match(lines[j]))
        ignored = ignored or bool(IGNORE_ATTR_RE.match(lines[j]))

        fn = FN_RE.match(lines[j])
        if fn:
            name = fn.group(1)
            break

        paste = PASTE_FN_RE.match(lines[j])
        if paste:
            # A macro composes the name, so there is none to record — but the
            # tag does sit on a test, which is what the orphan rule asks.
            name = f"[<{paste.group(1)} ${paste.group(2)}>]"
            break

        if not SKIP_RE.match(lines[j]):
            break
        j += 1

    return name, is_test, ignored


def read_tags(repo: Path, out: list):
    """{invariant: {(class, site, ignored, line)}}, read off the implementation tree."""
    tagged = defaultdict(set)
    for rel, lines in rust_sources(repo):
        for i, line in enumerate(lines):
            m = TAG_RE.match(line)
            if not m:
                if TAG_LOOSE_RE.match(line):
                    out.append(Finding("tag-malformed", rel, i + 1,
                                       f"`{line.strip()}` reads like a coverage tag but does "
                                       f"not parse as one — a tag is `/// Covers CT-n · "
                                       f"INV-Xn`, with a middle dot and nothing after the IDs",
                                       repo_relative=True))
                continue

            cls, ids = m.group(1), ID_RE.findall(m.group(2))
            # The tag belongs to the item it sits above, and only to a test: a
            # tagged helper claims coverage its own assertions do not make.
            name, is_test, ignored = tagged_item(lines, i)
            if name is None or not is_test:
                out.append(Finding("tag-orphan", rel, i + 1,
                                   f"`Covers {cls}` sits on no test — a tag on anything "
                                   f"else claims coverage nothing asserts",
                                   repo_relative=True))
                continue

            for ident in ids:
                tagged[ident].add((cls, f"{rel}::{name}", ignored, i + 1))
    return tagged


def test_fns(repo: Path):
    """Every `#[test]` function name in the tree, memoised for the run."""
    cached = getattr(test_fns, "_cache", {})
    key = str(repo)
    if key in cached:
        return cached[key]
    names = set()
    for _rel, lines in rust_sources(repo):
        names |= paste_tests(lines)
        marked = False
        for line in lines:
            if TEST_ATTR_RE.match(line):
                marked = True
                continue
            fn = FN_RE.match(line)
            if fn:
                if marked:
                    names.add(fn.group(1))
                marked = False
            elif line.strip() and not SKIP_RE.match(line):
                marked = False
    cached[key] = names
    test_fns._cache = cached
    return names


def tree_words(repo: Path):
    """Every identifier-shaped word in the tree, memoised.

    Deliberately crude, because it answers one question: does this name exist at
    all? A note may legitimately name a method or a column, so anything the tree
    spells counts; a name the tree does not spell anywhere has been renamed or
    deleted out from under the row that cites it.
    """
    cached = getattr(tree_words, "_cache", {})
    key = str(repo)
    if key in cached:
        return cached[key]
    words = set()
    for _rel, lines in rust_sources(repo):
        for line in lines:
            words.update(WORD_RE.findall(line))
    cached[key] = words
    tree_words._cache = cached
    return words


def check_test_tags(s: Suite, out: list):
    repo = s.root.parent
    absent = [name for name in TREE_ROOTS if not (repo / name).is_dir()]
    if absent:
        # Half a tree is worse than none: every rule below still runs, reads the
        # tags that survive, and reports the rest of the matrix as backed by
        # nothing. So any missing root stops the whole layer.
        out.append(Finding("tag-tree-absent", CONFORMANCE_DOC, 1,
                           f"{', '.join(n + '/' for n in absent)} does not sit beside the "
                           f"spec, so §{MATRIX_HEADING.split()[0]} would be checked against "
                           f"a partial tree"))
        return

    # `read_tags` reports the two rules that need no matrix — a tag on a non-test,
    # a tag that does not parse — so they run either way.
    tagged = read_tags(repo, out)
    matrix = s.matrix
    if not matrix:
        # §8.11 did not parse, and `check_traceability` has already said so. Every
        # rule below compares a tag against a row, so with no rows they would
        # report one invented finding per tag and bury the real one.
        return

    for ident, sites in sorted(tagged.items()):
        row = matrix.get(ident)
        if row is None:
            _cls, site, _ignored, lineno = sorted(sites)[0]
            path, name = site.split("::")
            out.append(Finding("tag-unknown-id", path, lineno,
                               f"`{name}` claims to cover {ident}, which has no matrix row",
                               repo_relative=True))
            continue
        lineno, classes, status, _ = row
        for cls, site, _ignored, _tagline in sorted(sites):
            if cls not in classes:
                out.append(Finding("tag-class-mismatch", CONFORMANCE_DOC, lineno,
                                   f"`{site}` covers {ident} as {cls}; the matrix files "
                                   f"{ident} under {', '.join(sorted(classes))}"))
        if status == "U":
            out.append(Finding("tag-status-understated", CONFORMANCE_DOC, lineno,
                               f"{ident} is marked unchecked, but {len(sites)} test(s) "
                               f"claim it — promote the row or drop the tag"))

    check_tag_filing(tagged, out)

    # A note that names a test by hand is a promise the tag has to keep, and the
    # promise is per invariant: a test tagged for one row does not back another
    # that happens to cite it.
    backs = {(ident, site.split("::")[1])
             for ident, sites in tagged.items() for _c, site, _i, _l in sites}
    tests, words = test_fns(repo), tree_words(repo)

    for ident, (lineno, _classes, status, note) in sorted(matrix.items()):
        named = NOTE_NAME_RE.findall(note)

        cited = {n for n in named if n in tests}
        missing = sorted(n for n in cited if (ident, n) not in backs)
        if missing:
            out.append(Finding("tag-missing", CONFORMANCE_DOC, lineno,
                               f"{ident}'s note names {', '.join('`'+n+'`' for n in missing)}, "
                               f"which carry no `Covers` tag for {ident}"))

        # A name that is neither a test nor anything else the tree spells has
        # been renamed or deleted. That is the direction that matters — a row
        # still claiming evidence that no longer exists — and it passed silently
        # because the name was dropped before anything compared it.
        stale = sorted({n for n in named if n not in tests and n not in words})
        if stale:
            out.append(Finding("note-stale", CONFORMANCE_DOC, lineno,
                               f"{ident}'s note names {', '.join('`'+n+'`' for n in stale)}, "
                               f"which nothing under {' or '.join(n + '/' for n in TREE_ROOTS)} "
                               f"defines — a row cannot claim a test that is not there"))

        if status not in ("C", "P"):
            continue
        if ident not in tagged:
            out.append(Finding("tag-unbacked", CONFORMANCE_DOC, lineno,
                               f"{ident} is marked {status} on prose alone — no test claims "
                               f"it, so nothing recomputes the status"))
        elif all(skipped for _c, _s, skipped, _l in tagged[ident]):
            out.append(Finding("tag-data-backed-only", CONFORMANCE_DOC, lineno,
                               f"{ident} is marked {status}, but every test tagged for it is "
                               f"`#[ignore]`d — MG-3's portable job would not notice it break"))

    check_evidence_counts(s, matrix, tagged, out)


def check_tag_filing(tagged, out: list):
    """The directory a test sits in against the classes it claims.

    Class is written twice — once in the `ctN_` directory, once in the tag — and
    two claims nothing reconciles drift. A test may pin several classes and live
    in any of them; what it may not do is sit in a class it never claims.
    """
    claimed, where = defaultdict(set), {}
    for sites in tagged.values():
        for cls, site, _ignored, lineno in sites:
            claimed[site].add(cls)
            where[site] = min(where.get(site, lineno), lineno)

    for site, classes in sorted(claimed.items()):
        rel, name = site.split("::")
        if not rel.startswith(CONFORMANCE_DIR):
            continue
        m = re.match(r"ct(\d+)_", rel[len(CONFORMANCE_DIR):].split("/")[0])
        if not m:
            continue
        home = f"CT-{m.group(1)}"
        if home not in classes:
            out.append(Finding("tag-class-misfiled", rel, where[site],
                               f"`{name}` sits under {home}'s directory but claims only "
                               f"{', '.join(sorted(classes))} — move it to a class it pins, "
                               f"or tag it for {home}",
                               repo_relative=True))


def check_evidence_counts(s: Suite, matrix, tagged, out: list):
    """The hand-typed split of §8.11's covered rows into tagged and prose-only.

    The totals line is recomputed; this sentence was not, so the one number that
    says how much of the matrix is machine-checked was itself an opinion.
    """
    covered = [i for i, row in matrix.items() if row[2] in ("C", "P")]
    if not covered:
        return
    backed = sum(1 for i in covered if i in tagged)
    skipped = sum(1 for i in covered
                  if i in tagged and all(ig for _c, _s, ig, _l in tagged[i]))


    body = re.sub(r"\s+", " ", s.nofence.get(CONFORMANCE_DOC, ""))
    check_data_backed_count(s, body, backed, skipped, out)

    m = EVIDENCE_RE.search(body)
    stated = f"{len(covered)} rows at C or P, {backed} backed by a tagged test, " \
             f"{len(covered) - backed} on prose alone"
    if not m:
        out.append(Finding("trace-evidence-stale", CONFORMANCE_DOC,
                           s.find_line(CONFORMANCE_DOC, "rest on"),
                           f"no `Of the n rows at **C** or **P**, n are backed by a tagged "
                           f"test and n rest on prose alone` sentence — the matrix has "
                           f"{stated}"))
        return
    if (int(m.group(1)), int(m.group(2)), int(m.group(3))) != \
            (len(covered), backed, len(covered) - backed):
        out.append(Finding("trace-evidence-stale", CONFORMANCE_DOC,
                           s.find_line(CONFORMANCE_DOC, "rest on"),
                           f"the stated split is {m.group(1)} rows, {m.group(2)} backed, "
                           f"{m.group(3)} on prose; the matrix has {stated}"))


def check_data_backed_count(s: Suite, body, backed, skipped, out: list):
    """The count of covered rows whose only evidence a portable job skips."""
    anchor = s.find_line(CONFORMANCE_DOC, "are backed only by tests marked")
    m = DATA_BACKED_RE.search(body)
    if not m:
        out.append(Finding("trace-evidence-stale", CONFORMANCE_DOC, anchor,
                           f"no `n of the n are backed only by tests marked` sentence — "
                           f"{skipped} of the {backed} tagged rows are"))
        return
    if (int(m.group(1)), int(m.group(2))) != (skipped, backed):
        out.append(Finding("trace-evidence-stale", CONFORMANCE_DOC, anchor,
                           f"the stated count is {m.group(1)} of {m.group(2)}; the matrix "
                           f"has {skipped} of {backed}"))


def check_matrix_totals(s: Suite, counts, out: list):
    """The two hand-typed numbers derived from the matrix, against the matrix.

    MG-1 ratchets on `P-COV-PROPERTY`. A ratchet whose baseline is typed by hand
    is a ratchet anyone can loosen by editing a sentence, so both the totals line
    and the registry's observed cell are recomputed here rather than trusted.
    """
    total = sum(counts.values())
    if not total:
        return
    fraction = counts["C"] / total

    body = s.nofence.get(CONFORMANCE_DOC, "")
    m = TOTALS_RE.search(body)
    if not m:
        out.append(Finding("trace-totals-stale", CONFORMANCE_DOC, 1,
                           f"no `**Totals: n C, n P, n U** of n` line — the matrix states "
                           f"{counts['C']} C, {counts['P']} P, {counts['U']} U of {total}"))
    else:
        stated = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        actual = (counts["C"], counts["P"], counts["U"], total)
        if stated != actual:
            out.append(Finding("trace-totals-stale", CONFORMANCE_DOC,
                               body[:m.start()].count("\n") + 1,
                               f"totals say {stated[0]} C, {stated[1]} P, {stated[2]} U of "
                               f"{stated[3]}; the rows say {actual[0]} C, {actual[1]} P, "
                               f"{actual[2]} U of {actual[3]}"))

    for lineno, line in enumerate(s.nolines.get(PARAMETERS_DOC, []), 1):
        if "P-COV-PROPERTY" not in line:
            continue
        cells = cells_of(line)
        if len(cells) < 4:
            continue
        m = COV_OBSERVED_RE.search(cells[2])
        if not m:
            out.append(Finding("param-observed-stale", PARAMETERS_DOC, lineno,
                               f"`P-COV-PROPERTY` observed reads `{cells[2]}`; it is "
                               f"derived from the matrix and must read "
                               f"`{fraction:.2f} ({counts['C']} of {total})`"))
            break
        stated = (m.group(1), int(m.group(2)), int(m.group(3)))
        if stated != (f"{fraction:.2f}", counts["C"], total):
            out.append(Finding("param-observed-stale", PARAMETERS_DOC, lineno,
                               f"`P-COV-PROPERTY` observed says {stated[0]} "
                               f"({stated[1]} of {stated[2]}); the matrix says "
                               f"{fraction:.2f} ({counts['C']} of {total})"))
        break


def harness_statuses(s: Suite, out: list):
    """HC-n -> its build status in the register, and the register's line span."""
    hstart, hend = s.section_body(CONFORMANCE_DOC, HARNESS_HEADING)
    if hstart is None:
        out.append(Finding("section-missing", CONFORMANCE_DOC, 1,
                           f"no `## {HARNESS_HEADING}` section — the checks that read it "
                           f"cannot run, and their silence would look like a pass"))
        return {}, (None, None)
    status = {}
    for i in range(hstart, hend):
        line = s.nolines[CONFORMANCE_DOC][i]
        m = BOLD_DEF_RE.match(line)
        if not m or not m.group(1).startswith("HC-"):
            continue
        cells = cells_of(line)
        if len(cells) < 3:
            out.append(Finding("trace-row-malformed", CONFORMANCE_DOC, i + 1,
                               f"capability row has {len(cells)} columns; the table "
                               f"declares Capability, Needed by, Status and Note"))
            continue
        status[m.group(1)] = re.sub(r"[*\s]", "", cells[2])
    return status, (hstart, hend)


def check_gates(s: Suite, defs, classes, registry, out: list):
    hc_status, (hstart, hend) = harness_statuses(s, out)

    start, end = s.section_body(CONFORMANCE_DOC, GATES_HEADING)
    if start is None:
        out.append(Finding("gate-no-capability", CONFORMANCE_DOC, 1,
                           f"no `## {GATES_HEADING}` section — nothing says what a change "
                           f"must pass"))
        return
    lines = s.nolines[CONFORMANCE_DOC]
    gated_classes = set()
    for i in range(start, end):
        line = lines[i]
        if not line.startswith("|") or line.startswith("|---"):
            continue
        cells = cells_of(line)
        gate = re.findall(r"\bMG-\d+\b", cells[0] if cells else "")
        if not gate:
            continue
        gate = gate[0]
        if len(cells) < 5:
            out.append(Finding("trace-row-malformed", CONFORMANCE_DOC, i + 1,
                               f"{gate}'s row has {len(cells)} columns; the table declares "
                               f"Gate, Threshold, When, Enforced by and Blocking?"))
            continue
        threshold, enforcer, blocking = cells[1], cells[3], cells[4]
        if not PARAM_RE.search(threshold) and not ID_RE.search(threshold):
            if re.search(r"\d", threshold):
                out.append(Finding("gate-threshold-literal", CONFORMANCE_DOC, i + 1,
                                   f"{gate}'s threshold `{threshold}` is a literal — give "
                                   f"it a P-* symbol and a row in {PARAMETERS_DOC}"))
        for prm in PARAM_RE.findall(threshold):
            if prm not in registry:
                out.append(Finding("param-undefined", CONFORMANCE_DOC, i + 1,
                                   f"{gate} names {prm}, which has no registry row"))
        capabilities = re.findall(r"\bHC-\d+\b", enforcer)
        if not capabilities:
            out.append(Finding("gate-no-capability", CONFORMANCE_DOC, i + 1,
                               f"{gate} names no HC-n capability — a gate nothing enforces "
                               f"is a wish"))
        # §8.12: "A gate is advisory while the capability it needs is unbuilt."
        # Nothing cross-joined the two tables, so the rule held only by hand.
        # Anything short of **C** counts: promoting a capability from U to P is
        # a note about progress, not a machine that runs, and reading only U let
        # such a promotion silently disarm this rule for every gate citing it.
        if "blocking" in blocking.lower() and "advisory" not in blocking.lower():
            unbuilt = [c for c in capabilities if hc_status.get(c) != "C"]
            if unbuilt:
                out.append(Finding("gate-blocking-unbuilt", CONFORMANCE_DOC, i + 1,
                                   f"{gate} is blocking but rests on "
                                   f"{', '.join(unbuilt)}, not built in "
                                   f"§{HARNESS_HEADING.split()[0]} — mark it advisory "
                                   f"until the capability exists, or build it"))
        for c in re.findall(r"\bCT-\d+\b", cells[1] + cells[2]):
            gated_classes.add(c)
            if c not in classes:
                out.append(Finding("gate-unknown-class", CONFORMANCE_DOC, i + 1,
                                   f"{gate} names test class {c}, which is not in the "
                                   f"taxonomy"))

    for c in sorted(classes):
        if c not in gated_classes:
            doc, lineno = defs[c]
            out.append(Finding("ct-ungated", doc, lineno,
                               f"{c} is reachable from no merge gate — say which gate runs "
                               f"it, or that it runs outside CI"))

    if hstart is None:
        return
    # Everything except the register itself. The build order in §8.14 is the only
    # prose citing several capabilities, so a corpus that stops at the register
    # reports them orphaned.
    corpus = [t for d, t in s.nofence.items() if d != CONFORMANCE_DOC]
    corpus.append("\n".join(lines[:hstart] + lines[hend:]))
    body = "\n".join(corpus)
    for ident, (doc, lineno) in sorted(defs.items()):
        if not ident.startswith("HC-"):
            continue
        if not re.search(rf"\b{ident}\b", body):
            out.append(Finding("hc-orphan", doc, lineno,
                               f"{ident} is needed by no test class and no merge gate — "
                               f"machinery nobody asked for"))


def check_decisions(s: Suite, refs, out: list):
    for doc in sorted(s.text):
        if not doc.startswith(DECISIONS_DIR + "/"):
            continue
        m = re.search(r"^Status:\s*(\S+)", s.text[doc], re.M)
        if not m or not m.group(1).startswith(ADR_STATUSES):
            out.append(Finding("adr-bad-status", doc, 1,
                               "needs a `Status: Proposed | Accepted (date) | Superseded "
                               "by ADR-n` line"))
        ident = re.search(r"^#\s*(ADR-\d+)", s.text[doc], re.M)
        if not ident:
            continue
        cited = [d for d, _ in refs.get(ident.group(1), []) if d in NORMATIVE]
        if not cited:
            out.append(Finding("adr-uncited", doc, 1,
                               f"{ident.group(1)} is cited from no normative document — "
                               f"an unreferenced decision is a smell"))


def check_normative_shape(s: Suite, registry, out: list):
    manifest = s.root.parent / "Cargo.toml"
    # `path`, `optional`, `features` and friends are TOML keys inside a
    # dependency's own table, not dependency names. Taking them as names makes
    # the word "path" leakage in every spec ever written.
    TOML_KEYS = {"path", "version", "features", "optional", "default-features",
                 "workspace", "git", "branch", "rev", "tag", "package", "registry"}
    deps = set()
    if manifest.is_file():
        in_deps = False
        for line in manifest.read_text(encoding="utf-8").split("\n"):
            if line.strip().startswith("["):
                in_deps = "dependencies" in line
                continue
            if in_deps:
                name = line.split("=")[0].strip()
                if len(name) > 3 and name not in TOML_KEYS \
                        and re.fullmatch(r"[a-z][a-z0-9_-]+", name):
                    deps.add(name)
    # A dependency whose name is also an ordinary English word (`bytes`, `arrow`)
    # is only leakage when it is written as code. Distinctive names — anything
    # carrying a hyphen, underscore or digit — leak in prose too.
    distinctive = [d for d in sorted(deps) if re.search(r"[-_0-9]", d)]
    plain = [d for d in sorted(deps) if d not in distinctive]
    # Each alternative carries its own boundary. A shared `(?<![\w/])` cannot
    # work for both a path prefix and a file extension: a stem ends in a word
    # character, so the lookbehind fails at the dot and the pattern never fires.
    alts = [r"(?<![\w/])(?:src|tests)/[\w./-]*",
            r"\b[\w-]+(?:\.[\w-]+)*\.(?:rs|yaml|toml)\b"]
    alts += [rf"\b{re.escape(d)}\b" for d in distinctive]
    alts += [rf"`{re.escape(d)}`" for d in plain]
    leak_re = re.compile("|".join(alts))

    for doc in sorted(NORMATIVE & set(s.text)):
        for lineno, line in enumerate(s.nolines[doc], 1):
            for m in leak_re.finditer(line):
                out.append(Finding("impl-leakage", doc, lineno,
                                   f"normative text names `{m.group(0)}` — say what the "
                                   f"mechanism does, not what implements it"))

    for doc in sorted(NO_BARE_CONSTANTS & set(s.text)):
        for lineno, line in enumerate(s.nolines[doc], 1):
            if NON_NORMATIVE_LINE.match(line):
                continue
            m = BARE_CONST_RE.search(line)
            if m:
                out.append(Finding("param-bare-constant", doc, lineno,
                                   f"`{m.group(0).strip()}` is a bare constant — give it a "
                                   f"P-* symbol and a row in {PARAMETERS_DOC}"))

    lines = s.nolines.get(INVARIANTS_DOC, [])
    for i, line in enumerate(lines):
        m = INV_DEF_RE.match(line)
        if not m:
            continue
        j = i + 1
        while j < len(lines) and not INV_DEF_RE.match(lines[j]) and not lines[j].startswith("## "):
            j += 1
        if not re.search(r"^\*Test:\*", "\n".join(lines[i:j]), re.M):
            out.append(Finding("inv-missing-check", INVARIANTS_DOC, i + 1,
                               f"{m.group(1)} states no `*Test:*` line — say how a harness "
                               f"would falsify it"))

    # A ⚠ must route to a decision or a gap. Inside the registry that is the
    # whole rule — a target is a proposal until an ADR accepts it. Elsewhere a ⚠
    # may instead name the parameter whose registry row carries the routing,
    # which is not circular only because the row is in another document.
    unratified_params = set()
    for line in s.nolines.get(PARAMETERS_DOC, []):
        if "⚠" in line:
            unratified_params.update(PARAM_RE.findall(line))
    route_re = re.compile(r"\bADR-\d+\b|\bGAP \d+\b|\bProposed\b")

    for doc in sorted(set(s.text) - {README_DOC}):
        # A decision is where a ⚠ routes *to*. One written inside an ADR is that
        # ADR discussing the marker, not a value waiting on a decision.
        if doc.startswith(DECISIONS_DIR + "/"):
            continue
        lines = s.nolines[doc]
        # A document's preamble declares its conventions — including what a ⚠
        # means. The marker being defined is not a marker in use.
        legend_end = next((i for i, ln in enumerate(lines)
                           if HEADING_RE.match(ln) and ln.startswith("## ")), 0)
        for lineno, line in enumerate(lines, 1):
            if "⚠" not in line or lineno <= legend_end:
                continue
            if doc == PARAMETERS_DOC:
                # A registry row routes through its own section: the row names
                # the gap, or the prose under the heading names the decision.
                scope = enclosing_section(lines, lineno - 1)
                routed = bool(route_re.search(scope))
            else:
                scope = line if line.lstrip().startswith("|") else \
                    enclosing_paragraph(lines, lineno - 1)
                routed = bool(route_re.search(scope)) or \
                    any(p in scope for p in unratified_params)
            if routed:
                continue
            out.append(Finding("warn-unratified", doc, lineno,
                               "⚠ marks an unratified value but routes nowhere — name the "
                               "ADR or gap that owns it"))


def check_gaps(s: Suite, out: list):
    body = s.text.get(GAPS_DOC)
    if not body:
        return
    lines = s.lines[GAPS_DOC]
    summary, bodies = {}, set()
    for lineno, line in enumerate(lines, 1):
        m = re.match(r"^\|\s*(\d+)\s*\|", line)
        if m:
            summary.setdefault(int(m.group(1)), lineno)
        m = re.match(r"^###\s+(\d+)\.", line)
        if m:
            bodies.add(int(m.group(1)))
    for num, lineno in sorted(summary.items()):
        if num not in bodies:
            out.append(Finding("gap-missing-body", GAPS_DOC, lineno,
                               f"gap {num} is in the summary table but has no `### {num}.` "
                               f"section"))


def check_bands(s: Suite, defs, out: list):
    by_band = defaultdict(list)
    for ident in defs:
        m = re.fullmatch(r"INV-([A-Z])(\d+)", ident)
        if m and m.group(1) in INV_BANDS:
            by_band[m.group(1)].append(int(m.group(2)))
    for band, nums in sorted(by_band.items()):
        nums.sort()
        for a, b in zip(nums, nums[1:]):
            for missing in range(a + 1, b):
                out.append(Finding("id-number-gap", INVARIANTS_DOC, 1,
                                   f"INV-{band}{missing} is unused between INV-{band}{a} "
                                   f"and INV-{band}{b} — retire it explicitly or reuse the "
                                   f"number now, before anything cites it"))


def check_freshness(s: Suite, out: list):
    for doc in sorted(MUTABLE & set(s.text)):
        dates = sorted(set(DATE_RE.findall(s.nofence[doc])))
        if len(dates) > 1:
            out.append(Finding("date-inconsistent", doc,
                               s.find_line(doc, dates[0]),
                               f"states several as-of dates ({', '.join(dates)}) — one "
                               f"document, one date"))
        elif not dates:
            out.append(Finding("date-inconsistent", doc, 1,
                               "states no as-of date — a document that records current "
                               "state has to say when it was current"))

    def commit_time(rel_path):
        try:
            r = subprocess.run(
                ["git", "log", "-1", "--format=%cI", "--", str(s.root / rel_path)],
                capture_output=True, text=True, timeout=10, cwd=s.root.parent)
            return r.stdout.strip() or None
        except Exception:
            return None

    # The matrix is stale when a contract document was committed after it.
    #
    # Both sides are committer timestamps from the same history, so a rebase, an
    # amend or a squash-merge moves them together. Comparing against a date typed
    # into the prose cannot work: `%cs` is rewritten by all three, so the check
    # fires on any branch that outlives its own as-of date — including, when this
    # was written that way, the commit that introduced the check. Satisfying it
    # meant typing the date of a commit not yet made.
    #
    # The mutable documents are excluded: the matrix records coverage of the
    # contract, so it goes stale when the contract moves, not when a parameter
    # is retuned.
    bar = commit_time(CONFORMANCE_DOC)
    if bar is None:
        return  # no git, or no history — skip rather than fail
    matrix_line = s.find_line(CONFORMANCE_DOC, MATRIX_HEADING)
    for doc in sorted((NORMATIVE - MUTABLE) & set(s.text)):
        d = commit_time(doc)
        if d and d > bar:
            out.append(Finding("stale-matrix", CONFORMANCE_DOC, matrix_line,
                               f"{doc} was committed after {CONFORMANCE_DOC} — re-check "
                               f"the statuses against it and re-date the matrix"))


IGNORE_FILE = ".speccheck-ignore"


def load_ignores(root: Path):
    """`check | file-glob | message-regex` per line -> (rules, usage errors).

    Suppressions on a blocking gate are a standing invitation, so this is as
    narrow as it can be and still be useful: a rule names one real check, never
    a wildcard, and a malformed rule is a usage error rather than a line quietly
    skipped. `main` reports how many findings each run suppressed; a run that
    suppressed anything never prints `clean`.
    """
    path = root / IGNORE_FILE
    rules, errors = [], []
    if not path.is_file():
        return rules, errors
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
        line = raw.split("#")[0].strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 3 or not all(parts):
            errors.append(f"{IGNORE_FILE}:{lineno}: expected "
                          f"`check | file-glob | message-regex`")
            continue
        if parts[0] not in CHECKS:
            errors.append(f"{IGNORE_FILE}:{lineno}: `{parts[0]}` is not a check "
                          f"(a wildcard is not accepted; run --list-checks)")
            continue
        try:
            pattern = re.compile(parts[2])
        except re.error as exc:
            errors.append(f"{IGNORE_FILE}:{lineno}: bad message regex: {exc}")
            continue
        rules.append((parts[0], parts[1], pattern))
    return rules, errors


def ignored(finding, rules):
    from fnmatch import fnmatch
    for check, glob, pattern in rules:
        if check == finding.check and fnmatch(finding.path, glob) \
                and pattern.search(finding.message):
            return True
    return False


def main(argv=None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("spec_dir", nargs="?", default="spec")
    ap.add_argument("--format", choices=("text", "json", "github"), default="text")
    ap.add_argument("--severity", choices=("error", "warning", "info"), default="warning",
                    help="lowest severity to *report* (default: warning)")
    ap.add_argument("--only", action="append", default=[], metavar="CHECK")
    ap.add_argument("--gate", choices=("error", "warning", "info"), default="error",
                    help="lowest severity that *fails* the run (default: error). "
                         "Independent of --severity, which only decides what is printed.")
    ap.add_argument("--strict", action="store_true", help="alias for --gate warning")
    ap.add_argument("--no-ignore", action="store_true")
    ap.add_argument("--list-checks", action="store_true")
    args = ap.parse_args(argv)

    if args.list_checks:
        width = max(len(c) for c in CHECKS)
        for name, (sev, desc) in sorted(CHECKS.items(), key=lambda kv: (SEV_ORDER[kv[1][0]], kv[0])):
            print(f"{SEV_NAME[sev][0].upper()}  {name:<{width}}  {desc}")
        return 0

    for c in args.only:
        if c not in CHECKS:
            print(f"unknown check: {c}", file=sys.stderr)
            return 2
    root = Path(args.spec_dir)
    if not root.is_dir():
        print(f"not a directory: {root}", file=sys.stderr)
        return 2

    rules, ignore_errors = ([], []) if args.no_ignore else load_ignores(root)
    if ignore_errors:
        for e in ignore_errors:
            print(e, file=sys.stderr)
        return 2

    s = Suite(root)
    if not s.files:
        print(f"no .md files under {root}", file=sys.stderr)
        return 2

    out = []
    defs = collect_definitions(s, out)
    refs = check_references(s, defs, out)
    registry = check_parameters(s, out)
    check_links(s, out)
    check_document_map(s, out)
    classes = check_traceability(s, defs, out)
    check_test_tags(s, out)
    check_gates(s, defs, classes, registry, out)
    check_decisions(s, refs, out)
    check_normative_shape(s, registry, out)
    check_gaps(s, out)
    check_bands(s, defs, out)
    check_freshness(s, out)

    selected = [f for f in out if not args.only or f.check in args.only]
    suppressed = sum(1 for f in selected if ignored(f, rules))
    # What gates is decided before the display filter. Taking the counts after it
    # meant `--severity error` hid warnings from `--strict`, and no flag
    # combination could ever gate on an info finding.
    gating = [f for f in selected if not ignored(f, rules)]

    floor = SEV_ORDER[{"error": "E", "warning": "W", "info": "I"}[args.severity]]
    findings = [f for f in gating if SEV_ORDER[f.sev] <= floor]
    findings.sort(key=Finding.key)

    if args.format == "json":
        print(json.dumps([{"check": f.check, "severity": SEV_NAME[f.sev], "file": f.path,
                           "line": f.line, "message": f.message} for f in findings], indent=1))
    elif args.format == "github":
        for f in findings:
            level = {"E": "error", "W": "warning", "I": "notice"}[f.sev]
            prefix = "" if f.repo_relative else f"{root}/"
            print(f"::{level} file={prefix}{f.path},line={f.line},"
                  f"title={f.check}::{f.message}")
    else:
        for f in findings:
            print(f"{f.path}:{f.line}: {SEV_NAME[f.sev]}: [{f.check}] {f.message}")
        counts = {k: sum(1 for f in findings if f.sev == k) for k in "EWI"}
        total = len(s.files)
        note = f", {suppressed} suppressed by {IGNORE_FILE}" if suppressed else ""
        if findings:
            below = len(gating) - len(findings)
            hidden = f", {below} below --severity {args.severity}" if below else ""
            print(f"\n{counts['E']} error(s), {counts['W']} warning(s), "
                  f"{counts['I']} info across {total} files{hidden}{note}")
        elif gating or suppressed:
            # Never the word "clean" while findings stand. A silenced run that
            # reads like a passing one is the whole hazard of a suppression file,
            # and a run hiding a backlog behind a display threshold is the same
            # mistake in a milder form.
            below = f", {len(gating)} below --severity {args.severity}" if gating else ""
            print(f"nothing at or above {args.severity}: {total} files, {len(defs)} "
                  f"IDs, {len(registry)} parameters{below}{note}")
        else:
            print(f"clean: {total} files, {len(defs)} IDs, {len(registry)} parameters")

    gate = "warning" if args.strict else args.gate
    gate_floor = SEV_ORDER[{"error": "E", "warning": "W", "info": "I"}[gate]]
    return 1 if any(SEV_ORDER[f.sev] <= gate_floor for f in gating) else 0


if __name__ == "__main__":
    sys.exit(main())
