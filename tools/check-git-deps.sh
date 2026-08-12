#!/usr/bin/env sh
# The distribution gate: every git dependency is either absent, or recorded in
# tools/allowed-git-deps.toml together with the crates it stops us publishing.
#
# WHY THIS EXISTS AT ALL. A git dependency is invisible to every other check
# this project runs — it compiles, tests, lints and benchmarks exactly like a
# registry one — and only announces itself at `cargo publish`, long after the
# design has hardened around it. Worse, the two ways of writing one differ in
# a way that is easy to get backwards:
#
#   git + version  -> `cargo publish` SUCCEEDS and SILENTLY STRIPS the git
#                     source. The uploaded crate depends on the registry
#                     version instead, compiles, and behaves like upstream.
#                     Whatever the fork existed to provide is simply absent,
#                     and nothing warns at any point.
#   git, no version -> `cargo publish` REFUSES, before building anything.
#
# So the form that looks more careful is the one that ships wrong code. This
# check treats it as never allowlistable.
#
# Members come from `cargo metadata`, not from the manifest's `members` list:
# a path dependency inside the workspace is a member whether or not anyone
# listed it, and a check that reads only the list would wave through a git
# dependency in one — which is the precise silent pass this exists to prevent.
# The git/version DETECTION reads raw manifests, because `cargo metadata`
# normalises a git dependency's version requirement away.
#
# Prerequisites: python3 >= 3.11 (stdlib tomllib) and cargo. Builds nothing.
set -eu

TOOLS_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$TOOLS_DIR/.." && pwd)

if ! python3 -c 'import tomllib' 2>/dev/null; then
    echo "git-deps: python3 with tomllib (3.11+) is required" >&2
    exit 2
fi

cd "$REPO_ROOT"
cargo metadata --format-version 1 --all-features 2>/dev/null > /tmp/rdlt-git-deps-meta.json || {
    echo "git-deps: cargo metadata failed" >&2
    exit 2
}

python3 - "$REPO_ROOT" /tmp/rdlt-git-deps-meta.json <<'PY'
import json, pathlib, sys, tomllib

repo = pathlib.Path(sys.argv[1])
meta = json.load(open(sys.argv[2]))
allowlist_path = repo / "tools" / "allowed-git-deps.toml"
findings = []

# ---- members, authoritatively (implicit ones included) ----------------------
members = {p["id"]: p for p in meta["packages"] if p["id"] in set(meta["workspace_members"])}
by_name = {p["name"]: p for p in members.values()}
publishable = {p["name"] for p in members.values() if p.get("publish") != []}

# ---- git-sourced packages in the resolved graph -----------------------------
git_pkgs = {p["id"]: p for p in meta["packages"] if (p.get("source") or "").startswith("git+")}

# ---- blast radius: publishable members reaching a git package via non-dev ----
nodes = {n["id"]: n for n in meta.get("resolve", {}).get("nodes", [])}

def reaches(start, target, seen):
    if start in seen:
        return False
    seen.add(start)
    for dep in nodes.get(start, {}).get("deps", []):
        kinds = [k.get("kind") for k in dep.get("dep_kinds", [{}])]
        # A dev edge does not survive into the manifest cargo uploads, so it
        # does not propagate unpublishability. Normal and build edges do.
        if all(k == "dev" for k in kinds):
            continue
        if dep["pkg"] == target or reaches(dep["pkg"], target, seen):
            return True
    return False

radius = sorted(
    name for name, pkg in by_name.items()
    if name in publishable and any(reaches(pkg["id"], gid, set()) for gid in git_pkgs)
)

# ---- raw manifest scan: the git/version pairing metadata hides --------------
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")

def scan_table(manifest_rel, table_name, table):
    for dep, spec in (table or {}).items():
        if not isinstance(spec, dict) or "git" not in spec:
            continue
        yield {
            "manifest": manifest_rel, "table": table_name, "dep": dep,
            "git": spec["git"], "rev": spec.get("rev"),
            "moving": spec.get("branch") or spec.get("tag"),
            "version": spec.get("version"),
        }

declared = []
manifests = [repo / "Cargo.toml"] + [
    pathlib.Path(p["manifest_path"]) for p in members.values()
]
for path in dict.fromkeys(manifests):
    doc = tomllib.load(open(path, "rb"))
    rel = str(path.relative_to(repo))
    for t in DEP_TABLES:
        declared += list(scan_table(rel, t, doc.get(t)))
    declared += list(scan_table(rel, "workspace.dependencies",
                                doc.get("workspace", {}).get("dependencies")))
    for key in ("patch", "replace"):
        section = doc.get(key, {})
        entries = section if key == "replace" else {
            k: v for reg in section.values() for k, v in reg.items()
        }
        for hit in scan_table(rel, key, entries):
            hit["patch_like"] = True
            declared.append(hit)

# ---- allowlist --------------------------------------------------------------
allowed = []
if allowlist_path.exists():
    allowed = tomllib.load(open(allowlist_path, "rb")).get("allow", [])
matched = set()

def fail(title, body):
    findings.append(f"{title}\n{body}")

for hit in declared:
    where = f"  {hit['manifest']} [{hit['table']}] {hit['dep']}"
    if hit.get("patch_like"):
        fail("PATCH/REPLACE with a git source -- publishes silently",
             f"{where}\n"
             "  redirects a dependency to git WITHOUT any `git` key in a member's\n"
             "  dependency table. Local builds use the fork; a published crate\n"
             "  resolves the registry. Same end state as carrying `version`.\n"
             "  Remove it and declare the dependency directly.")
        continue
    if hit["version"] is not None:
        fail("MODE A -- publishes SILENTLY against the registry",
             f"{where}\n"
             f"  carries BOTH `git` and `version = \"{hit['version']}\"`.\n\n"
             "  `cargo publish` SUCCEEDS and deletes the git source. The crate that\n"
             "  reaches users depends on the REGISTRY version, compiles, and behaves\n"
             "  like upstream -- whatever the fork provides is absent. Nothing warns.\n\n"
             f"  Would silently affect: {', '.join(radius) or '(none resolved)'}\n\n"
             "  There is no allowlist for this. Choose:\n"
             "    the fork      -> delete `version`; publishing then fails loudly\n"
             "                     and you record the consequence in\n"
             "                     tools/allowed-git-deps.toml\n"
             "    upstream      -> delete `git` (and any `rev`/`branch`/`tag`)")
        continue
    if hit["moving"]:
        fail("MOVING GIT REFERENCE -- builds are not reproducible",
             f"{where}\n"
             f"  is pinned to `{hit['moving']}`, which moves under `cargo update`.\n"
             "  Pin `rev` to an exact revision instead.")
        continue
    entry = next((a for a in allowed if a.get("dependency") == hit["dep"]
                  and a.get("manifest") == hit["manifest"]), None)
    if entry is None:
        fail("MODE B -- publishing is blocked, and the block is unrecorded",
             f"{where}\n"
             "  is a git dependency with no `version`, and has no entry in\n"
             "  tools/allowed-git-deps.toml.\n\n"
             "  `cargo publish` REFUSES for every crate that reaches it:\n"
             f"    {', '.join(radius) or '(none resolved)'}\n\n"
             "  That is the SAFE failure -- loud rather than silent. Record it:\n"
             "  the reason the fork is required, the exit that retires it, and the\n"
             "  crates it renders unpublishable.")
        continue
    matched.add(id(entry))
    if entry.get("git") != hit["git"]:
        fail("ALLOWLIST STALE -- git URL", f"{where}\n  records {entry.get('git')!r}, manifest says {hit['git']!r}")
    if entry.get("rev") != hit["rev"]:
        fail("ALLOWLIST STALE -- revision",
             f"{where}\n  records {entry.get('rev')!r}, manifest says {hit['rev']!r}.\n"
             "  Every fork bump becomes a diff line beside the recorded reason.")
    if sorted(entry.get("unpublishable", [])) != radius:
        fail("ALLOWLIST STALE -- blast radius",
             f"{where}\n"
             f"  records {sorted(entry.get('unpublishable', []))}\n"
             f"  graph says {radius}\n"
             "  The recorded consequence is a claim this check verifies, not a\n"
             "  comment. A dependency that spread to a new crate says so here.")

for entry in allowed:
    if id(entry) not in matched:
        fail("ALLOWLIST DEAD ENTRY -- the guard has gone stale",
             f"  {entry.get('dependency')} in {entry.get('manifest')} is recorded but no\n"
             "  longer declared. Delete the entry; a guard nobody notices is\n"
             "  expired is the failure this check exists to prevent.")

if findings:
    print("git-deps: FAIL\n")
    for f in findings:
        print(f + "\n")
    print(f"git-deps: {len(findings)} finding(s). Rules: tools/check-git-deps.sh header.")
    sys.exit(1)

n = len(declared)
print(f"git-deps: OK ({n} git dependenc{'y' if n == 1 else 'ies'}, all recorded)")
PY
