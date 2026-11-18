---
title: "RAGNART -- v0 implementation log"
created_at: 2026-11-13--14-04
created_by: Claude Code (/brain pipeline -- Claude Opus 5.5 orchestrating, Claude Haiku 5.5 implementing)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- section pointers updated after the as-is rewrite); Claude Code (Claude Opus 5.5, 2026-11-14 -- design review session entry, v0.2 handover); Claude Code (Claude Haiku 5.5, 2026-11-15 -- bridge session entry, v0.2 handover open items); Claude Code (Claude Fable 5.2, 2026-11-15 -- workflow design session entry); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 fix-first item 5 session entry); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session A entry); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session B entry; decisions as an unlabelled numbered list, session-uuid rule cites `docs/RAGNART-design.md` §14 D11; both point counts stated); Claude Code (Claude Haiku 5.5, 2026-11-16 -- v0.2 session C entry, the closing session; v0.2 closed; `## v0.2` heading added above `## v0.1`; v0.2 session B, A, fix-first item 5 and bridge subsections moved under `## v0.2`; workflow design session subsection moved under `## v0.2`); Claude (Claude Fable 5.2, claude.ai design-review session with the operator, 2026-11-16 -- v0.3 section opened with the design review entry); Claude Code (Claude Opus 5.5, 2026-11-17 -- design discussion entry: the notebook model, from what to how); Claude Code (Claude Opus 5.5, 2026-11-18 -- follow-up entry: persistent `cd` after `init`, session relocation open, how the agent searches, no session state, `session:current`, open, capture hook, collateral, resume and compaction, the web as a scope)
updated_at: 2026-11-18--12-34
context: >
  Per-session log for RAGNART's v0 train (v0.1 spin-off through v0.5 tenancy),
  mirroring AYA's docs/v0--Implementation-log.md format. The plan is
  docs/Implementation-plan.md; this log is the narrative record of how each
  version was actually reached.
---

# RAGNART -- v0 implementation log

## 0. How this document works

**Reverse chronological throughout. Newest on top, at every level.**

- One `##` section per version (`v0.1`, `v0.2`, ...). The newest version sits above
  the older ones.
- One `###` subsection per coding session, titled with the session timestamp in
  `YYYY-MM-DD--HH-MM` format.
- Each session subsection carries **What was done** (with commit hashes),
  **Deviations from the plan**, and -- for a session that closes a version --
  **Decisions**, **Evidence**, **Open items** and a **Handover** block.

**Citation convention.** A bare "Section x" means this document. `Plan Section x` is
`docs/Implementation-plan.md`. The v0.1 session that wrote this entry is the only
session of v0.1; `Plan Section 6 v0.1` means the v0.1 work-item list in that document.

**Log, not status.** The status table is `docs/Implementation-plan.md` Section 9. This
log records what happened, including what diverged.

---

## v0.3 -- Sources

Plan: `docs/Implementation-plan.md` Section 6, v0.3. Status: **planned** (see Plan Section 9).

### 2026-11-18 -- design follow-up: the session moves into the project folder it creates; how the agent searches; no session state; open, capture, collateral, resume; the web as a scope

Short follow-up with the operator in Claude Code (Claude Opus 5.5), documentation only.

| # | Decision | Design |
| --- | --- | --- |
| 1 | After `ragnart init`, the creating session moves its shell into the project folder with a persistent `cd` and stays there. Only `init` runs in a subshell. This replaces the subshell per command of the 2026-11-17 entry | D38, §7.3 |
| 2 | The operator's preferred behaviour is a real relocation: the session continues as a Claude Code session of the project folder (its `.claude/` state, memory and transcript there, resumed from there). It is recorded as an open item to verify; the persistent `cd` is the behaviour until then | §13.3 |
| 3 | The agent searches by a fixed request template in the `/ragnart` skill, as `/rag` does today. What matters is not the mechanism but two properties: the request comes from the session's state, and the verdict (`good_hits`, the echoed scope) comes from the API and is copied, never judged by the model. D31 reworded from "printed by the command" to "copied from the response". A `ragnart search` verb only if the template proves unreliable. Today's `/rag only` rule (score below 0.45) is wrong for hybrid search, where `score` is a rank-fusion value | D31, §8.8 |
| 4 | There is no separate store of session state. The session's own data belongs to its harness; project settings are in `project.toml`; the current project is the session's reference in the project, marked `working = true`, found by location or through the hub. A change of scope or grounding lasts for the conversation only, then the project's defaults apply. The open item "where a session's state lives" is closed | D38, §4.5, §8.4 |
| 5 | `ragnart add session:current` adds the running session; the CLI reads the session's id from the harness, so the model never types it. The mechanism is specific to each harness. Claude Code: `CLAUDE_CODE_SESSION_ID` in the agent's shell equals the transcript uuid (verified with Claude Code 2.1.326). OpenCode: the shell tool sets `OPENCODE_SESSION_ID` for every agent command, after plugin hooks (source, since v2.0.19; installed v2.0.28); undocumented, so checked at upgrades. octmux passes nothing: the agent's commands run under the OpenCode server | §7.3, §13.3 |
| 6 | `/ragnart open <name>` lets a session with no current project enter an existing ragnart project: the creation sequence without `init`, behind the same gate. Only a project on a source of the session's own host | §7.3, D38 |
| 7 | Capture is triggered by a hook on the harness's fetch tool (Claude Code `PostToolUse` on `WebFetch`; OpenCode `tool` / `execute.after`), detached, not by the model remembering. The skill instructs the model that searching and reading the web is a particular kind of tool use: always through the harness's search and fetch tools, never `curl` or a shell command, so the hook sees every page. Other routes stay best-effort | §7.3, deviation H, D9 |
| 8 | Collateral: the agent writes Markdown into `collateral/` with its own file tools and the skill commits; a presentation or chart sits beside its Markdown source; the model lists `sources:` in the front matter and the first commit records their content hashes in `.ragnart/provenance/` for the staleness check; working files stay outside `collateral/` | §7.3 |
| 9 | Resume and compaction: the hook restores the current project, the project folder (after a resume the skill moves the shell back), the default scope and grounding, the project listing, and re-injects a short block of the librarian rules. The working reference is found by a local lookup among the host's registered project folders, not through the hub | §7.4, §4.5 |
| 10 | Scope is where evidence may come from and now includes `web`, combinable with the RAG terms and resolved by the client through the harness's tools; grounding is only whether an answer may go beyond the evidence (`open` adds the model's own knowledge). The two are different concepts but not fully orthogonal: the web under grounding bundled two widenings and made "only sources, with the web as a source" inexpressible; scope `none` with `strict` is nonsensical for asking | §8.1, D4, D4d |

A `cd` moves only the shell. Claude Code binds the transcript, the per-project `.claude/` state,
the `CLAUDE.md` files loaded at start and the resume listing to the directory the session started in.

### 2026-11-17 -- design discussion: the notebook model, from what to how

Design discussion with the operator in Claude Code (Claude Opus 5.5), documentation only. Starting
question: how individual `.ragnart/` directories are told apart, when several ragnart projects serve
one topic (ACME-Q1, ACME-Q2). The session read `docs/RAGNART-design.md` and the plan and compared
them with the v0.2 code. The operator then reversed the order of the discussion: first what a
ragnart project is (a self-hosted NotebookLM notebook), then git as the model for how it is built.
Each decision below was proposed and agreed one at a time. No code was changed, and nothing was run
against the live services.

#### What was done

Commit: the docs commit that adds this entry.

- All current documents: "central" renamed "the hub" (D42).
- **`docs/RAGNART-design.md`**: §4.5 rewritten (what a ragnart project is, `init <name>`, content,
  `.ragnart/` as state, references, sessions, registration, keys of managed files, moves, copies);
  §1.1, §1.2 (git table, deviations B, D, E, F, H, I), §2.3, §4.2, §4.3, §4.6, §6.1, §6.3, §7.1 to
  §7.5, §8.1, §8.8, §10.3, §12, §13.3, §13.4; new §10.6 (concurrency); D2, D2b, D3, D9, D10, D12,
  D15, D19, D21, D27, D28, D29 and others amended, D32 to D42 added.
- **`docs/Implementation-plan.md`**: §2 decisions, §5 access model, the v0.3 registry item, v0.4
  rewritten, the v0.5 uploads, §7 Terms; §8 item 19 updated and item 21 removed.
- **`README.md`**: the Transient tier and the definition of "nested".

#### Decisions

| # | Decision | Design |
| --- | --- | --- |
| 1 | A ragnart project is a notebook: many per topic, material added and curated, librarian answers, collateral. A topic is a tag, never a folder | D27 |
| 2 | `ragnart init <name>` creates the folder `<name>` with `.ragnart/` inside it, and refuses an existing folder. The current directory has no meaning | D32, D10 |
| 3 | A ragnart project's material is only what it manages: the managed folders `inbox/`, `resources/`, `collateral/`, beside `.ragnart/`. Every other file of the folder is external (git: untracked). A hand-made marker adopts nothing | D32, D27 |
| 4 | Nesting has no meaning: no parent, no inclusion in scope, no inherited grant | D32, D2 |
| 5 | `.ragnart/` holds state only: what would be lost if the folder were copied and every database were empty. `project.toml` is the operator's; RAGNART writes `provenance/` and `dropped/`, one file per entry (references: revised by decision 15) | D33 |
| 6 | References by stable id are the rule, for ragnart projects and sessions. Files are the exception: snapshotted into `resources/`. Live or frozen is decided per kind | D34, D28 |
| 7 | Sessions stay `global` items; ingestion is unchanged. The session-start hook references a session started in a project folder; any session can be referenced explicitly. `launch_dir` is metadata only | D34, D15 |
| 8 | Membership by location in `resources/` and `collateral/`: a file placed by hand belongs to the project and was to be picked up by the next sync (revised by decision 20) | D36 |
| 9 | Exclusion rules and ragnart projects are mutually exclusive: no `.ragignore` inside the managed folders; admission rules apply | D35 |
| 10 | `add` copies and commits synchronously; `add --no-commit` stages; `commit` indexes what is there. Commits coalesce per project; every file gets an outcome | D36 |
| 11 | The races of concurrent writers are documented with their rules (R1 to R14) | §10.6 |
| 12 | `init` registers the project folder; the crawl remains a safety net; `ragnart repair` re-finds a folder moved by hand | D37 |
| 13 | Managed files are keyed `ragnart://<project id>/<relative path>`; a move changes locators only | D37 |
| 14 | A move to another source is explicit, by `ragnart mv`; the hub rewrites `source_id`, nothing is re-embedded | D37, D19 |
| 15 | "Reference" replaces "ref" everywhere. A reference is a resource: a `.reference` file in `resources/`. There is no `refs/` directory and no separate reference command; `add` copies a file and references a session or a ragnart project | D34, D33 |
| 16 | `ragnart add <item>` adds to the current project only. The CLI has no `-C` option: it finds its project from the working directory, and the skill runs it in a subshell in the current project's folder. A path that holds a ragnart project is read for its `id`; only the `id` is stored | D38, D34 |
| 17 | A ragnart project is created from inside a session: the skill runs `ragnart init`, makes the new project the session's current project (scope `project`, the project's default grounding) and adds the session to it. `init` is unchanged and knows nothing about sessions | D38 |
| 18 | A session works in at most one ragnart project for its whole life; the skill refuses to create a ragnart project from a session that has one. Every `ragnart` action is scoped to the current project; acting on another is not permitted. Being referenced as material by other projects is separate | D38 |
| 19 | The global crawl does not enter a ragnart project's folder: a `.ragnart/` marker is, for the crawl, a `.ragignore` holding `*`. Files outside the managed folders, and `inbox/`, are not indexed. `.ragnart/` is never ingested | D39 |
| 20 | No implicit commit: a project's index changes only by `add`, `note`, `commit` and `drop`. This reverses "picked up by the next sync" of decision 8. Uncommitted changes are counted by the hook and the status line | D40 |
| 21 | Collateral stays in `collateral/` as the project's output. It becomes a resource only by `ragnart add`, which moves it | D36, D39 |
| 22 | A code repository is not a ragnart project. The RAGNART repository's hand-written marker is removed in the v0.4 migration; its files stay `global` | D41 |
| 23 | No marker changes what is indexed: a marker with an unknown `id` in a folder with indexed `global` items is refused | D41 |
| 24 | `ragnart init` is not a commit and creates nothing in Qdrant. It publishes the project record to the hub at once, so search knows the new project; if the hub is unreachable, `init` succeeds and the next commit or sync publishes | D10 |
| 25 | "Central" is renamed "the hub" in the current documents: the hub registry, a hub source. The ingestion side keeps its name. Earlier log entries keep the old word | D42 |
| 26 | Lifecycle: a copy in `resources/` may be edited and is re-indexed at the next commit; keep and the promotion of collateral move the file with its provenance; a hand-placed inbox file expires 30 days after its modification time; stale collateral is reported, never repaired | D36 |
| 27 | A commit removes what has left `resources/` and `collateral/` at once, by a drop or by hand, with no tombstone. It runs no deletion phase when a folder cannot be read or the `id` does not match. The 24-hour tombstone belongs to the global crawl | D36 |
| 28 | Collateral stays apart from resources because it is the project's own synthesis, not gathered evidence: marked by its folder, shown as derived, checked for staleness. Changes made by hand in the managed folders are picked up by the next commit, never by the scheduled run | D36, D40 |
| 29 | A ragnart project is addressed by its `name`, unique across the hub registry; the path of its folder is metadata and transient. Scope term `name:<name>`; `init` refuses a taken name | D2b, D12 |
| 30 | Where a session's state lives is specific to each harness and not defined: a design topic of its own, although a session's identity is already fixed. How a project action from another host reaches the project's home stays open | §13.3 |

#### Applied as consequences, then confirmed by the operator

These follow from the decisions above. They were written without being discussed one by one, and
the operator confirmed them at the end of the session.

- `inherit_access` is reserved and ignored, and a grant covers one ragnart project only (from
  decision 4).
- `ragnart init --from <project> <name>` starts a ragnart project from another with a new `id`; a
  folder copied by hand is refused as a duplicate id.
- An attached folder is a folder added with `ragnart add`, copied into `resources/`.
- A selection scope includes the sessions its ragnart projects reference, resolved by the API
  before the search.
- A folder that contains a ragnart project below it is refused by `add`, naming that project.
- The `<path>/` scope term stays: it selects ragnart projects by where their folders are, and
  implies no relation between them.

#### Not decided

- Where a session's state lives, which is specific to each harness (Claude Code, OpenCode), and
  whether the agent can read its own session id in each (a v0.4 prerequisite).
- The write path for project actions from a host other than the project's ingestion host.

#### Not changed

- **Code.** The v0.2 code follows the earlier model; the design marks each difference as "built
  today", and Plan Section 6 v0.4 lists the work.
- **`.ragnart/project.toml` of this repository.** It keeps `mode = "crawl"` until the v0.4 migration removes the marker (decision 22).
- **`docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md`, `docs/design-history.md`.** They are the
  operator's documents.

### 2026-11-16--17-47 to 2026-11-16--23-55 -- design review session: pipeline scope, ingestion requests, the notebook model, the librarian's guarantee

Design review with the operator (Claude Fable 5.2, in a claude.ai session, not Claude Code),
documentation only. Starting question: a checkpoint evaluation of the design and the concept, and
whether the design is over-architected. The session read `docs/RAGNART-design.md` and the plan in
full and spot-checked the code. It raised five items, and the operator and the session then settled
them one at a time. The purpose, in the operator's words, is to iterate until the mental model is
sound and the design document expresses it consistently. No code was changed, and nothing was run
against the live services.

#### What was done

Commit: the docs commit that adds this entry.

- **`docs/RAGNART-design.md`**: §4.1 rewritten (what a pipeline is, the two kinds of version
  change); §4.5 rewritten (the notebook model, membership, references, marker guards, `init`); §2.3
  and §12 (four ingestion requests, the listing carries the pipeline version); §4.4, §4.7 and §4.8
  (`pipeline_version` on chunks, connector rendering moved out of §5.1); §5 and §5.5 (retrieval is
  not part of `p1`); §6.1, §7.2 to §7.5 (an add always copies, no ingestion mode, references); §8.1,
  §8.3, §8.7, §8.8, §9.2 and §9.3 (the librarian's guarantee, the relevance verdict, calibration);
  deviation I in §1.2; open items in §13.3; D8, D10, D15 and D19 amended, D27 to D31 added.
- **`docs/Implementation-plan.md`**: §2 decisions, §4 invariants, the v0.3 and v0.4 work items and
  definitions of done, the Terms paragraph, the future-work version rule, and open decisions 19 to
  21.
- **`README.md`**: the definition of "nested", and the v0.4 and v0.5 roadmap rows.

#### Decisions

| # | Decision | Design |
| --- | --- | --- |
| 1 | A pipeline is what central does to a submitted item, from parsing to embedding. There is one, for every source. Retrieval is not part of it | D30 |
| 2 | The pipeline version is recorded per item, and central stamps it on every chunk. A change of parsing, admission or chunking is applied per item as sources re-submit. A change of embedding model is applied to the whole collection by central, and is never mixed | D30, D8 |
| 3 | The ingestion API has four requests per item: upsert, delete, change of key, change of metadata. The last two re-embed nothing | D19 |
| 4 | A session is assigned once to the closest enclosing ragnart project of its launch directory, and keeps it until that ragnart project is removed. `launch_dir` is metadata, not an identifier | D15 |
| 5 | A ragnart project is a notebook: the content of one `.ragnart/` directory. The directory that holds it is not the ragnart project, and the files beside `.ragnart/` are `global`. One directory holds one ragnart project. There is no `mode`; "crawl" names the static policy only | D27 |
| 6 | An add always copies the file into the ragnart project's `resources/`. The redundancy is accepted for robustness and simplicity | D28 |
| 7 | "Reference" replaces "link". References point at ragnart projects, widen scope and never access. A symbolic link is a filesystem construct and never creates a reference | D28 |
| 8 | A missing or malformed `project.toml` is guarded as an item is: last good record, or a 24-hour tombstone | D29 |
| 9 | `ragnart init` never changes what is indexed, by construction, and refuses a directory that already has a `.ragnart/` | D10 |
| 10 | The directory keeps the name `.ragnart`. `.ragnart-project` was considered and dropped, for the stutter in `.ragnart-project/project.toml` | D27 |
| 11 | The librarian's guarantee is split by who can keep it: the search API reports the evidence and its verdict, the client's command prints the status line, and staying within the evidence in free conversation is best-effort | D31 |
| 12 | `min_relevance` is applied by the API, with one global value. The labelled set includes questions that should not be answered, and `ragnart calibrate` finds `min_relevance` only | D31 |

#### Applied as proposed, for the operator to confirm

These were proposed in the session and not contested, but not confirmed in so many words.

- The pipeline item as a whole (decisions 1 and 2). The operator's questions were answered, and the
  session moved to the next item.
- References are not transitive.
- The form of `.ragnart/references`: one `id` per line, with the name in a comment.
- The client resolves scope `project` from the references in `GET /v1/projects`; the scope grammar of
  the API is unchanged.
- `mode` stays a reserved, ignored key, so that an existing `project.toml` does not gain a tag.

#### Not decided

- The evaluation also recommended a smaller v0.3 and building project mode before the ingestion
  API. That was not discussed. The order and the scope of the versions are unchanged.
- The open items are in `docs/RAGNART-design.md` §13.3: the mechanism of an embedding change,
  `ignored` items on a version change, a fix inside a version on another host, the spelling of the
  launch path, references to items, the command that adds a reference, attached folders.

#### Not changed

- **Code.** The v0.2 code follows the earlier model. The design document marks each difference as
  "built today", and Plan Section 6 v0.4 lists the work.
- **`docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md`, `docs/design-history.md`.** They are the
  operator's documents. `docs/RAG-strategy.md` describes the built behaviour and stays accurate
  until the code changes. Its table of state tables still calls `project_links` "links between
  projects".
- **`.ragnart/project.toml` of this repository.** It still carries `mode = "crawl"`,
  `grounding = "open"` and `capture = false`. The code reads `mode` today, so the file is left as
  it is.

---

## v0.2 -- Nested

Plan: `docs/Implementation-plan.md` Section 6, v0.2. Status: **done** (see Plan Section 9).

### 2026-11-16--01-31 to 2026-11-16--12-05 -- v0.2 session C: ragnart projects, scope, collapse, reconciliation; v0.2 closed

Run as a `/brain` pipeline (Claude Haiku 5.5 executing in Actor dispatches). Plan: Plan Section 6 v0.2,
the sync, scope, retrieval and reconciliation work items and the Definition of done. Third and last of
the v0.2 sessions (A, B, C). Closing session: v0.2 is done. Each phase was reviewed before the next began.

#### What was done

Commits: `64472db` (phase 1), `c7c4999` (phase 2), `49e03de` (phase 3), `7804877` (smoke test and
endpoint comparison send `scope=all`), and the docs commit that adds this entry.

- **Phase 1, `64472db` (sync and data):** ragnart project markers (`.ragnart/project.toml`) and attachment
  (`docs/RAGNART-design.md` §4.5); move detection, tombstones with a 24 h grace period, a purge log line,
  and plan-then-apply sync with `--dry-run` (§10.2); one sync call for all roots, including La-Familia
  (§14 D16); ingestion computes `project_id`.
- **Phase 2, `c7c4999` (retrieval):** scope grammar and required `scope` on `GET /v1/search`
  (`ragnart/scope.py`; §8.1; §14 D12); `GET /v1/projects` (§8.8); collapse of identical copies (§9.1).
  The two `/rag` command files are outside the repo and were updated to send `scope=all`.
- **Phase 3, `49e03de` (reconciliation):** reconciliation after the ingestion daemon in every scheduled run,
  under the run-wide lock (§10.3; §14 D17).
- **Tests:** 368 pass before, 517 pass after.

#### Deviations from the plan

- **Dry-run on a copy:** the dry-run runs on a copy of the state DB, because the scanners write.
- **Marker removal:** a ragnart project whose `project.toml` disappears is removed; its items then attach
  by the attachment rule (§14 D15).
- **Tools missed in phase 2:** the smoke test and the endpoint comparison had to send `scope=all`; the
  docs pass found this, not the phase 2 review. Fixed in `7804877`.
- **Search service DB access:** `docs/RAG-strategy.md` said the search service never opens the state DB.
  It now holds a read-only connection on the same host as the writer.
- **Scratch cleanup:** an Actor's scratch cleanup removed `/tmp/claude-555` (harness scratch). The operator
  confirmed that deleting it is expected cleanup.

#### Decisions

Recorded in `docs/RAGNART-design.md` §14 D12 to D17, with §4.5, §4.6, §8.1, §8.8 and §9.1.

1. **Scope grammar:** `scope` is a list of terms: `all`; `<path>` (exactly one ragnart project);
   `<path>/` (that project and those below it); `tag:<key>=<value>`. Terms are unioned. The parameter is
   required; a missing `scope` is HTTP 422, and so is a term that names no ragnart project. `project` is
   resolved client-side to the current ragnart project's path; `none` is client-side and never sent. `all`
   means global items (owner rule) plus all accessible ragnart projects. The response echoes the resolved
   scope. (D12)
2. **First ragnart project:** this repo, with a hand-written `.ragnart/project.toml` (name `RAGNART`, id
   `2b29e12b-01cb-43e7-bc7b-8abb2f092eae`, mode `crawl`, grounding `open`, capture off per the init
   invariant, D10). Its files and the sessions launched in it move from `global` into it. A malformed
   `project.toml` (bad TOML, missing name or id) is logged loudly and ignored; the directory stays outside
   ragnart projects. (§4.5)
3. **Move rule:** within one sync, exactly one vanished item and exactly one new item share a non-empty
   content hash. The empty-file MD5 and NULL never match. Any other case is a deletion plus a new item.
   A move rewrites the key in SQLite, the `source_path` payload of its chunks and its link rows; no
   re-embedding. Sessions never move. (D13)
4. **Tombstones:** a vanished item gets `missing_since`; its points stay searchable for 24 h. The first sync
   after 24 h purges its points and row and logs the purge (key, chunk count, reason, time). Reappearance
   clears `missing_since`. The mass-delete guard gates new tombstones, not purges. (D14)
5. **Plan-then-apply:** each sync computes moves, tombstones, purges and new items as a plan and logs it.
   `--dry-run` prints the plan and writes nothing; otherwise the plan is applied. (§10.2)
6. **Attachment:** re-evaluated on every sync for every completed item: the closest enclosing ragnart project
   by path, or for sessions the `launch_dir` by verbatim prefix match, else `global`. A change updates SQLite
   and the payload (`set_payload`). (D15)
7. **Scheduled sync:** covers all configured roots in one run, including La-Familia. `--user` remains for
   manual runs. (D16)
8. **Reconciliation:** runs after the ingestion daemon in every scheduled run, under the run lock. It fills
   NULL `chunk_count` from Qdrant; re-queues rows with fewer points than `chunk_count` (above zero); deletes
   points whose key has no row, under the point-count guard. `chunk_count` 0 with 0 points is consistent.
   Anomalies and a refused delete are logged as WARN; the run never fails. (D17; §10.3)
9. **Collapse:** after rerank, before `max_hits_per_source` and `top_k`. Only copies at other paths collapse.
   The empty-file hash and NULL never collapse. A collapsed hit lists every location. (§9.1)
10. **`GET /v1/projects`:** lists the `global` row first (no path), then the ragnart projects, with id, path,
    name, parent, owner, mode, tags and item count; filtered by `user` like search. (§8.8)

#### Evidence

- **Bench baseline:** 14 queries, top-5 sources, captured twice before any change; the two runs differed on
  1 of 14 (known non-determinism).
- **Snapshot pair:** 2026-11-16 10:25 UTC (3.0 GB Qdrant snapshot, 258 MB `rag_state.db`).
- **Sync dry-run on a DB copy:** 1 project upsert, 135 attachments, nothing deleted.
- **Real sync:** 135 items re-attached to RAGNART; its 2,559 points carry the project id; total points
  347,865, unchanged; 44/44 sample vectors identical.
- **Reconciliation, first run:** 433 chunk counts filled; 2 orphaned sources (307 points) deleted under the
  guard (limit 34,786); 0 anomalies.
- **Reconciliation after ingest:** 22 pending rows were ingested; a second check found zero differences
  (13,415 ok, 1,269 ok with zero chunks, 0 anomalies, 0 orphans).
- **Move test (throwaway ragnart project):** renaming its directory changed its `projects` row (path) and
  detected both files as moves (same point ids, `project_id` unchanged, `source_path` rewritten, total
  unchanged). Deleting it removed the project and tombstoned its 2 items; purge due about 2026-11-17 10:42 UTC
  by the scheduled sync.
- **Search:** missing `scope` and an unknown path -> 422; a scoped search returned only RAGNART files and
  sessions (10/10), with the per-source cap respected.
- **`/v1/projects`:** global 14,529 items; RAGNART 156.
- **Bench, `scope=all` vs baseline:** 12/14 identical; the 2 differences are identical copies, now collapsed
  into one hit with all locations.
- **First scheduled run with the new code (10:42 UTC):** success; reconciliation 0 anomalies, 0 orphans;
  snapshot correctly skipped.
- **Smoke test:** passed.

#### Open items

- **Moved ragnart project, sessions:** sessions launched in a ragnart project fall back to `global` when the
  project directory is moved, because `launch_dir` is verbatim. Operator decision pending.
- **Hit fields:** hits do not yet carry `project`, `source_type` or `modified_at` (`docs/RAGNART-design.md` §8.7).
- **Reconciliation alerts:** anomalies and a refused orphan delete only log a WARN; the systemd unit does not fail.
- **Unwritten tables:** `project_links`, `source_links` and `grants` are not written.
- **SoHoAI stub:** removal of SoHoAI's `/v1/rag/search` stub awaits the operator. The log check on 2026-11-16
  found no calls since 2026-11-14. The stub forwards without `scope`, so it would get 422 anyway.
- **Test items:** the purge of the two throwaway test items, due about 2026-11-17, is to be confirmed.

#### Handover to v0.3

- **Tree state:** RAGNART HEAD is the docs commit that adds this entry; its parent is `7804877`.
- **v0.3 scope (Plan Section 6 v0.3):** `ragnart init`; the `/ragnart` skill and commands (scope resolved
  client-side, `none` client-side, grounding); the session-start hook; the status line; capture and inbox.
- **Reserved keys:** the `project.toml` keys `grounding`, `capture` and `inbox_expiry_days` are reserved and
  read by the client (`docs/RAGNART-design.md` §4.5).
- **Keep in step:** the two `/rag` command copies (`~/.claude/commands/rag.md` and
  `~/.config/opencode/commands/rag.md`) live outside the repo. Any change to `scope` must update both.

### 2026-11-15--21-16 to 2026-11-15--23-42 -- v0.2 session B: baseline data model, Claude Code re-key, metadata backfill

Run as a `/brain` pipeline (Claude Haiku 5.5 executing in Actor dispatches). Plan: Plan Section 6 v0.2,
work items 1-4 delivered, items 5 and 8 rewritten. Second of three back-to-back `/brain` sessions for v0.2
(A, then B, then C). Not a closing session: v0.2 stays open.

#### What was done

Commits: `8a0f6b3` (baseline data model, Claude Code re-key, metadata backfill), `7cedd0c` (session-uuid
collision repair), and the docs commit that adds this entry.

- **Schema and state:** schema version 2 through `PRAGMA user_version`; per-item columns (including
  `origin_path` and `launch_dir`); tables `projects` (with the reserved `global` row), `project_tags`,
  `project_links`, `source_links`, `grants`.
- **Qdrant:** eight payload indexes (keyword: `source_path`, `project_id`, `owner`, `source_type`,
  `file_type`, `content_hash`; datetime: `modified_at`, `ingested_at`).
- **Claude Code re-key:** scanner keys `claude-session://` (`docs/RAGNART-design.md` §4.2); workflow journals excluded.
- **Backfill:** `utils/rag_metadata_backfill.py` (dry-run, prefix pilot, checkpoint, resumable, per-row failure
  handling); runbook in `docs/RAG-strategy.md` §5.7.
- **Decisions (v0.2 session B, recorded in the docs):**
  1. **Baseline data model:** items outside ragnart projects (today all items) follow the single-user RAG data model and retrieval, now and later.
  2. **`project_id`:** always present; `global` outside ragnart projects; one reserved `projects` row.
  3. **Access:** grants govern items in ragnart projects (v0.5); the owner rule governs `global` items, permanently.
  4. **No root or sessions projects:** a crawl root is a traversal start and owner mapping; launch directories recorded verbatim; no alias map.
  5. **Membership:** an item belongs to the closest enclosing ragnart project; a ragnart project's parent is the closest enclosing one.
  6. **Path selection:** not part of the baseline model and not scheduled; a path term in a `<selection>` names ragnart projects only.
  7. **`origin_path`:** the key for a NAS file, the `.jsonl` path for a Claude Code transcript, `NULL` for OpenCode.
  8. **Claude Code keys:** main transcript `claude-session://<uuid>`; subagent and workflow agent `claude-session://<uuid>/agent-<id>`; workflow journals excluded.
  9. **Metadata of existing items:** `modified_at` from the row's last mtime, `ingested_at` and `first_seen_at` from `completed_at`, `pipeline_version` `p1`, hash read once by the backfill; OpenCode `content_hash` NULL.
  10. **Pilot:** the backfill restricted by prefix, in three parts; a migration safety step, not a project.
  11. **Kickoff values:** checkpoint every 100 items; one `set_payload` per `source_path`; 10% delete guard; 24 h grace (session C); pilot first.
  12. **Duplicate copies (found during the live run):** the session-uuid rule (`docs/RAGNART-design.md` §14 D11): one session uuid is one item; the newest copy by mtime wins (then the greatest path); other copies are duplicate locations, not indexed.
- **Rejected alternatives:** root projects; per-owner `sessions` project or an unfiled project; `NULL` `project_id`;
  path selection via per-point ancestor lists; alias map for `fotel--NAS-home`; separate keys for diverged session copies.
- **Tests:** 300 pass before, 368 pass after.
- **Live evidence:**
  - Snapshot pair 2026-11-15 21:23 UTC (3.1 GB Qdrant snapshot, 258 MB `rag_state.db`).
  - Dry run: 14,681 candidates (9,915 NAS, 4,332 Claude Code, 433 OpenCode, 1 journal); 13.5 GB to hash.
  - Pilot: 167 rows (21 NAS, 88 Claude Code, 58 OpenCode), 0 errors; 36/36 sample vectors identical; datetime range query works.
  - Full run: 14,634 migrated; 46 re-key collisions (same uuid in two project directories: 35 identical, 9 prefix, 2 diverged).
  - Collision repair (session-uuid rule, `docs/RAGNART-design.md` §14 D11): 46 resolved; 38 sessions re-queued and re-ingested; 0 failed. Points 348,715 to 347,314 after the repair (the colliding keys' points deleted, 38 sessions re-ingested from their newest copy: net -1,401); 58/58 sample vectors identical.
  - Every point has `project_id` and the other new fields, except 307 points of two known zombie sources under `dot.claude-Linux` (no queue row; session C reconciliation).
  - Bench: the 14 queries against a temporary restore of the pre-migration snapshot, compared with the live collection: 12/14 identical top-5 source sets. The two differences are identical-content copies (d2l mxnet/pytorch/tensorflow variants; Dropbox and certifications copies of the same AWS file). The pre-migration collection itself varied between two runs of the same query (known non-determinism). Temporary collection deleted.
  - First sync with the new scanner deleted only the workflow journal row. Points 347,812 after that sync (new and changed transcripts, including this session's).
  - New ingestion writes `project_id`, `source_type`, `modified_at`, `ingested_at`, `content_hash` and `launch_dir`.
  - Search service restarted; timer resumed.

#### Deviations from the plan

- **Baseline bench not captured before the migration:** verified instead by restoring the snapshot into a temporary collection.
- **`launch_dir` column added (amendment B1):** beyond the Planner's draft.
- **Duplicate copies found during the live run:** the full run exposed the 46 collisions; the repair followed as a separate commit.
- **Failure evidence:** the systemd journal of the failed per-unit run is not available; the evidence is the re-run output.
- **Review fixes:** early-exit paths now set `pipeline_version` (a zero-turn Claude transcript regression test);
  NAS `.jsonl` files outside the Claude layout handled; empty Claude root guard added.

### 2026-11-15--19-28 to 2026-11-15--20-11 -- v0.2 session A: continuous worker pool, OpenCode backup, max_hits_per_source; v0.2 decisions

Run as a `/brain` pipeline (Claude Haiku 5.5 executing in Actor dispatches). Plan: Plan Section 6
v0.2, fix-first item 6, work item 11 and work item 9 (cap). First of three back-to-back `/brain`
sessions for v0.2 (A, then B: data model and backfill, then C: sync, scope, projects, collapse and
reconciliation). Not a closing session: v0.2 stays open.

#### What was done

Commits: `c4e5616` (code) and the docs commit that adds this entry.

- **Fix-first item 6, continuous worker pool:** `--workers` coroutines each claim the next pending row
  with `StateDB.claim_next_pending` (one SELECT and one UPDATE, no `await` between them) and run
  `ingest_file()` until no row is pending. `StateDB.fetch_pending_full` removed.
- **Work item 11, OpenCode backup:** `scripts/opencode-db-backup.sh` runs `sqlite3 .backup` into
  `RAGNART--databases/opencode-backups/<host>-<ip>/`, keeps 7 copies, takes no index lock. Units
  `ragnart-opencode-backup.{service,timer}` run at 03:30; `scripts/install-units.sh` installs and enables them.
- **Work item 9 cap:** `rag.search.max_hits_per_source` (default 3) applied after rerank, before top_k.
  `GET /v1/search` takes a per-request override (`ge=1`) and echoes the applied value. CLI flag
  `--max-hits-per-source` on `utils/rag_search_cli.py`.
- **Tests:** 290 pass before, 300 pass after.
- **Live evidence:** 3-worker ingest run of 13 items. Worker 2 held one long session transcript while
  workers 0 and 1 processed the other 12. No item processed twice, none left in `processing`
  (14,681 completed, 268 ignored). `/v1/search` echoes 3; `max_hits_per_source=1` gives distinct
  sources; `=0` gives HTTP 422. `install-units.sh` installed and enabled the backup timer (next run 03:30).
  One manual backup: 2.2 GiB in 21 s, `PRAGMA integrity_check` ok.
- **Decisions (v0.2, recorded in the docs):** settled kickoff values; `scope` required on `/v1/search`,
  with the two `/rag` commands changed in the same change; session attribution by physical-path match,
  with the per-owner `sessions` project as fallback; subagent keys; shaping order; `scope=none` handled
  client-side and never sent to the API; `/v1/projects` shape; project ids; empty `grants` means the owner rule applies;
  plan-then-apply defined; hashing seeded by the backfill.

#### Deviations from the plan

- **Collapse by `content_hash` moved to session C:** it needs `content_hash` in the payload.
- **Backup output world-readable on the first live run:** `umask 077` added before the code commit;
  the first copy chmod 600.
- **WAL copy checks:** `sqlite3 -readonly` on a WAL copy creates `-wal` and `-shm` side files, so
  troubleshooting uses the `immutable=1` URI.
- **SoHoAI 307 stub:** keeps its 2026-11-16 removal date (not waived).
- **Review follow-ups:** dead `fetch_pending_full` removed; CLI `>=1` check added; stale design §7.4
  citation in Plan Section 2 corrected to design §4.3 and §4.5.

### 2026-11-15--18-35 to 2026-11-15--19-19 -- v0.2 fix-first item 5: single-writer lock for every index writer

Run as a `/brain` pipeline (Claude Haiku 5.5 executing in Actor dispatches). Plan: Plan Section 6
v0.2, fix-first item 5 (widened). Not a closing session: v0.2 stays open.

#### What was done

Commits: `b88d4a4` (code) and the docs commit that adds this entry.

- **Research:** only the ingest daemon took a lock. It exited 0 on contention, wrote no holder and
  truncated the lock file. The other five writers took none. `fcntl.lockf` works on the NFSv4.1
  mount between Server 2 processes; `flock` and `lockf` conflict there. systemd prevents overlapping
  timer runs only.
- **`ragnart/lock.py`:** exit 75 (`EXIT_LOCK_HELD`, sysexits `EX_TEMPFAIL`) naming the holder; the
  holder line is written into the lock file; `run` and `held` subcommands; ancestor check
  (`held_by_ancestor`) so a child of a run-wide hold does not block on its parent; `rag.ingest_lock`
  is required, with no `/tmp` fallback.
- **Writers:** `utils/rag_sync_nfs.py`, `utils/rag_ingest_daemon.py`, `utils/rag_purge_rescan.py`
  (`--apply` only), `utils/rag_purge_corrupt.py` (`--confirm` only), `utils/rag_reset.py`
  (non-dry-run only). **Scripts:** `scripts/sqlite-qdrant-snapshot.sh` (self re-exec guard);
  `scripts/ragnart-ingest-run.sh` (run-wide hold).
- **Tests:** 290 pass, including `tests/test_lock.py` and one lock test per writer.
- **Live evidence:** with the lock held by `python -m ragnart.lock run -- sleep 25`, sync, daemon and
  snapshot each exited 75 naming the holder. `systemctl start ragnart-ingest.service` under the
  run-wide hold ended `Result=success` with 5 new items. The nested snapshot path was not exercised
  live (20 h rule).
- **Reviews:** code PASS; docs PASS.
- **Docs:** `docs/RAGNART-design.md` §10.3 and §13.2; `docs/Implementation-plan.md` fix-first item 5,
  §3 "One database host, one writer", the reserved `project.toml` keys and the §4.3 citation;
  `docs/RAG-strategy.md` §5.6; `docs/RAG-troubleshoot.md` §2.3; the `rag.ingest_lock` comment in
  `ragnart.yaml`.

#### Deviations from the plan

- **Item 5 widened** from the sync to every writer. Operator-approved.
- **Daemon contention exit changed** from 0 to 75 (`EXIT_LOCK_HELD`, sysexits `EX_TEMPFAIL`: a
  temporary failure that a retry can clear). Contention no longer reads as success.
- **Two plan corrections** against the refined design folded into this session: the reserved
  `project.toml` keys in `docs/Implementation-plan.md` go from 4 to the 8 of `docs/RAGNART-design.md`
  §4.5; the session-attribution citation goes from design §7.4 to design §4.3 (§7.4 is the v0.3
  harness section).
- **Starting log line moved** below the lock guard in `scripts/ragnart-ingest-run.sh`, so it does not
  print twice when the wrapper re-execs.

### 2026-11-15--18-30 -- design session: the project workflow

Design discussion with the operator (Claude Fable 5.2), documentation only. Starting question:
how an operator works with RAGNART to get the NotebookLM, librarian interaction, and how
material moves between the tiers. `docs/RAGNART-design.md` defined the tiers, the inbox, distill
and `MEMORY.md`, but not the workflow. No code was changed.

#### What was done

Commit: the docs commit that adds this entry ("docs: project workflow; log: workflow design
session").

- **`docs/RAGNART-design.md`**: §7.3 rewritten as "Project workflow"; §7.4 "Project context" and
  §7.5 "Transient lifecycle" defined; the `init` invariant in §4.5; §8 and §9 clarified; the
  inbox row and deviation H in §1.2; decisions D3 and D3b updated, D4d, D9 and D10 added; open
  items in §13.3 replaced.
- **`docs/Implementation-plan.md`**: v0.3 (goal, entry criteria, work items, definition of done,
  risks, decisions to confirm), the Terms paragraph, and the Transient expiry row (settled).
- **`README.md`**: the Transient tier row and the v0.3 roadmap row.

#### Decisions

| # | Decision |
| --- | --- |
| 1 | `ragnart init` behaves like `git init`. `.ragnart/` is the project; there is no mode to enter or leave. |
| 2 | Gather, work, distill and ask are activities, not workflow states. The only session state is scope and grounding; gather is grounding `open`. |
| 3 | The harness learns of a project through three parts: a global `/ragnart` skill, a session-start hook, and the status line. |
| 4 | The `ragnart` CLI is plumbing behind plain-language requests. The command prints the action line. |
| 5 | Capture is ambient, by the skill's policy, and best-effort. The CLI fetches the source itself. Recorded as deviation H from git. |
| 6 | The inbox is not vector-indexed (D3 stands). It expires after 30 days; a keep, or a listing by collateral, persists an item; a drop is undoable. |
| 7 | Material the operator adds skips the inbox and becomes a resource, in the global collection under `project_id`. A project-local vector store was rejected. |
| 8 | Distill produces collateral on demand, indexed as derived material (`source_type = collateral`). |
| 9 | `MEMORY.md` is removed with no replacement file. The catalogue is `ragnart ls-files` and `ls-files --others`. |
| 10 | The `init` invariant: `init` never changes what is indexed. An indexed folder gets `crawl`, `open`, capture off; an empty one gets `curated`, `strict`, capture on. |
| 11 | Default scope and grounding are tunable at three levels. `strict` governs every informational answer. `open` reaches the web as its outermost extent. |

#### Deviations from the plan

- **The v0.3 entry criterion "Transient lifecycle defined" is met by this session**, ahead of
  v0.2, and removed from Plan Section 6 v0.3.
- **v0.3's work items changed.** The `distill` and `inbox` commands and `MEMORY.md` are gone;
  `capture`, `drop`, `note`, the session-start hook and the status line are added.
- **Filled in while writing, without a separate discussion:** the command names `capture`, `drop`
  and `note`; the `.ragnart/dropped/` folder; `ragnart gc` deleting expired and dropped items;
  the `project.toml` keys `scope`, `grounding`, `capture` and `inbox_expiry_days`.

#### Open items

Recorded in `docs/RAGNART-design.md` §13.3:

- The write path for project actions from hosts other than Server 2 (the API is read-only).
- The OpenCode equivalent of the session-start hook (unverified).
- Where a session's scope and grounding are stored, for the status line.
- How a capture that is not Markdown records its origin.
- Video transcripts: capture has no transcript fetcher.
- Kotaemon as the view of a project's resources and inbox.

### 2026-11-14--10-00 to 2026-11-15--09-00 -- v0.2 fix-first items 0-4: corpus hygiene, .ragignore, sync guards, read-only CLI

Bridge session between v0.1 and v0.2, run as a `/brain` pipeline (Claude Haiku 5.5 executing
in Actor dispatches). Plan: Plan Section 6 v0.2, fix-first items 0-4, and the measured
baseline in Plan Section 3. Fix-first items 5 and 6 were not delivered (see Deviations).

#### What was done

Commits: `c41c08c` (code: admission rules, `.ragignore`, sync guards, read-only CLI, tests); the docs commit that adds this entry ("docs: p1 admission rules, git parallel, .ragignore, measured corpus; log: bridge session").

**Corpus before the cleanup** (2026-11-13 23:55): 2,988,440 points, 15,348 items.

| Class | Points | Share | Items |
| --- | ---: | ---: | ---: |
| Oversized (>= 5,000 points) | 1,348,315 | 45.1% | 25 |
| PDF/DOCX read as raw binary | 1,138,039 | 38.1% | 210 |
| File gone | 105,129 | 3.5% | -- |
| Large (1,000-4,999 points) | 85,759 | 2.9% | -- |
| Sessions | 114,189 | 3.8% | -- |
| Normal | 197,009 | 6.6% | -- |

Worst culprits: P4OC R8 mapping files (942,670 points), raw-binary PDFs (about 1.13M), CSV
dumps (about 300k), the duplicated `_AWS` subtree (about 171k), Cisco VPN logs (20k), build
output (about 35k), reveal.js decks (about 45k).

**Root causes and fixes** (`ragnart/ingest.py`, `ragnart/textquality.py`, `ragnart.yaml`):

| # | Cause | Fix |
| --- | --- | --- |
| R1 | docling CUDA OOM: GPU shared with llama-server and Ollama, 3 workers each with a converter; the failure fell back to a raw read | No raw fallback for PDF/DOCX; docling on CPU |
| R2 | Quality gate counted U+FFFD as printable | NUL and U+FFFD sniff; corrected gate |
| R3 | No size or chunk caps | 5 MB size cap; 5,000-chunk cap; both mark the item `ignored` with a reason |
| R4 | Gate applied only at (re)ingestion | `utils/rag_purge_rescan.py` re-checks existing rows against admission |
| R5 | Over-broad includes (`.txt`, `.csv`), mirrored trees, build directories | `.csv` removed from includes; `config/ragignore` and per-directory `.ragignore` |

**Operator actions:** deleted `OpenFlow-Book-Preview.pdf` and the duplicate `Gitlab--fotel/_AWS`;
renamed the P4OC mappings to `*.txt.norag`; removed offending PDFs; later deleted the large Java
books. Added `.ragignore` in `notebooklm-py` (`*.yaml`) and in `AWS--fotel/_AWS/billing` (`*`).

**Point counts:**

| When | Points | Event |
| --- | ---: | --- |
| 2026-11-14 00:27 | 2,988,440 | Start of cleanup |
| 2026-11-14 01:00 run | 1,719,988 | After operator deletions |
| 2026-11-14 14:30 | -- | Snapshot pair `2026-11-14-14-30-30` (rollback point) |
| Manual sync | 1,442,702 | 207 rows, 203 of them CSV |
| `rag_purge_rescan --apply` | 321,424 | 187 PDF/DOCX re-queued for re-parse; 1,067,181 points deleted; 86 marked `ignored` (54,097 points); 230 transient failures re-queued |
| Re-parse backlog done, 2026-11-15 00:28 | 391,919 | -- |
| 2026-11-15 08:54 | 348,163 | After the two operator `.ragignore` files (146 cassette YAML, 1 billing PDF) |

**Code delivered:**
- `ragnart/textquality.py`: text checks (NUL and U+FFFD sniff, quality gate).
- `ragnart/ragignore.py`: gitignore-semantics matcher; `git check-ignore` is the test oracle.
- `config/ragignore`, `config/ragignore.example`.
- `ragnart/cli.py`, `ragnart/__main__.py`, `ragnart/status.py`: `ragnart status`,
  `ls-files [<path>...]`, `check-ignore -v`. Read-only.
- `ragnart/ingest.py`: admission (docling on CPU, no raw fallback, NUL and U+FFFD sniff,
  5 MB size cap, 5,000-chunk cap, corrected gate).
- `ragnart/scanner.py`: `.ragignore` integration; `ragnart.yaml` exclusion keys removed.
- `ragnart.yaml`: `rag.admission` block, `rag.sync.max_delete_fraction`, `.csv` commented out.
- Fix-first items 1-4: skipped roots, owner-scoped deletion, `max_delete_fraction` with
  `--allow-mass-delete`, `wait=True`, `collection_exists` and alias refusal, `--dry-run`,
  `/health` with a 2 s check and 503 mapping.
- `scripts/ragnart-ingest-run.sh`: ingestion continues after a failed sync.
- `utils/rag_purge_rescan.py`: purge and rescan tool (dry-run by default).
- `pyproject.toml`: scripts entry, packages, `test` extra. `tests/`: 266 tests.
- Editable install of `ragnart` into the shared venv.

**Ops:**
- Ollama `OLLAMA_CONTEXT_LENGTH` 768 -> 2048, via the drop-in
  `/etc/systemd/system/ollama.service.d/ragnart-context-length.conf`. VRAM 0.83 -> 1.09 GB.
- Finding: Ollama rejects over-long inputs with HTTP 500; it does not truncate. This caused the
  earlier "500" ignored rows.

**Docs updated:** `docs/RAGNART-design.md` (§1.2 git parallel with deviations A-G; p1 made
explicit, including admission; version rule; D6-D8), `docs/Implementation-plan.md`,
`docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md`, `README.md`.

#### Deviations from the plan

1. **No "p2".** Operator: a version change is a major event; fixes stay `p1` (Plan Section 2).
2. **Chunk cap 2,000 -> 5,000** (operator). Real books exceeded 2,000 (python-in-a-nutshell
   2,146 chunks; AYA v1 log 2,167). Briefly 10,000, back to 5,000 after the operator deleted
   the large Java books.
3. **Pathspec not used.** Its matching disagreed with git. The own matcher is fuzz-tested
   against `git check-ignore` (2,000 random rule sets, 0 mismatches).
4. **Re-parse backlog.** The first run (3 workers) was stopped by Claude Code's low-memory
   reaper at 20:19 (docling about 16.7 GB RSS). Restarted as a systemd unit with 1 worker.
5. **Operator requests.** `ls-files` path argument and logical-`$PWD` resolution added;
   `config/ragignore.example` added.
6. **Batch barrier observed, not fixed.** The re-parse run showed the docling batch barrier
   (Plan Section 6 v0.2 fix-first item 6).
7. **Fix-first items 5 and 6 not delivered** in this session. Both are open (Handover to v0.2).

## v0.1 -- Spin-off

Plan: `docs/Implementation-plan.md` Section 6, v0.1. Status: **done** (see Plan Section 9).

### 2026-11-14--00-30 -- design review, design document, as-is docs, corpus analysis, v0.2 handover

Intermediate session between v0.1 and v0.2, run directly by Claude Opus 5.5 (with subagents
for the doc rewrites and independent reviews). Operator-led design review of RAGNART before
v0.2, producing `docs/RAGNART-design.md`.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `fd07805` | RAGNART | docs: describe the current state only (as-is rewrite) |
| `938fe7e` | RAGNART | plan: v0.2 fix-first items (NFS root guard, --user scope, alias, health) |
| `d44cc41` | RAGNART | docs: RAGNART design document; plan aligned with it |
| `f384b20` | RAGNART | README: roadmap and tiers aligned with the design document |

The commit carrying this entry also adds v0.2 fix-first item 0 and the version assignments
below to `docs/Implementation-plan.md` and the matching status markers to
`docs/RAGNART-design.md`.

1. **As-is documentation rule** (operator): descriptive docs describe only the current state;
   history lives in git and in this log. `docs/RAG-strategy.md` rewritten as a current-state
   reference (3,119 -> ~1,010 lines), `docs/RAG-troubleshoot.md` as a symptom/check/fix guide
   (1,661 -> ~712), plan and README given an as-is pass. Independent review: FIX, one item, fixed.
   Rule recorded in project memory; README keeps one origin reference; log files are exempt.
2. **Defects found in the code** while rewriting, verified and written into the plan as v0.2
   fix-first items 1-4: an unreachable NFS root deletes its tree; a `--user` sync deletes other
   owners' rows; `rag_status.py`/`rag_reset.py` ignore the `documents` alias; `/health` blocks
   and search stays 503 after Qdrant was down at start.
3. **Design review** (topics worked in order, decisions by the operator): identity (natural
   key + MD5 attribute, no surrogate id); schema version vs pipeline version, `p1` defined
   explicitly; payload metadata and filters, collapse of identical copies; everything is a
   project, grants to persons and groups, inheritance opt-out, roles read/admin, sharing TBD;
   projects addressed by path with immutable `id` and free tags; tiers by lifecycle with
   Transient as an ephemeral, non-indexed cache; project mode (gather -> compact -> references,
   `MEMORY.md` index); retrieval as scope x grounding with explicit-only changes; retrieval-only;
   predictable-not-rigid quality with calibration knobs; failure and consistency rules;
   `opencode.db` nightly backup. Full register: `docs/RAGNART-design.md` §14.
4. **Corpus analysis** (operator suspected pollution): facet over `source_path` plus file sizes.
   Of 2,988,440 points, 1,243,155 (41.6%) come from 211 PDF/DOCX read as raw bytes, 987,376
   (33.0%) from 15 very large text files (P4OC R8 mapping files, VPN logs), 362,643 (12.1%) from
   23 CSV exports; the real corpus is ~395k points. Root causes in `ragnart/ingest.py`: raw-read
   fallback on docling failure; quality gate defeated by U+FFFD counting as printable; no size or
   chunk caps; gate applied only at ingestion; static policy takes every txt/csv. Cleanup and
   `p2` handed to a separate operator session (the bridge to v0.2).
5. **Design document** `docs/RAGNART-design.md` written, independently reviewed (FIX: four
   present-tense statements about unbuilt parts, a table residual; all fixed with a status
   table and per-section status lines). Plan and README aligned with it; the handover in
   `~/Gin-AI/tmp` is no longer referenced.

#### Deviations from the plan

1. v0.3 changed from "Transient" (real-time `transient` collection, `/v1/transient`) to
   "Project mode" (CLI, `/ragnart` skill, compact, `MEMORY.md`, ingest-now; no second vector
   index), per design decision D3.
2. v0.2 drops `source_id` and gains: schema version, design §4.3/§4.4 metadata and indexes,
   re-keying of Claude Code sessions to `claude-session://<uuid>`, the `scope` parameter,
   `max_hits_per_source` and collapse, reconciliation, the `opencode.db` backup, and fix-first
   item 0 (corpus hygiene, delivered by the bridge session).
3. v0.3 gains retrieval calibration (`ragnart calibrate`, labelled set).

#### Evidence

- 19:00 ingestion run (2026-11-13): sync 1.5 min, 15 files ingested, 0 failed; snapshot
  correctly skipped ("newest pair is younger than 20 h"). The deleted
  `OpenFlow-Book-Preview.pdf` (105,116 points) was removed from disk at 00:09 on 2026-11-14,
  after that run; its points are removed by the 01:00 run.
- Corpus at the end of the session: 2,988,440 points (cleanup not yet started).

#### Handover to v0.2

- **Tree state:** RAGNART HEAD = the commit carrying this entry (parent `f384b20`); SoHoAI HEAD
  `95d5d0d`. Clean tree.
- **Read first:** `docs/RAGNART-design.md` §1.1 (what is built vs planned) and §14 (decision
  register); then `docs/Implementation-plan.md` §2–§5 and §6 v0.2 in full.
- **Prerequisites (entry criteria):** fix-first item 0 delivered by the bridge session and
  verified (point count and composition, `p2` in code and described in design §5, plan §3
  refreshed); SoHoAI `/v1/rag/search` 307 stub removed on or after 2026-11-16 after checking
  `SoHoAI-gateway.log` for 307 hits.
- **Decisions to take at kickoff:** plan §6 v0.2 "Decisions to confirm at kickoff" (sync scope,
  missing-sources threshold, grace period, backfill batch size and pilot, `max_hits_per_source`
  default, OpenCode backup retention).
- **Open design topics not needed for v0.2:** Transient lifecycle (before v0.3), interfaces
  (API versioning, CLI/skill/API split, Kotaemon fit), notifications, sharing and ownership
  transfer, Qdrant placement after the cleanup.
- **Environment:** Qdrant on `.93` (config `/etc/qdrant/config.yaml`, wired 1 Gb/s), search and
  ingestion on `.95`, data root `/mnt/nfs/__Backups/RAGNART--databases/`; privileged commands via
  `sudo -n` or `ssh root@localhost` (`.95`) and `ssh 192.168.1.93` + `sudo -n` (`.93`).
- **Open items tracked in `docs/Implementation-plan.md`** (Plan Section 6 v0.2, Plan Section 7 Future work): fix-first items 5 (sync lock) and 6 (batch barrier); reconciliation additions (2 zombie paths under `dot.claude-Linux`, 307 points; 1,270 `completed` rows without points; rescan crash window); future work (chunk sizing in bge-m3 tokens, page numbers, secret filter, unused `rrf_k`, merge `rag_purge_corrupt.py` into `rag_purge_rescan.py`, configurable docling device); 2 rows `ignored` with one-off errors ("All connection attempts failed", "[Errno 24] Too many open files") to re-queue.

### 2026-11-13--16-15 -- post-close ops: Qdrant config, snapshot retention, data root, Server 1 back on wired

Intermediate session between v0.1 and v0.2 (operator-requested re-evaluation). Run directly by
Claude Opus 5.5, not as a `/brain` pipeline. Not a change to v0.1's scope; it fixes defects found
after the close and sets the snapshot policy.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `6c30d2a` | RAGNART | ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases |

1. **Server 1 (`.93`) back on wired Ethernet.** Root cause of the snapshot slowdown, confirmed from
   three sources:
   - `.93` journal: on 2026-11-08 at 09:27:44 CET the Wi-Fi profile `inet5` was created, and at
     09:28:14 NetworkManager made it the default route. `eno1` had no carrier from the ~09:31
     reboot until 2026-11-13 15:13:32.
   - AYA `docs/v2--Implementation-log.md`, entry 2026-11-08--13-30: "`.93` moved to Wi-Fi on the
     operator's desk" (um690 became the Windows 11 VM host for AYA v2.4).
   - Snapshot log: about 530 runs (May to 2026-11-08 07:11 CET) downloaded in 1-4 min; every run
     from 2026-11-08 13:09 CET took 32-59 min. Three 07:00-slot runs exceeded the 3600 s curl
     limit (11-09 twice, 11-13 once), leaving the ~103 GB of orphans cleaned in v0.1.
   Before power-off: VM `um690-g1` shut down; `btusb` blacklist removed (backup
   `/root/aya-btusb-blacklist.conf.2026-11-13` on `.93`, initramfs rebuilt); `Wired connection 1`
   set to static `192.168.1.93/24`, `inet5` autoconnect off (profile backup
   `/root/nm-system-connections.2026-11-13`). GRUB default verified: plain Debian 7.3.3, no vfio.
   After boot: `eno1` 1000 Mb/s full duplex, 680M on `amdgpu`, BT up, 102 MB/s measured.
2. **Qdrant was running on built-in defaults.** `qdrant.service` on `.93` still pointed at
   `…/SoHoAI/scripts/qdrant/qdrant-config.yaml`, deleted by SoHoAI `95d5d0d`. At the 15:13 boot
   Qdrant logged "Config file via --config-path is not found" and started on defaults; it worked
   only because the default `./storage` under `WorkingDirectory=/var/lib/qdrant` is the same path.
   Fixed: config installed locally as `/etc/qdrant/config.yaml` by `scripts/qdrant/install-qdrant.sh`
   (never from NFS, which may not be mounted when Qdrant starts). Restarted 15:54 CET: no config
   error, 2,988,227 points, green.
3. **Snapshot location and retention** (`docs/RAG-strategy.md` §9.1):
   - `snapshots_path` is now `/var/lib/qdrant/snapshots` (Server 1 NVMe, transient). Before, it was
     the NAS, so every run wrote ~20 GiB to the NAS twice (Qdrant's archive, then the download).
   - One restore pair per day (snapshot when the newest NAS pair is older than 20 h; normally the
     01:00 run), 7 kept (~150 GB). Ingestion stays 4x/day. Safety nets: stale Qdrant-local
     snapshots and NAS `.tmp` deleted before each run, 60 GB NAS free-space guard, EXIT-trap
     cleanup, unpaired `rag_state` copies pruned.
4. **Clean transition.** Deleted: 109 GB of May 10-12 snapshots of the pre-sparse `documents`
   collection on `.93`'s NVMe (unrestorable); on the NAS, 9 of 12 current snapshot pairs (kept the
   newest per day: 11-11 18:07, 11-12 18:10, 11-13 12:10 UTC), 3 April/May `.checksum` files, 12
   May `rag_state` copies, the 2026-11-04 `rag_state-pre-rerun` backup, empty `qdrant/`,
   `qdrant-snapshots/documents_new`, `qdrant-snapshots/tmp`. NAS snapshot area 240 GB -> 61 GB.
5. **Data root split.** `/mnt/nfs/__Backups/SoHoAI--databases/{sqlite,logs,qdrant-snapshots,rag-ingest.lock}`
   moved (rename on the same NFS filesystem) to `/mnt/nfs/__Backups/RAGNART--databases/`.
   `SoHoAI--databases` keeps only SoHoAI's own `archive/` (old chat/telemetry DBs) and
   `SoHoAI-gateway.log` (written by `start-sohoai.sh`). All scripts, `ragnart.yaml`, utils defaults,
   logrotate and docs repointed.
6. **systemd / logrotate on Server 2.** `scripts/install-units.sh` (re-run as root): installs
   `/etc/logrotate.d/ragnart-ingest` with `su florian florian` (the old file had failed every night
   since 2026-05-06: "parent directory has insecure permissions"; log had grown to 228 MB, now
   rotated to 9 MB `.gz`), removes `/etc/logrotate.d/rag-ingest` and SoHoAI's
   `rag-ingest.service`/`.timer`.

#### Evidence

Verification run with `RAG_FORCE_SNAPSHOT=1`, 15:57:51-16:13:41 CET, exit 0, new data root:

| Step | Before (Wi-Fi, snapshots_path on NAS) | 2026-11-13 16:09 CET |
| --- | --- | --- |
| Ingest | -- | 28 files, 0 failed |
| Snapshot creation | 5-6 min | 44 s |
| 20 GiB download to NAS | 32-59 min | 3 min 21 s |
| Snapshot step total | ~40 min | 4 min 16 s |
| `.93` `/var/lib/qdrant/snapshots` after | -- | 0 bytes; Qdrant lists no snapshots |
| NAS pairs | 12 (240 GB) | 4/7 (data root 81 GB) |

#### Decisions

- Snapshots daily, 7 kept, instead of every run with 12 kept (operator: "balance availability with
  space"). Recovery point at most ~24 h; ingestion is idempotent.
- Data root split rather than a plain rename, so SoHoAI's gateway log path and archive stay valid.
- Qdrant config is a local file on Server 1, installed from the repo; the NFS copy is the source only.

#### Open items

- 19:00 timer run should skip the snapshot (newest pair 16:13, younger than 20 h); first scheduled
  daily snapshot on the 01:00 run of 2026-11-14.
- `redis-server` package still installed (disabled) on `.93`; unused, can be removed.
- Qdrant on `.93` vs `.95`: undecided; measure `.95` peak RAM first (Qdrant RSS is 15 GB).
- RAGNART `/health` hangs when Qdrant is down (no short client timeout) -- v0.2.

### 2026-11-13--14-04 -- v0.1 close

Single session, closing v0.1. It moved RAG code, config, service and ingestion from SoHoAI
into this repo, retired SoHoAI's `/v1/rag/search` handler to a 307 redirect stub, and ran
the two Definition-of-done checks. The session was run as a `/brain` pipeline with
Claude Opus 5.5 orchestrating and Claude Haiku 5.5 executing the steps.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `e4de8a3` | RAGNART | import: RAG code and docs from SoHoAI@5f4f1ed (plain copy, 32 files, sha256-identical to the source) |
| `1d702fe` | RAGNART | ragnart: adapt SoHoAI's RAG spin-off (package rename, ragnart.yaml, service module, systemd units, pyproject) |
| `796635f` | RAGNART | ragnart: add tie-aware old-vs-new endpoint comparison; align pins with venv |
| `95d5d0d` | SoHoAI | rag: spin off into RAGNART -- redirect stub, remove rag_engine/RAG utils/scripts, freeze RAG docs |

The docs commit that carries this entry cites only the code hashes above; its own hash is
recorded in `git log`, not here. Two RAGNART files that were not part of this session's
work were folded into the same docs commit: `README.md` (rewritten in a separate session)
and the avatar assets (`assets/ragnart-avatar.png`, `assets/ragnart-social-preview.png`,
and `assets/RAGNART-avatar.jpeg`, moved from the repo root).

Work by phase:

- **P1 -- import.** Byte-identical copy of `rag_engine/`, the RAG `utils/`, the RAG `scripts/`,
  and the two RAG docs from SoHoAI@5f4f1ed. Checked per file by sha256.
- **P2 -- adaptation.** `rag_engine` renamed to `ragnart`; `multi_query` and
  `utils/rag_mmr_bench.py` dropped; `ragnart.yaml` split out of `SoHoAI-config.yaml`
  (the RAG blocks only, no YAML anchors involved); about ten config-path sites switched
  to `ragnart.yaml`; `ragnart/service.py` added (`GET /v1/search`, `GET /health`);
  ingestion scripts and units renamed to `ragnart-*`; `scripts/install-units.sh` added;
  snapshot script given a 7200 s download timeout and a failure-cleanup trap;
  `pyproject.toml` and `.gitignore` added. Stale `ollama_url` fallbacks corrected to `.95`.
- **P3 -- live verification before cutover.** Manual uvicorn on `:8100`; `/health`
  returned `{"status":"ok","qdrant":true}`; `utils/rag_smoke_test.py` passed; the
  comparison tool was added and run (see Evidence). The first exact-ordered comparison
  failed; see Deviation 6.
- **P5 -- snapshot cleanup and cron.** Three orphaned Qdrant-local snapshots and three
  orphaned NAS `.tmp` files deleted (exact names only, re-listed and size-checked first).
  The 03:00 `sqlite-qdrant-snapshot.sh` crontab line removed; the crontab is now empty
  (backup kept in the session directory).
- **P4 -- cutover.** `scripts/install-units.sh` run as root at 13:00:23 CET. `ragnart-search.service`
  active on `:8100`; `ragnart-ingest.timer` enabled (next slot 19:00); `rag-ingest.timer`
  disabled. First RAGNART ingestion run 13:01-13:50 CET: NFS sync of 11773 files, 4263
  Claude sessions and 433 OpenCode sessions; 32 new files ingested, 0 failed; the
  20 GiB Qdrant snapshot downloaded in 34 min (13:16:13-13:50:40 CET), the Qdrant-local
  copy then deleted; Result=success; 12/12 snapshot pairs after rotation.
- **P6 -- client repoint.** Both `/rag` command files (`dot.claude-Linux/commands/rag.md`
  and `~/.config/opencode/commands/rag.md`) pointed at `http://192.168.1.95:8100/v1/search`
  (lines 16, 37, 76). Plain files, not under git.
- **P7 -- SoHoAI removal.** In SoHoAI: `main.py` gained a 307 stub for `/v1/rag/search`;
  the Qdrant client set-up, `rag_cfg` plumbing and `variant_llm_fn` removed from `main.py`;
  the RAG blocks removed from `SoHoAI-config.yaml`; 30 RAG tracked files removed
  (`rag_engine/`, `utils/rag_*`, RAG `scripts/`); doc banners on the two RAG docs;
  `CLAUDE.md`, `README.md` and `docs/design-history.md` updated. Single commit `95d5d0d`.
  The gateway reloader did not pick up the edits; the operator restarted it (Deviation 7).
- **P8 -- docs and close.** RAG docs corrected by name and path; `docs/design-history.md`,
  this log and the plan status updated; RAGNART `README.md` and avatar assets committed
  as described above. After review, two residual `~/Gin-AI/projects/SoHoAI` mentions in the carried-over RAG docs were fixed (RAG-strategy.md setup instruction repointed; RAG-troubleshoot.md gateway-reload note annotated).

#### Deviations from the plan

1. **`docs/Changelog.md` was not created.** Plan Section 7 planned it. This log, in the
   AYA format, replaces it from v0.1 onward. Recorded as a dated note in Plan Section 6
   v0.1 and Plan Section 7.
2. **`numpy` dropped from `pyproject.toml`.** Plan Section 6 v0.1 named it. After dropping
   `multi_query.py` and `utils/rag_mmr_bench.py`, nothing in the import graph imports
   `numpy` directly. Recorded in Plan Section 6 v0.1. All other pins in `pyproject.toml` were likewise aligned to the versions actually installed in the shared venv (`fastapi==0.135.1`, `qdrant-client==1.17.1`, `uvicorn==0.42.0`) instead of the plan's literal values; `python-pptx==1.0.2` was taken from the venv as planned.
3. **Snapshot work was not a named v0.1 item.** The download timeout fix (`--max-time`
   3600 to 7200 and a failure trap), the cleanup of six orphaned artefacts, and the
   removal of the 03:00 cron line were done here. They were required by this session's
   constraints: the 07:00 slot had failed with exit 28 on the 3600 s timeout, and the
   03:00 cron snapshot was redundant with the snapshot each ingestion run already takes.
4. **Cutover was run with passwordless sudo by the Actor, not by the operator.** Plan
   Section 6 v0.1 names the systemd move; the install script ran via `sudo -n` at 13:00:23
   CET, with `ssh root@localhost` as the fallback route. Passwordless sudo was
   timestamp-based and worked intermittently during the session.
5. **The SoHoAI `rag-ingest.timer` was stopped early by Brain.** At 12:49 CET Brain
   stopped it via root ssh, so the 13:00 SoHoAI run could not start while the cutover
   was pending. The timer stayed enabled until `install-units.sh` disabled it at 13:00:23.
6. **The exact-ordered old-vs-new gate was replaced by a tie-aware gate.** The first
   comparison failed on exact ordering (4/14, then 6/14 passing). The root cause is
   non-determinism on the old endpoint alone: Qdrant RRF breaks exact score ties
   inconsistently (OLD-vs-OLD, `rerank=false`: 5/14 identical orderings), and the
   reranker's `relevance_score` moves by about 1e-3 to 1e-2 with batch composition, which
   flips near-ties. The operator chose a tie-aware gate (see Decisions).
7. **The SoHoAI gateway was restarted by the operator, not reloaded.** The
   `uvicorn --reload` process on Server 1 did not reload on NFS edits, from Server 2 or
   from a local `touch`. The operator restarted it at 13:20 CET. Startup completed at
   13:24:56 CET, slowed by the concurrent 20 GiB snapshot write saturating the NAS. The
   307 stub was then verified live.
8. **The 307 stub stays in SoHoAI until 2026-11-16.** The plan's removal of the stub at
   the end of the transition was not done in this session. The operator set the date
   (originally 2026-11-27). Open item below.
9. **Extra SoHoAI config blocks removed.** Beyond the `rag:` block, the `users`, `shared`,
   `claude_chats`, `opencode` and `db_base_path` blocks were removed from `SoHoAI-config.yaml`,
   after a repo-wide grep found no remaining reader. The anchors used by `audio_models`
   and `proxy` (`server1_ip`, `server2_ip`) are kept.
10. **Commit trailers name the executing model.** The plan's trailers say Claude Opus 5.5.
    The commits carry `Co-Authored-By: Claude Haiku 5.5`, which is the model that actually
    ran the steps.
11. **README and avatar files folded into the docs commit.** A separate session rewrote
    `README.md` and converted the avatar. Its files were folded into this session's docs
    commit at the operator's direction, rather than a separate commit.
12. **Stale `ollama_url` fallbacks fixed.** `embeddings.py`, `search.py` and `ingest.py`
    had fallback literals pointing at Server 1 (`.93`). They now point at `.95`. The
    `qdrant_url` fallbacks correctly stay on `.93`. Plan Section 8 #4 noted the port choice.

#### Decisions

- **Service and unit naming.** `ragnart/service.py` (`uvicorn ragnart.service:app`);
  `ragnart-search.service` on port 8100; `ragnart-ingest.service` and `.timer`;
  `scripts/install-units.sh`. Port 8100 was free on Server 2 (8000 llama-swap, 4096
  OpenCode, 11434 Ollama, 8011/8012 llama-server).
- **Comparison tool kept.** `utils/rag_compare_endpoints.py` is committed, not a scratch
  file. It re-checks Plan Section 4's invariant ("no new params means unchanged ranking")
  and is reusable for every future version.
- **Tie-aware equivalence gate (operator-approved).** With `rerank=false`, each query must
  have the same multiset of scores (tolerance 1e-6), and within each tied-score group the
  same set of `source_path` values (the last group, if cut at `top_k`, compared on count
  and score only). With `rerank=true`, top-1 equality and set overlap are reported but do
  not gate. An OLD-vs-OLD control runs with the same checks, so the noise floor is visible.
- **Production non-determinism is not fixed here.** Adding a stable tie-break or pinning
  reranker batching changes search behaviour, so it is an open item for v0.2+, not v0.1.
- **Snapshot schedule.** One snapshot per ingestion run (`--keep 12`); the 03:00 cron
  entry removed. Scheduling of the 4x/day snapshots is an open item.

#### Evidence

Bench comparison, `utils/rag_compare_endpoints.py`, 14 queries from
`utils/rag_bench_queries.txt`, `top_k=5`, user `florian`. OLD is SoHoAI `:8000/v1/rag/search`;
NEW is RAGNART `:8100/v1/search`. The per-query PASS/FAIL lines are not kept in the session
directory; the aggregate results are below.

| Check | Scope | Result |
| --- | --- | --- |
| HARD | OLD vs NEW, `rerank=false`, tie-aware equivalence | **14/14 PASS** |
| CONTROL | OLD vs OLD, same checks | 14/14 PASS |
| INFO | OLD vs NEW, `rerank=true`, top-1 equal | 14/14 |
| INFO | mean top-k set overlap, OLD vs NEW | 0.79 (control: 0.79) |

- `/health` on NEW: `{"status":"ok","qdrant":true}` (P3 and after cutover).
- `utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS-Certification"`:
  PASS (P3, and again after cutover).
- `systemctl list-timers`: `ragnart-ingest.timer` enabled, next run 19:00 CET;
  `rag-ingest.timer` disabled.
- Live 307 from SoHoAI `/v1/rag/search?q=...` to `http://192.168.1.95:8100/v1/search?...`: first verified at about 13:25 CET, right after the operator's restart of the Server 1 gateway (13:24:56 CET); re-verified after the final P7 edits at 13:59:38 and 14:01:28 CET (12:59:38Z / 13:01:28Z), and again by the Reviewer.
- Ingestion run 13:01-13:50 CET: 32 new files, 0 failed; WAL checkpoint logged; snapshot
  phase completed with `Result=success`.

#### Open items

- **Production search is non-deterministic.** No stable tie-break; reranker batch jitter
  can flip near-ties. Candidate fix for v0.2+; changes behaviour, so not in v0.1.
- **`utils/rag_status.py` reports a false "collection 'documents' does not exist".** It
  does not check aliases, and `get_collections` excludes them. Pre-existing; not fixed here.
- **SoHoAI residuals, out of scope for v0.1.** `schemas.py` orphaned RAG models; RAG
  dependencies (`qdrant-client`, `docling`) still in SoHoAI's `pyproject`; `CLAUDE.md`
  stale RAG mentions (servers table with the rag-ingest timer and 03:00 cron, storage
  paths, a code-style line about `rag_engine`/`state.py`, the architecture diagram);
  `.claude/settings.local.json` `rag-*` allowlist entries.
- **Stub removal, 2026-11-16 (operator date).** Remove SoHoAI's `/v1/rag/search` 307 stub
  on or after 2026-11-16, or at v0.2 kickoff if that is earlier. Before removal, check
  `SoHoAI-gateway.log` for `GET /v1/rag/search ... 307` hits and repoint any caller found.
- **`scripts/qdrant/qdrant.service` and `qdrant-config.yaml`** are carried over with the
  path fixed but **not reinstalled**. Qdrant keeps running from its existing unit.
- **Logrotate.** `ragnart-ingest-logrotate` is renamed but not installed to
  `/etc/logrotate.d/`. The log filename (`rag-ingest.log`) is unchanged, so an existing
  logrotate config should still match. To verify whether one exists.
- **Server 1 gateway reloader is unreliable on NFS.** `uvicorn --reload` did not pick up
  NFS edits. Changes to `main.py` need a manual restart on Server 1 until this is fixed.
- **Snapshot load on the NAS.** Four runs a day, each about 35 min, saturate the NAS and
  slowed the gateway start. Consider scheduling.

#### Handover to v0.2

- **Tree state.** RAGNART HEAD: the docs commit that carries this entry (its parent is
  `796635f`). SoHoAI HEAD: `95d5d0d`.
- **v0.2 entry criteria (Plan Section 6 v0.2).** RAGNART's service and timer are the only
  ones running -- confirmed by this session's P4 verification. **Also required:** the
  SoHoAI `/v1/rag/search` 307 stub is removed (on or after 2026-11-16), after the
  gateway-log check in Open items.
- **Must read first.** v0.2 must read Plan Sections 2-5 in full plus its own Plan Section 6
  v0.2, per Plan Section 1's rule.
- **Environment prerequisites.** Qdrant 1.17.1 at `192.168.1.93:6333`; Ollama bge-m3 at
  `192.168.1.95:11434`; reranker via llama-swap at `192.168.1.95:8000/v1/rerank`; the
  `ragnart-search.service` on `:8100` and `ragnart-ingest.timer` both running from this repo.
