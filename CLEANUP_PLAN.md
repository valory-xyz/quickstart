# Cleanup plan for quickstart

This is a planning-only PR. Nothing in this commit changes runtime
behaviour. Once the plan is approved, implementation commits will be
pushed on top of this same branch. There will be no separate PRs per
phase. This `CLEANUP_PLAN.md` will be deleted in the final
implementation commit.

The goal is to bring quickstart in line with where `trader` sits today:
on the latest `tomte` for linting, with a minimal `tox.ini`, and a
slim CI workflow that delegates to `tomte tox` for lint envs.

## What's already done (not in scope)

PR #172 (chore/uv-migration) already landed most of the "wave 2"
work that applies here:

- `pyproject.toml` is on PEP 621 with `tool.uv.package = false`.
- `poetry.lock` removed, `uv.lock` present.
- All 8 operator shell scripts (`run_service.sh`, `stop_service.sh`,
  `analyse_logs.sh`, `claim_staking_rewards.sh`, `reset_configs.sh`,
  `reset_password.sh`, `reset_staking.sh`,
  `terminate_on_chain_service.sh`) use `uv sync` and `uv run` instead
  of `poetry install` and `poetry run`.

Nothing in this PR touches those.

## What's in scope

### 1. Add tomte and a `[tool.tomte]` block to `pyproject.toml`

Tomte is not in `[dependency-groups].dev` today. Add it pinned to the
same version trader is on (v0.7.0):

```toml
[dependency-groups]
dev = [
    "pexpect==4.9.0",
    "pytest==9.0.3",
    "pytest-cov==7.1.0",
    "tomte[cli, tests] @ git+https://github.com/valory-xyz/tomte.git@v0.7.0",
]

[tool.tomte]
tomte_dep_pin = " @ git+https://github.com/valory-xyz/tomte.git@v0.7.0"
# extra excludes / pylint disables added at implementation time
# based on actual lint output against current scripts/ and tests/.
```

The check_dependencies_extra_excludes and pylint disables will be set
empirically. Quickstart has only `scripts/` and `tests/` to lint, so
the list should stay short.

### 2. Add a minimal `tox.ini`

Quickstart has no `tox.ini` today. Add one that follows the trader
shape: a thin `[tomte-extensions]` block plus any per-repo overrides.
Drops AEA-only envs (`check-hash`, `check-packages`,
`check-abciapp-specs`, `check-handlers`, `liccheck`,
`check-third-party-hashes`) because quickstart has no `packages/`
directory.

Draft:

```ini
; Local extensions to tomte's canonical tox.ini. Consumed by `tomte tox`.

[tomte-extensions]
extra_pylint_disables = C0114,C0115,C0116,R0801

[pytest]
tomte_defaults = true
addopts = -p no:pytest_anchorpy
```

The `-p no:pytest_anchorpy` line stays because the same anchorpy
issue that `pyproject.toml` already addresses still applies under
`tomte tox`. If lint surfaces missing imports, add minimal `[mypy-*]`
blocks at implementation time.

### 3. Fix the Makefile

The Makefile is the one file PR #172 didn't update. It still uses
poetry:

```make
install:
	poetry install --only main

test-install:
	poetry install

run_no_staking_tests:
	poetry run pytest -v tests/test_run_service.py -s --log-cli-level=INFO

test: test-install run_no_staking_tests
```

Two options for the reviewer to pick:

- **Option A (keep, fix):** rewrite to use `uv sync` and `uv run
  pytest`. One line per target.
- **Option B (drop):** delete the Makefile. It's not referenced from
  README, CI, or any other script. The 8 shell scripts already cover
  install + run.

Recommendation: Option B. Open to feedback.

### 4. Wire CI to run `tomte tox` for lint envs

Current `.github/workflows/python-tests.yml` is 344 lines. It runs
`uv sync` and `uv run pytest` for three e2e jobs plus a unit test
matrix. It does no linting. No `black`, `isort`, `flake8`, `mypy`,
`pylint`, `darglint`, `bandit`, `safety`.

Add a `linter_checks` job that mirrors trader's `common_checks.yaml`:

```yaml
linter_checks:
  runs-on: ubuntu-24.04
  steps:
    - uses: actions/checkout@v6
    - uses: actions/setup-python@v6
      with:
        python-version: "3.10"
    - name: Install dependencies
      run: pip install 'tomte[tox,cli] @ git+https://github.com/valory-xyz/tomte.git@v0.7.0' tox-uv
    - name: Code checks
      run: tomte tox -p -e black-check -e isort-check -e flake8 -e mypy -e pylint -e darglint
    - name: Security checks
      run: tomte tox -p -e safety -e bandit
```

The four existing jobs (`setup`, `e2e-test-run-service`,
`e2e-test-staking`, the pearl-migration test, `unit-tests`) stay as
they are. They already use uv correctly.

Expected drop: ~344 lines is mostly the four big jobs. The linter job
adds ~20 lines net.

### 5. CONTRIBUTING.md slim down

Current `CONTRIBUTING.md` is 135 lines with two distinct sections:

- A generic contribution workflow (lines 1 to 60ish). This is
  near-identical to the equivalent section in every other Valory
  repo and is exactly the kind of "useless doc" the wave-2 cleanup
  pattern replaces with a stub linking to
  `open-autonomy/CONTRIBUTING.md`.
- A `config.json` schema reference (lines 60ish to 135). This is
  quickstart-specific and useful.

Plan:

- Replace the workflow section with a ~10-line stub linking to the
  canonical `open-autonomy/CONTRIBUTING.md`.
- Move the schema reference into `README.md` (it sits naturally next
  to the existing config explanation there) or into a new
  `docs/config_schema.md`. Open question, recommendation is README.md.

After the move, `CONTRIBUTING.md` ends up ~15 lines.

## What's explicitly out of scope

- **`pyproject.toml` and lockfile work**. Done in PR #172.
- **Shell scripts**. Done in PR #172.
- **Removing migration scripts**
  (`scripts/predict_trader/migrate_legacy_quickstart.py`,
  `scripts/optimus/migrate_legacy_optimus.py`). Both are still
  referenced from `scripts/*/README.md` and from
  `scripts/pearl_migration/prompts.py`. They're load-bearing.
- **`.gitleaks.toml` stub**. Quickstart runs no gitleaks scan today.
  Adding one is a separate security task.
- **README rewrite**. Only one section gets added (the config schema
  moved from CONTRIBUTING.md). The rest stays as-is.
- **Removing any `.sh` script**. All 8 are referenced from README as
  the public operator interface.
- **Touching `scripts/pearl_migration/`**. It's recently added and
  actively used.

## Open questions for the reviewer

1. **Makefile: keep or drop?** Recommendation: drop. It's not
   referenced anywhere and the shell scripts cover install + run.
2. **Config schema doc: move to `README.md` or to a new
   `docs/config_schema.md`?** Recommendation: `README.md` so it
   lives next to the existing config explanation.

## Sequencing once approved

Implementation lands as commits on this same branch in this order:

1. Add tomte to dev deps and `[tool.tomte]` to `pyproject.toml`.
   Run `uv lock` to refresh `uv.lock`.
2. Add `tox.ini`. Run `tomte tox -p -e black-check -e isort-check -e
   flake8 -e mypy -e pylint -e darglint` locally. Fix lint output.
3. Apply the Makefile decision (drop or rewrite).
4. Add the `linter_checks` CI job.
5. Slim `CONTRIBUTING.md` and move the schema section per the
   reviewer's pick.
6. Delete this `CLEANUP_PLAN.md`.

Expected total diff (commits 1 to 6): ~250 lines added (tox.ini,
`[tool.tomte]` block, CI job, README section), ~120 lines removed
(CONTRIBUTING.md trimmed, Makefile possibly dropped, `uv.lock`
delta is mechanical).
