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

Tomte is not in `[dependency-groups].dev` today. Add it pinned to
v0.7.0 (also the version trader is on). v0.7.0 is published on PyPI,
so a regular `==` pin is fine. No need for the git URL form:

```toml
[dependency-groups]
dev = [
    "pexpect==4.9.0",
    "pytest==9.0.3",
    "pytest-cov==7.1.0",
    "tomte[cli, tests]==0.7.0",
]

[tool.tomte]
tomte_dep_pin = "==0.7.0"
# extra excludes / pylint disables added at implementation time
# based on actual lint output against current scripts/ and tests/.
```

The check_dependencies_extra_excludes and pylint disables will be set
empirically. Quickstart has only `scripts/` and `tests/` to lint, so
the list should stay short.

### 2. Add a minimal `tox.ini`

Quickstart has no `tox.ini` today. Add one that follows the trader
shape: a thin `[tomte-extensions]` block, a `[Licenses]` block for
`liccheck`, and any per-repo overrides. Drops AEA-only envs
(`check-hash`, `check-packages`, `check-abciapp-specs`,
`check-handlers`, `check-third-party-hashes`) because quickstart has
no `packages/` directory. `liccheck` stays in scope (it reads
`[Licenses]` and `[Authorized Packages]`, not AEA state).

Draft:

```ini
; Local extensions to tomte's canonical tox.ini. Consumed by `tomte tox`.

[tomte-extensions]
extra_pylint_disables = C0114,C0115,C0116,R0801

[pytest]
tomte_defaults = true
addopts = -p no:pytest_anchorpy

[Licenses]
authorized_licenses =
    bsd
    new bsd
    bsd license
    apache
    apache 2.0
    apache software
    mit
    mit license
    python software foundation license
unauthorized_licenses =
    gpl v3

[Authorized Packages]
; per-repo allowlist filled at implementation time, mirroring trader's pattern
```

The `-p no:pytest_anchorpy` line stays because the same anchorpy
issue that `pyproject.toml` already addresses still applies under
`tomte tox`. If lint surfaces missing imports, add minimal `[mypy-*]`
blocks at implementation time.

### 3. Delete the Makefile

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

Confirmed: no references from README, CI, or any `.sh` script. The 8
operator shell scripts already cover install + run. Per reviewer
feedback, the Makefile gets deleted outright at implementation time.

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
      run: pip install 'tomte[tox,cli]==0.7.0' tox-uv
    - name: Code checks
      run: tomte tox -p -e black-check -e isort-check -e flake8 -e mypy -e pylint -e darglint
    - name: Security checks
      run: tomte tox -p -e safety -e bandit
    - name: License compatibility check
      run: tomte tox -e liccheck
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

Plan (per reviewer):

- Replace the workflow section with a ~10-line stub linking to the
  canonical `open-autonomy/CONTRIBUTING.md`.
- Keep the `config.json` schema reference where it is, in
  `CONTRIBUTING.md`. No move.

After the trim, `CONTRIBUTING.md` ends up at the stub plus the
existing schema section (~85 lines, down from 135).

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

## Reviewer decisions resolved

- Tomte pin: PyPI `==0.7.0` form, not the git URL.
- `liccheck`: kept in scope.
- Makefile: deleted outright.
- Config schema doc: stays in `CONTRIBUTING.md` (no move).

## Sequencing once approved

Implementation lands as commits on this same branch in this order:

1. Add tomte to dev deps and `[tool.tomte]` to `pyproject.toml`.
   Run `uv lock` to refresh `uv.lock`.
2. Add `tox.ini` (with `[tomte-extensions]`, `[pytest]`, `[Licenses]`,
   `[Authorized Packages]` blocks). Run
   `tomte tox -p -e black-check -e isort-check -e flake8 -e mypy -e
   pylint -e darglint -e bandit -e safety -e liccheck` locally. Fix
   lint output.
3. Delete the Makefile.
4. Add the `linter_checks` CI job (including the `liccheck` step).
5. Slim `CONTRIBUTING.md` (workflow section becomes a stub, schema
   stays).
6. Delete this `CLEANUP_PLAN.md`.

Expected total diff (commits 1 to 6): ~200 lines added (`tox.ini`,
`[tool.tomte]` block, CI job), ~80 lines removed
(`CONTRIBUTING.md` workflow stub, `Makefile` deletion, plus the
mechanical `uv.lock` delta).
