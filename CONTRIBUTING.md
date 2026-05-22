# Contributing to OLAS AI Agents Quickstart

This repository follows the Valory Open-Autonomy contribution workflow.

See the canonical guide for the PR checklist, pre-commit routine,
coding style, and linter / test commands:
**[open-autonomy/CONTRIBUTING.md](https://github.com/valory-xyz/open-autonomy/blob/main/CONTRIBUTING.md)**

Repo-specific notes for this project:

- **Scope**: contributions are welcome in `configs/` (agent configs),
  `scripts/` (agent-specific scripts), and `tests/`.
- **Open an issue first** for any non-trivial change so it can be
  discussed before you spend time on it.
- **Run lint locally** with `uv run tomte tox -p -e black-check -e
  isort-check -e flake8 -e mypy -e pylint -e darglint -e safety -e
  bandit -e liccheck`. CI runs the same set.
- **Run unit tests locally** with `PYTHONPATH=. uv run pytest tests
  --ignore=tests/test_run_service.py
  --ignore=tests/test_staking_service.py
  --ignore=tests/test_migrate_to_pearl.py`. The ignored files are
  e2e tests that need Docker and live RPC secrets.

## Guide for the AI agent `config.json`

This is the configuration file whose path is passed as an argument to the `./run_service.sh` and other commands.
Please follow this guide when creating or modifying the configs.
The JSON file should have the following schema:

### Top-level Fields

| Field Name      | Type                | Description                                                                             |
|-----------------|---------------------|-----------------------------------------------------------------------------------------|
| name            | string              | Name of the AI agent. This name is used for caching, so don't modify it afterwards.     |
| hash            | string              | IPFS hash of the AI agent package.                                                      |
| description     | string              | Description of the AI agent.                                                            |
| image           | string              | URL to an image representing the agent blueprint.                                       |
| service_version | string              | Version of the AI agent.                                                                |
| home_chain      | string              | Name of the home blockchain network.                                                    |
| configurations  | object              | Chain-specific configuration. See table below.                                          |
| env_variables   | object              | Environment variables to be set for the AI agent. See table below.                      |

---

#### `configurations` Object

| Field Name      | Type    | Description                                                                                     |
|-----------------|---------|-------------------------------------------------------------------------------------------------|
| [chain name]    | object  | Keyed by chain name (e.g., "gnosis"). Contains AI agent configuration for that chain. See below.|

##### Example: `configurations.gnosis`

| Field Name           | Type    | Description                                                                     |
|----------------------|---------|---------------------------------------------------------------------------------|
| agent_id             | integer | Agent blueprint ID of the registered agent blueprint package in OLAS registry.  |
| nft                  | string  | IPFS hash of the image of the NFT of this AI agent.                             |
| threshold            | integer | It is deprecated now and will be removed in the future. Leave it `1` for now.   |
| use_mech_marketplace | bool    | It is deprecated now and will be removed in the future. Leave it `true` for now.|
| fund_requirements    | object  | Funding requirements for agent instances and AI agent safe. See table below.    |

###### `fund_requirements` Object

| Field Name (Token Address) | Type     | Description                                         |
|----------------------------|----------|-----------------------------------------------------|
| agent                      | number   | Amount required for the agent instances (in wei).   |
| safe                       | number   | Amount required for the AI agent safe (in wei).     |

> Token address is `0x0000000000000000000000000000000000000000` for native currency like ETH, xDAI, etc.
---

#### `env_variables` Object

| Field Name                  | Type    | Description                                                                                   |
|-----------------------------|---------|-----------------------------------------------------------------------------------------------|
| [variable name]             | object  | Keyed by variable name. Contains details for each environment variable. See below.            |

##### Example: `env_variables.GNOSIS_LEDGER_RPC`

| Field Name      | Type    | Description                                                      |
|-----------------|---------|------------------------------------------------------------------|
| name            | string  | Human-readable name of the variable.                             |
| description     | string  | Description of the variable.                                     |
| value           | string  | Default or user-provided value.                                  |
| provision_type  | string  | How the variable is provided: "user", "computed", or "fixed".    |

What happens when the `provision_type` is:
- `user` - The quickstart will ask for this value from CLI at runtime, and save it to avoid asking again.
- `fixed` - The `value` written in the config.json will be provided to the agent instance's environment variable, as is.
- `computed` - These are for special environment variables that the quickstart will set for you, based on other configurations like staking program, priority mech, etc.

## License

By contributing to this repository, you agree that your contributions will be licensed under the Apache License 2.0 (as specified in the [LICENSE](./LICENSE) file).

---

Thank you for helping improve the OLAS AI agents quickstart!
