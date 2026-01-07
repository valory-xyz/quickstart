# Contributing to OLAS AI Agents Quickstart

## How to Contribute

### Before You Start

1. **Open an Issue First**: Before starting work on a feature, bug fix, or significant change, please [open an issue](https://github.com/valory-xyz/quickstart/issues) to discuss your idea with the maintainers. This helps ensure your contribution aligns with the project's direction and avoids duplicate work.

2. **Check Existing Issues**: Review open and closed issues to see if your concern has already been discussed.

### Contribution Areas

We welcome contributions in the following areas:

- **`configs/`**: Agent configurations and setup files
- **`scripts/`**: Agent-specific scripts and utilities
- **`tests/`**: Test cases and testing improvements

### Git Workflow

We follow a standard git workflow:

1. **Create a Branch**: Create a feature branch from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**: Implement your changes following the guidelines below

3. **Commit Your Work**: Make clear, descriptive commits
   ```bash
   git commit -m "Brief description of your changes"
   ```

4. **Push and Create a PR**: Push your branch and create a pull request
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Code Review**: Address any feedback from reviewers

6. **Merge**: Once approved, your PR will be merged into `main`

### Code Quality

- **CI Checks**: All code changes are validated through CI checks. Ensure your changes pass all automated checks (linting, testing, etc.)
- **Local Testing**: While not required, testing your changes locally is recommended
- **Documentation**: Include or update documentation as needed for new features or changes

### Commit Guidelines

- Use clear, descriptive commit messages
- Reference related issues when applicable: `Fixes #123` or `Related to #123`
- Keep commits focused on a single logical change

## Questions?

If you have questions or need clarification:
- Check existing [issues](https://github.com/valory-xyz/quickstart/issues) and [PRs](https://github.com/valory-xyz/quickstart/pulls)
- Open a new issue with your question

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
