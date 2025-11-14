# Optimus Migration Guide

## Migrate from optimus-quickstart

If you were previously using [optimus-quickstart](https://github.com/valory-xyz/optimus-quickstart) and want to migrate to the new unified [quickstart](https://github.com/valory-xyz/quickstart) repository, follow these steps:

> Note: Please ensure to meet the [system requirements](https://github.com/valory-xyz/quickstart/?tab=readme-ov-file#system-requirements) of this new quickstart.

1. Copy the `.optimus` folder from your optimus-quickstart repository to the root of quickstart:

    ```bash
    cp -r /path/to/optimus-quickstart/.optimus /path/to/quickstart/
    ```

2. Run the migration script to create the new `.operate` folder compatible with unified quickstart:

    ```bash
    poetry install
    poetry run python -m scripts.optimus.migrate_legacy_optimus configs/config_optimus.json
    ```

3. Follow the prompts to complete the migration process. The script will:
   - Parse your existing configuration
   - Set up the new operate environment
   - Migrate your AI agent configuration
   - Handle any necessary transfers and settings

4. Once migration is complete, follow the instructions in the [Run the AI agent](../../README.md#run-the-ai-agent) section to run your Optimus AI agent.

5. After you ensure that the AI agent runs fine with the new quickstart, please delete the `.optimus` folder(s) to avoid any private key leaks.


## Migrate your Optimus to only Optimism chain

If you have been running Optimus before v0.5.0 then your agent would have made investments on Optimism, Base and Mode chains. From v0.5.0 onwards, Optimus only supports the Optimism chain, which means your funds in Base and Mode chains will be left idle. You may withdraw your agent's investments from Base and Mode chains by following this guide:

1. Identify your AI agent's folder - Look into the `.operate/services/` directory, the one that contains `optimism` folder inside it - is your Optimus agent's folder. It should start with `sc-...`.

2. Delete the file `.operate/services/sc-<optimus-folder>/persistent_data/assets.json`. This is to ensure that the fresh assets configuration is picked up.

3. Make your AI agent to withdraw investments on Base chain:

    a. Open the file `configs/config_optimus.json` and look for the configuration key `TARGET_INVESTMENT_CHAINS`.
    b. Three lines below, change its "value" to `[\"base\"]`, so that the entire line looks like `"value": "[\"base\"]",`.
    c. Save the file.
    d. Run your agent as usual: `./run_service.sh configs/config_optimus.json`.
    e. After it has started, wait for a few seconds and then run the following command to start the withdrawal process (replace `<your_address>` with your actual address):

    ```bash
    curl -X POST http://localhost:8716/withdrawal/initiate \
    -H "Content-Type: application/json" \
    -d '{
        "target_address": "<your_address>",
    }'
    ```
    f. Wait until your address has received all the funds on Base chain.

4. Repeat step 3 for Mode chain by changing the `TARGET_INVESTMENT_CHAINS` value to `[\"mode\"]`.

5. Finally, revert the `TARGET_INVESTMENT_CHAINS` value back to `[\"optimism\"]` in the `configs/config_optimus.json` file to continue using Optimus on Optimism chain only.

Now you may continue using Optimus using the command `./run_service.sh configs/config_optimus.json`.
