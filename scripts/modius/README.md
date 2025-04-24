# Modius Migration Guide

## Migrate from modius-quickstart

If you were previously using [modius-quickstart](https://github.com/valory-xyz/modius-quickstart) and want to migrate to the new unified [quickstart](https://github.com/valory-xyz/quickstart) repository, follow these steps:

> Note: Please ensure to meet the [system requirements](https://github.com/valory-xyz/quickstart/?tab=readme-ov-file#system-requirements) of this new quickstart.

1. Copy the `.olas-modius` folder from your modius-quickstart repository to the root of quickstart:

    ```bash
    cp -r /path/to/modius-quickstart/.modius /path/to/quickstart/
    ```

2. Run the migration script to create the new `.operate` folder compatible with unified quickstart:

    ```bash
    poetry install
    poetry run python -m scripts.modius.migrate_legacy_modius configs/config_modius.json
    ```

3. Follow the prompts to complete the migration process. The script will:
   - Parse your existing configuration
   - Set up the new operate environment
   - Migrate your service configuration
   - Handle any necessary transfers and settings

4. Once migration is complete, follow the instructions in the [Run the service](https://github.com/valory-xyz/quickstart#run-the-service) section to run your Modius service.

5. After you ensure that the agent runs fine with the new quickstart, please delete the `.olas-modius` folder(s) to avoid any private key leaks.
