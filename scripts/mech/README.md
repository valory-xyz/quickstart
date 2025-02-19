# Mech Migration Guide

## Migrate from mech-quickstart

If you were previously using [mech-quickstart](https://github.com/valory-xyz/mech-quickstart) and want to migrate to the new unified [quickstart](https://github.com/valory-xyz/quickstart) repository, follow these steps:

1. Copy the `.mech_quickstart` folder from your mech-quickstart repository to the root of quickstart:

    ```bash
    cp -r /path/to/mech-quickstart/.mech /path/to/quickstart/
    ```

2. Run the migration script to create the new `.operate` folder compatible with unified quickstart:

    ```bash
    poetry run python -m scripts.mech.migrate_legacy_mech
    ```

3. Follow the prompts to complete the migration process. The script will:
   - Parse your existing configuration
   - Set up the new operate environment
   - Migrate your service configuration
   - Handle any necessary transfers and settings

4. Once migration is complete, follow the instructions in the [Run the service](https://github.com/valory-xyz/quickstart#run-the-service) section to run your Mech service.