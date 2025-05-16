# How to configure your Mech and run using this Quickstart?

There are three additional configurations to do for Mechs:

## `API_KEYS`
This will be asked when you run the quickstart script. It should be a JSON object.
1. Copy over the sample from .api_keys.json.example.

    ```sh
    cp scripts/mech/.api_keys.json.example scripts/mech/.api_keys.json
    ```

2. Setup key value pairs for every AI tool your mech uses
- The name of the tool will be the key used in the file
- The value will be an array of valid API keys the tool can use

3. Minimize the JSON file into a single line

   ```sh
   poetry run python -m scripts.mech.minimize_json
   ```

4. Enter this JSON **in one line** when asked in the quickstart as `API_KEYS`

## `METADATA_HASH`
This is the IPFS hash of the metadata file of this Mech
1.  Copy over the sample from .metadata_hash.json.example. The example file is valid for a single tool.

    ```sh
    cp scripts/mech/.metadata_hash.json.example scripts/mech/.metadata_hash.json
    ```

2.  Define your top level key value pairs
    | Name | Value Type | Description |
    | :--- | :---: | :--- |
    | name | str | Name of your mech |
    | description | str | Description of your mech |
    | inputFormat | str | Can leave it default |
    | outputFormat | str | Can leave it default |
    | image | str | Link to the imagerepresenting your mech |
    | tools | List | List of AI tools your mech supports |
    | toolMetadata | Dict | Provides more info on specific tools |

> [!IMPORTANT] \
> Each tool mentioned in `tools` should have a corresponding `key` in the `toolsMetadata`.

3.  Define your key value pairs for each specific tools.

    | Name         | Value Type | Description                             |
    | :----------- | :--------: | :-------------------------------------- |
    | name         |    str     | Name of the AI tool                     |
    | description  |    str     | Description of the AI tool              |
    | input        |    Dict    | Contains the input schema of the tool   |
    | output       |    Dict    | Contains the output schema of the tool  |

4.  Define your key value pairs for the output schema

    | Name       | Value Type | Description                                                  |
    | :--------- | :--------: | :----------------------------------------------------------- |
    | type       |    str     | Mentions the type of the schema                              |
    | properties |    Dict    | Contains the required output data                            |
    | required   |    List    | Contains the list of fields required in the `properties` key |

> [!IMPORTANT] \
> Each field mentioned in `required` should have a corresponding `key` in the `properties`.

5.  Define your key value pairs for the properties field

    | Name      | Value Type | Description                                                   |
    | :-------- | :--------: | :------------------------------------------------------------ |
    | requestId |    Dict    | Contains the request id and its description                  |
    | result    |    Dict    | Contains the result and its description with an example      |
    | prompt    |    Dict    | Contains the prompt used for the request and its description |

6. Use the following script to push the metadata file to IPFS and get its hash

    ```sh
    poetry run python -m scripts.mech.setup_metadata_hash
    ```

7. Enter this hash when asked in the quickstart as `METADATA_HASH`

## `TOOLS_TO_PACKAGE_HASH`
This is the IPFS hash of the tools file of this Mech. This file contains a JSON object that maps AI tools to their IPFS hashes.
1.  Copy over the sample from .tools_to_packages_hash.json.example.

    ```sh
    cp scripts/mech/.tools_to_packages_hash.json.example scripts/mech/.tools_to_packages_hash.json
    ```

2. Setup key value pairs for every AI tool your mech uses
- The name of the tool will be the key used in the file
- The value will be the IPFS hash of the tool's package

3. Minimize the JSON file into a single line

   ```sh
   poetry run python -m scripts.mech.minimize_json
   ```

4. Enter this JSON **in one line** when asked in the quickstart as `TOOLS_TO_PACKAGE_HASH`

# Mech Migration Guide

## Migrate from mech-quickstart

If you were previously using [mech-quickstart](https://github.com/valory-xyz/mech-quickstart) and want to migrate to the new unified [quickstart](https://github.com/valory-xyz/quickstart) repository, follow these steps:

> Note: Please ensure to meet the [system requirements](https://github.com/valory-xyz/quickstart/?tab=readme-ov-file#system-requirements) of this new quickstart.

1. Copy the `.mech_quickstart` folder from your mech-quickstart repository to the root of quickstart:

    ```bash
    cp -r /path/to/mech-quickstart/.mech /path/to/quickstart/
    ```

2. Run the migration script to create the new `.operate` folder compatible with unified quickstart:

    ```bash
    poetry install
    poetry run python -m scripts.mech.migrate_legacy_mech configs/config_mech.json
    ```

3. Follow the prompts to complete the migration process. The script will:
   - Parse your existing configuration
   - Set up the new operate environment
   - Migrate your service configuration
   - Handle any necessary transfers and settings

4. Once migration is complete, follow the instructions in the [Run the service](https://github.com/valory-xyz/quickstart#run-the-service) section to run your Mech service.

5. After you ensure that the agent runs fine with the new quickstart, please delete the `.mech_quickstart` folder(s) to avoid any private key leaks.
