import sys
import json
from typing import List, Dict
import multibase
import multicodec
from aea.helpers.cid import to_v1
from aea_cli_ipfs.ipfs_utils import IPFSTool
from operate.quickstart.utils import print_title

from scripts.utils import input_with_default_value


metadata_schema = {
    "name": str,
    "description": str,
    "inputFormat": str,
    "outputFormat": str,
    "image": str,
    "tools": List,
    "toolMetadata": Dict,
}

tool_schema = {
    "name": str,
    "description": str,
    "input": Dict,
    "output": Dict,
}
tool_input_schema = {
    "type": str,
    "description": str,
}
tool_output_schema = {"type": str, "description": str, "schema": Dict}

output_schema_schema = {
    "properties": Dict,
    "required": List,
    "type": str,
}

properties_schema = {
    "requestId": Dict,
    "result": Dict,
    "prompt": Dict,
}

properties_data_schema = {
    "type": str,
    "description": str,
}


def setup_metadata_hash() -> None:
    """
    Push the metadata file to IPFS.
    """

    print_title("Mech Quickstart: Metadata hash setup")

    metadata_hash_path = input_with_default_value(
        "Please provide the path to your metadata_hash.json file",
        "scripts/mech/.metadata_hash.json",
    )

    try:
        __validate_metadata_file(metadata_hash_path)
    except ValueError as error_msg:
        print(error_msg)
        print("Please refer to .metadata_hash.json.example for reference")
        sys.exit(1)

    response = IPFSTool().client.add(
        metadata_hash_path, pin=True, recursive=True, wrap_with_directory=False
    )
    v1_file_hash = to_v1(response["Hash"])
    cid_bytes = multibase.decode(v1_file_hash)
    multihash_bytes = multicodec.remove_prefix(cid_bytes)
    v1_file_hash_hex = "f01" + multihash_bytes.hex()

    print_title(f"Metadata hash successfully: {v1_file_hash_hex}")


def __validate_metadata_file(file_path):
    try:
        path = file_path
        with open(path, "r") as f:
            metadata: Dict = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Error: Metadata file not found at {file_path}")
    except json.JSONDecodeError:
        raise ValueError("Error: Metadata file contains invalid JSON.")

    for key, expected_type in metadata_schema.items():
        if key not in metadata:
            raise ValueError(f"Missing key in metadata json: '{key}'")

        if not isinstance(metadata[key], expected_type):
            expected = expected_type.__name__
            actual = type(metadata[key]).__name__
            raise ValueError(
                f"Invalid type for key in metadata json. Expected '{expected}', but got '{actual}'",
            )
        
        print(f'Validated "{key}" in metadata json.')

    tools = metadata["tools"]
    tools_metadata = metadata["toolMetadata"]
    num_of_tools = len(tools)
    num_of_tools_metadata = len(tools_metadata)

    if num_of_tools != num_of_tools_metadata:
        raise ValueError(
            f"Number of tools does not match number of keys in 'toolMetadata'. Expected {num_of_tools} but got {num_of_tools_metadata}.",
        )


    def __validate_input_schema(data: Dict):
        for i_key, i_expected_type in tool_input_schema.items():
            input_data = data["input"]
            if i_key not in input_data:
                raise ValueError(
                    f"Missing key for {tool} -> input: '{i_key}'",
                )

            if not isinstance(input_data[i_key], i_expected_type):
                i_expected = i_expected_type.__name__
                i_actual = type(input_data[i_key]).__name__
                raise ValueError(
                    f"Invalid type for '{i_key}' in {tool} -> input. Expected '{i_expected}', but got '{i_actual}'.",
                )
        
        print(f'Validated "{tool}" input schema.')


    def __validate_output_properties(output_schema_data: Dict, s_key: str):
        for (
            p_key,
            p_expected_type,
        ) in properties_schema.items():
            properties_data = output_schema_data[s_key]
            if p_key not in properties_data:
                raise ValueError(
                    f"Missing key for {tool} -> output -> schema -> properties: '{p_key}'",
                )

            if not isinstance(
                properties_data[p_key], p_expected_type
            ):
                p_expected = p_expected_type.__name__
                p_actual = type(properties_data[p_key]).__name__
                raise ValueError(
                    f"Invalid type for '{p_key}' in {tool} -> output -> schema -> properties. Expected '{p_expected}', but got '{p_actual}'.",
                )

            required = output_schema_data["required"]
            num_of_properties_data = len(properties_data)
            num_of_required = len(required)

            if num_of_properties_data != num_of_required:
                raise ValueError(
                    f"Number of properties data does not match number of keys in 'required'. Expected {num_of_required} but got {num_of_properties_data}.",
                )

            for (
                key,
                expected_type,
            ) in properties_data_schema.items():
                data = properties_data[p_key]
                if key not in data:
                    raise ValueError(
                        f"Missing key in properties -> {p_key}: '{key}'",
                    )

                if not isinstance(data[key], expected_type):
                    expected = expected_type.__name__
                    actual = type(data[key]).__name__
                    raise ValueError(
                        f"Invalid type for key in properties. Expected '{expected}', but got '{actual}'",
                    )

                print(f'Validated "{key}" in properties -> "{p_key}" schema.')

            print(f'Validated "{p_key}" in "{s_key}" schema.')


    def __validate_output_schema(data: Dict):
        for o_key, o_expected_type in tool_output_schema.items():
            output_data = data["output"]
            if o_key not in output_data:
                raise ValueError(
                    f"Missing key for {tool} -> output: '{o_key}'",
                )

            if not isinstance(output_data[o_key], o_expected_type):
                o_expected = o_expected_type.__name__
                o_actual = type(output_data[o_key]).__name__
                raise ValueError(
                    f"Invalid type for '{o_key}' in {tool} -> output. Expected '{o_expected}', but got '{o_actual}'.",
                )

            if o_key != "schema":
                continue

            for (
                s_key,
                s_expected_type,
            ) in output_schema_schema.items():
                output_schema_data = output_data[o_key]
                if s_key not in output_schema_data:
                    raise ValueError(
                        f"Missing key for {tool} -> output -> schema: '{s_key}'",
                    )

                if not isinstance(
                    output_schema_data[s_key], s_expected_type
                ):
                    s_expected = s_expected_type.__name__
                    s_actual = type(output_schema_data[s_key]).__name__
                    raise ValueError(
                        f"Invalid type for '{s_key}' in {tool} -> output -> schema. Expected '{s_expected}', but got '{s_actual}'.",
                    )

                if (
                    s_key == "properties"
                    and "required" in output_schema_data
                ):
                    __validate_output_properties(output_schema_data, s_key)

                print(f'Validated "{s_key}" in output schema -> "{o_key}" schema.')

            print(f'Validated "{o_key}" in output schema.')


    def __validate_tool(tool: str):
        if tool not in tools_metadata:
            raise ValueError(f"Missing toolsMetadata for tool: '{tool}'")

        for key, expected_type in tool_schema.items():
            data = tools_metadata[tool]
            if key not in data:
                raise ValueError(f"Missing key in toolsMetadata: '{key}'")

            if not isinstance(data[key], expected_type):
                expected = expected_type.__name__
                actual = type(data[key]).__name__
                raise ValueError(
                    f"Invalid type for key in toolsMetadata. Expected '{expected}', but got '{actual}'",
                )

            if key == "input":
                __validate_input_schema(data)

            elif key == "output":
                __validate_output_schema(data)

            print(f'Validated "{key}" in toolsMetadata -> "{tool}" schema.')

    for tool in tools:
        __validate_tool(tool)
        print(f'Validated "{tool}" schema.')


if __name__ == "__main__":
    setup_metadata_hash()
