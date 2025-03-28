import json
from operate.quickstart.utils import print_title

from scripts.utils import input_with_default_value


def minimize_json():
    """
    Minimize the JSON file.
    """

    print_title("Mech Quickstart: Minimize JSON")

    json_path = input_with_default_value(
        "Please provide the path to your JSON file",
        "scripts/mech/.api_keys.json",
    )

    with open(json_path, "r") as f:
        data = json.load(f)

    with open(json_path, "w") as f:
        json.dump(data, f)

    print_title(f"JSON successfully minimized at {json_path}")


if __name__ == "__main__":
    minimize_json()