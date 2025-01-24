import importlib
import json
import os

import yaml


def parse_yaml(file_path: str) -> dict | None:
    """
    Parse the given YAML file and return a dictionary.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict: Parsed contents of the YAML file.
    """
    with open(file=file_path, mode="r") as file:
        try:
            data = yaml.safe_load(stream=file)
            return data
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML file: {exc}")
            return None


def parse_json(file_path: str) -> dict | None:
    """
    Parse the given JSON file and return a dictionary.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict: Parsed contents of the JSON file.
    """
    with open(file=file_path, mode="r") as file:
        try:
            data = json.load(fp=file)
            return data
        except json.JSONDecodeError as exc:
            print(f"Error parsing JSON file: {exc}")
            return None


def get_clazz_object(absolute_path: str) -> type | None:
    """
    Retrieve a class object from its absolute import path.

    Args:
        absolute_path (str): The fully qualified name of the class,
                             including the module path.

    Returns:
        type: The class type object referred to by the import path.

    Example:
        get_clazz_object('package.module.ClassName')
    """
    module_path, clz = absolute_path.rsplit(sep=".", maxsplit=1)
    module = importlib.import_module(name=module_path)
    return getattr(module, clz)


def save_json(folder_path: str, file_name: str, data: dict) -> None:
    """
    Save a dictionary as a JSON file in the specified folder with the given file name.

    Args:
        folder_path (str): The path to the folder where the file will be saved.
        file_name (str): The name of the JSON file.
        data (dict): The dictionary data to be saved as a JSON file.

    Returns:
        None
    """
    # Combine folder path and file name to create the full file path
    file_path = os.path.join(folder_path, file_name)

    # Create the directory if it doesn't exist
    os.makedirs(name=folder_path, exist_ok=True)

    # Write the dictionary data to the file as JSON with improved formatting
    with open(file=file_path, mode="w") as json_file:
        json.dump(obj=data, fp=json_file, indent=4, separators=(",", ": "), ensure_ascii=False)
