from typing import Any, Dict, Union

from eth_abi import decode, encode
from eth_utils import keccak
from hexbytes import HexBytes
from web3.datastructures import AttributeDict


def get_signature(event_abi: Dict[str, Any]) -> str:
    """
    Generate the function signature from the event ABI, including nested tuples.

    Args:
        event_abi (Dict[str, Any]): The ABI definition of the event.

    Returns:
        str: The function signature as a string.
    """

    def process_type(input_type: Dict[str, Any]) -> str:
        """Recursively process the type, handling tuples and nested structures."""
        if input_type["type"] == "tuple":
            # Handle tuple components recursively
            component_types = [
                process_type(component)
                for component in input_type.get("components", [])
            ]
            return f"({','.join(component_types)})"
        elif input_type["type"].endswith("[]"):
            # Handle arrays of simple or tuple types
            base_type = {"type": input_type["type"][:-2]}
            return f"{process_type(base_type)}[]"
        else:
            # Handle basic types
            return input_type["type"]

    # Extract the function name
    function_name = event_abi["name"]

    # Extract the input types
    input_types = [process_type(input) for input in event_abi.get("inputs", [])]

    # Construct the function signature
    return f"{function_name}({','.join(input_types)})"


def get_keccak_hash(signature):
    return "0x" + keccak(text=signature).hex()


def get_topic_0(abi: Dict[str, Any]) -> str:
    return get_keccak_hash(signature=get_signature(abi))


def get_method_id(abi: Dict[str, Any]) -> str:
    return get_keccak_hash(signature=get_signature(abi))[:10]


def decode_hex(value: Union[bytes, HexBytes, str]) -> str:
    """
    Decode the given value.
    Hexadecimal fields in evm rpc responses are either returned as bytes or HexBytes objects,
    the decoder converts this into string objects with `0x` prefix

    Args:
        value (Union[bytes, HexBytes, any]): The value to decode.

    Returns:
        Union[str, any]: Decoded value in hexadecimal format if bytes, otherwise returns the value as is.
    """
    if isinstance(value, (bytes, HexBytes)):
        ret = value.hex().lower()
        if ret[:2] != "0x":
            ret = "0x" + ret
        return ret
    elif isinstance(value, str):
        if value[:2] == "0x":
            return value.lower()
        else:
            return value
    else:
        return value


def decode_any(data: Any) -> AttributeDict:
    # If the data is bytes, decode it using decode_hex
    if isinstance(data, (str, bytes, HexBytes)):
        return decode_hex(data)

    # If the data is a list, apply decoding recursively to each element
    elif isinstance(data, list):
        return AttributeDict({str(i): decode_any(item) for i, item in enumerate(data)})

    # If the data is a dictionary, apply decoding recursively to each value
    elif isinstance(data, (dict, AttributeDict)):
        return AttributeDict({key: decode_any(value) for key, value in data.items()})

    # If it's neither bytes nor a list/dict, return the data as is
    return data


def attribute_dict_to_dict(data):
    # If the data is bytes, decode it using decode_hex
    if isinstance(data, (str, bytes, HexBytes)):
        return decode_hex(data)

    # If the input is an AttributeDict, convert it to a dictionary
    if isinstance(data, AttributeDict):
        return {key: attribute_dict_to_dict(value) for key, value in data.items()}
    # Otherwise, return the data as-is
    else:
        return data


def add_0x_prefix(value) -> str:
    if isinstance(value, HexBytes):
        value = value.hex().lower()

    if value[:2] == "0x":
        return value
    else:
        return "0x" + value


def get_encoded_params(function_abi: list, params: list) -> str:
    function_types = []
    for abi in function_abi["inputs"]:
        if abi.get("type") == "tuple":
            tuple_types = []
            for component in abi.get("components", []):
                if component.get("type") == "tuple":
                    sub_tuple_types = [
                        c.get("type") for c in component.get("components", [])
                    ]
                    tuple_types.append(f"({','.join(sub_tuple_types)})")
                else:
                    tuple_types.append(component.get("type"))
            function_types.append(f"({','.join(tuple_types)})")
        else:
            function_types.append(abi.get("type"))
    return encode(function_types, params).hex()


def get_decoded_params(function_abi: dict, result: str) -> dict:
    # Get output types from function ABI
    output_types = []
    output_names = []

    for output in function_abi.get("outputs", []):
        if output.get("type") == "tuple":
            tuple_types = []
            for component in output.get("components", []):
                if component.get("type") == "tuple":
                    sub_tuple_types = [
                        c.get("type") for c in component.get("components", [])
                    ]
                    tuple_types.append(f"({','.join(sub_tuple_types)})")
                else:
                    tuple_types.append(component.get("type"))
            output_types.append(f"({','.join(tuple_types)})")
        else:
            output_types.append(output.get("type"))
        output_names.append(output.get("name"))

    # Remove 0x prefix if present
    if result.startswith("0x"):
        result = result[2:]

    # Decode the result
    decoded = decode(output_types, bytes.fromhex(result))

    # Create dictionary mapping output names to decoded values
    return dict(zip(output_names, decoded))


def get_short_hex(hex_str: str) -> str:
    return hex_str[:6] + "..." + hex_str[-4:]
