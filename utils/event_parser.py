import json
import os
from typing import Any, Dict, List, Optional

from eth_abi import decode
from eth_utils import decode_hex
from web3 import Web3


def load_aave_abi() -> List[Dict]:
    """Load the Aave ABI from aave/abi.json"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    abi_path = os.path.join(current_dir, "..", "aave", "abi.json")

    with open(abi_path, "r") as f:
        return json.load(f)


def get_event_signature_to_abi_mapping() -> Dict[str, Dict]:
    """Create a mapping from event signature (topic0) to ABI definition"""
    abi = load_aave_abi()
    mapping = {}

    for item in abi:
        if item.get("type") == "event":
            # Calculate the event signature
            event_name = item["name"]
            input_types = [inp["type"] for inp in item["inputs"]]
            signature_text = f"{event_name}({','.join(input_types)})"
            topic0 = Web3.keccak(text=signature_text).hex()
            mapping[topic0] = item

    return mapping


def parse_event_log(
    log_data: Dict, event_abi_mapping: Dict[str, Dict]
) -> Optional[Dict]:
    """Parse a single event log using the ABI mapping"""
    try:
        topics = log_data.get("topics", [])
        if not topics:
            return None

        topic0 = topics[0] if isinstance(topics[0], str) else topics[0]
        event_abi = event_abi_mapping.get(topic0)

        if not event_abi:
            return None

        # Parse the event using web3
        w3 = Web3()
        event_signature = w3.keccak(
            text=f"{event_abi['name']}({','.join([inp['type'] for inp in event_abi['inputs']])})"
        ).hex()

        if topic0 != event_signature:
            return None

        # Decode the event data
        decoded_data = {}
        topic_index = 1  # Skip topic0 which is the event signature

        # First pass: decode indexed parameters from topics
        for input_def in event_abi["inputs"]:
            if input_def["indexed"]:
                if topic_index < len(topics):
                    topic_value = topics[topic_index]
                    decoded_value = decode_topic_value(topic_value, input_def["type"])
                    decoded_data[input_def["name"]] = {
                        "value": decoded_value,
                        "type": input_def["type"],
                        "indexed": True,
                    }
                    topic_index += 1

        # Second pass: decode non-indexed parameters from data field
        non_indexed_inputs = [inp for inp in event_abi["inputs"] if not inp["indexed"]]
        if non_indexed_inputs and "data" in log_data and log_data["data"] != "0x":
            try:
                # Prepare types for ABI decoding
                types = [inp["type"] for inp in non_indexed_inputs]
                data_bytes = decode_hex(log_data["data"])

                # Decode the data field
                decoded_values = decode(types, data_bytes)

                # Map decoded values to parameter names
                for i, input_def in enumerate(non_indexed_inputs):
                    if i < len(decoded_values):
                        decoded_data[input_def["name"]] = {
                            "value": decoded_values[i],
                            "type": input_def["type"],
                            "indexed": False,
                        }
            except Exception as decode_error:
                # Fallback to raw data if decoding fails
                for input_def in non_indexed_inputs:
                    decoded_data[input_def["name"]] = {
                        "value": log_data["data"],
                        "type": input_def["type"],
                        "indexed": False,
                        "raw": True,
                        "decode_error": str(decode_error),
                    }

        return {
            "event_name": event_abi["name"],
            "signature": event_signature,
            "decoded_data": decoded_data,
            "raw_log": log_data,
        }

    except Exception as e:
        return {"event_name": "Unknown", "error": str(e), "raw_log": log_data}


def decode_topic_value(topic_hex: str, param_type: str) -> Any:
    """Decode a topic value based on its type"""
    try:
        if param_type == "address":
            # Address is the last 20 bytes
            return "0x" + topic_hex[-40:]
        elif param_type.startswith("uint") or param_type.startswith("int"):
            return int(topic_hex, 16)
        elif param_type == "bool":
            return bool(int(topic_hex, 16))
        elif param_type.startswith("bytes"):
            return topic_hex
        else:
            return topic_hex
    except Exception:
        return topic_hex


def parse_transaction_logs(logs: List[Dict]) -> List[Dict]:
    """Parse all logs in a transaction using Aave ABI"""
    event_mapping = get_event_signature_to_abi_mapping()
    parsed_logs = []

    for log in logs:
        parsed_log = parse_event_log(log, event_mapping)
        if parsed_log:
            parsed_logs.append(parsed_log)
        else:
            # Include unparsed logs as well
            parsed_logs.append(
                {"event_name": "Unknown Event", "raw_log": log, "decoded_data": {}}
            )

    return parsed_logs


def format_decoded_value(value: Any, param_type: str) -> str:
    """Format decoded values for display"""
    if value is None:
        return "N/A"

    if param_type == "address":
        return str(value)
    elif param_type.startswith("uint") or param_type.startswith("int"):
        # Format large numbers with commas
        try:
            int_value = int(value)
            if int_value > 1000000000000000000:  # > 1e18, likely wei amounts
                # Show both raw value and formatted
                formatted = f"{int_value:,}"
                if int_value >= 10**18:
                    eth_value = int_value / 10**18
                    return f"{formatted} ({eth_value:.6f} ETH)"
                return formatted
            else:
                return f"{int_value:,}"
        except Exception:
            return str(value)
    elif param_type == "bool":
        return "True" if value else "False"
    else:
        return str(value)
