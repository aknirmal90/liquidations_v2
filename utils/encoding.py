from typing import Any, Dict


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
        if input_type['type'] == 'tuple':
            # Handle tuple components recursively
            component_types = [process_type(component) for component in input_type.get('components', [])]
            return f"({','.join(component_types)})"
        elif input_type['type'].endswith('[]'):
            # Handle arrays of simple or tuple types
            base_type = {'type': input_type['type'][:-2]}
            return f"{process_type(base_type)}[]"
        else:
            # Handle basic types
            return input_type['type']

    # Extract the function name
    function_name = event_abi['name']

    # Extract the input types
    input_types = [process_type(input) for input in event_abi.get('inputs', [])]

    # Construct the function signature
    return f"{function_name}({','.join(input_types)})"
