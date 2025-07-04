from eth_abi import decode


class InvalidMethodSignature(Exception):
    pass


class InvalidObservations(Exception):
    pass


TRASMIT_METHOD_ID = "b1dc65a4"
FORWARD_METHOD_ID = "0x6fadcf72"
TRANSMIT_DATA_TYPES = ["bytes32[3]", "bytes", "bytes32[]", "bytes32[]", "bytes32"]

REPORT_DATA_TYPES = ["bytes32", "bytes32", "int192[]"]

FORWARD_DATA_TYPES = ["address", "bytes"]

MIN_OBSERVATIONS = 2
MAX_OBSERVATIONS = 10


def parse_forwarder_call(input_data: str) -> dict:
    """
    Parse a forwarder call to extract oracle data for median price calculation.

    Args:
        input_data (str): The hex-encoded input data for the forward(address to,bytes data) call

    Returns:
        dict: Parsed data containing:
            - oracle_address: The target oracle address
            - oracle_data: The forwarded oracle data
            - median_price: The median price from observations
            - config_digest: The config digest
            - epoch_and_round: The epoch and round
            - observations: List of price observations
    """
    if not input_data.startswith(FORWARD_METHOD_ID):
        raise InvalidMethodSignature(
            f"Invalid forward method signature: {input_data[:10]}..."
        )

    # Remove method ID and decode forward parameters
    forward_data = input_data[len(FORWARD_METHOD_ID) :]
    oracle_address, oracle_data = decode(
        FORWARD_DATA_TYPES, bytes.fromhex(forward_data)
    )

    # Parse the oracle data (which should be a transmit call)
    oracle_data_hex = oracle_data.hex()

    if not oracle_data_hex.startswith(TRASMIT_METHOD_ID):
        raise InvalidMethodSignature(
            f"Invalid oracle data method signature: {oracle_data_hex[:10]}..."
        )

    # Parse the transmit call data
    transmit_data = oracle_data_hex[len(TRASMIT_METHOD_ID) :]
    _reportContext, _report, _rs, _ss, _rawVs = decode(
        TRANSMIT_DATA_TYPES, bytes.fromhex(transmit_data)
    )

    # Parse the report data
    rawReportContext, rawObservers, observations = decode(REPORT_DATA_TYPES, _report)

    # Extract epoch and round (5 bytes) after configDigest
    epoch_and_round = int.from_bytes(rawReportContext[27:32], byteorder="big")

    # Validate observations are sorted
    for i in range(len(observations) - 1):
        if observations[i] > observations[i + 1]:
            raise ValueError("observations not sorted")

    # Calculate median
    median = observations[len(observations) // 2]

    return {
        "oracle_address": oracle_address,
        "oracle_data": oracle_data_hex,
        "median_price": median,
        "epoch_and_round": epoch_and_round,
        "observations": observations,
    }
