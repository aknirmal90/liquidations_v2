from eth_abi import decode


class InvalidMethodSignature(Exception):
    pass


class InvalidObservations(Exception):
    pass


TRASMIT_METHOD_ID = "0xc9807539"
TRANSMIT_DATA_TYPES = [
    "bytes",
    "bytes32[]",
    "bytes32[]",
    "bytes32"
]

REPORT_DATA_TYPES = [
    "bytes32",
    "bytes32",
    "int192[]"
]

MIN_OBSERVATIONS = 2
MAX_OBSERVATIONS = 10


def get_latest_answer(input_data: str) -> dict:
    if not input_data.startswith(TRASMIT_METHOD_ID):
        raise InvalidMethodSignature(f"Invalid method signature: {input_data}")

    _report, _rs, _ss, _rawVs = decode(TRANSMIT_DATA_TYPES, bytes.fromhex(input_data[len(TRASMIT_METHOD_ID):]))

    rawReportContext, rawObservers, observations = decode(REPORT_DATA_TYPES, _report)
    # Extract configDigest (16 bytes) after 11-byte padding
    config_digest = int.from_bytes(rawReportContext[11:27], byteorder='big')

    # Extract epoch and round (5 bytes) after configDigest
    epoch_and_round = int.from_bytes(rawReportContext[27:32], byteorder='big')

    # Validate observations are sorted
    for i in range(len(observations) - 1):
        if observations[i] > observations[i + 1]:
            raise ValueError("observations not sorted")

    if len(observations) != 10:
        raise InvalidObservations(f"Invalid number of observations: {len(observations)}")

    # Convert hex string to list of numbers, taking first 10 digits (each digit is an observer)
    observer_indices = [int(rawObservers.hex()[i:i + 2], 16) for i in range(0, 20, 2)]

    # Check for repeated non-zero numbers
    non_zero_indices = [x for x in observer_indices if x != 0]
    if len(non_zero_indices) != len(set(non_zero_indices)):
        raise InvalidObservations("Repeated non-zero observer indices found")

    # Calculate median
    median = observations[len(observations) // 2]

    return {
        'config_digest': hex(config_digest),
        'epoch_and_round': epoch_and_round,
        'median': median,
        'transmitter': None
    }
