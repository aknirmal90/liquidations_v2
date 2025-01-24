class ConfigFileNotFoundError(Exception):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        super().__init__(f"Config file not found: {file_path}")


class ABINotFoundError(Exception):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        super().__init__(f"ABI file not found: {file_path}")


class EventABINotFoundError(Exception):
    def __init__(self, event_name: str) -> None:
        self.event_name = event_name
        super().__init__(f"Event not found: {event_name} on ABI")
