class NightingaleError(Exception):
    """Base class for exceptions from within this package."""


class StreamNotStartedError(NightingaleError):
    """Raised when a streaming method is called before start_package_stream()."""

    def __init__(self):
        super().__init__("Stream writing has not been started. Call start_package_stream() first.")
