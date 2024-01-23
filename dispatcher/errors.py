"""Errors and Exceptions used by the dispatcher."""

class DispatcherError(RuntimeError):
    """Base class for all dispatcher errors."""

    pass


class InvalidJobType(DispatcherError):
    """Raised when a job has an unknown job type"""

    pass


class JobDirMissing(DispatcherError):
    """Raised when a job's input directory is missing"""

    pass
