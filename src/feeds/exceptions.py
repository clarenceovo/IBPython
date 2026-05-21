"""Structured exception hierarchy for IBKR and transport errors."""


class IBKRError(Exception):
    """Base for all IBKR-related errors."""
    pass


class IBKRConnectionError(IBKRError):
    """Connection to TWS/Gateway failed or dropped."""
    pass


class IBKRPacingError(IBKRError):
    """Historical data pacing violation."""
    pass


class IBKRCircuitOpenError(IBKRError):
    """Circuit breaker is open — fast-failing."""
    pass


class IBKRContractResolutionError(IBKRError):
    """Contract could not be qualified/resolved."""
    pass


class IBKROrderError(IBKRError):
    """Order placement/management failure."""
    pass


class QuestDBWriteError(Exception):
    """QuestDB write operation failed."""
    pass


class QuestDBConnectionError(Exception):
    """QuestDB connection failed."""
    pass
