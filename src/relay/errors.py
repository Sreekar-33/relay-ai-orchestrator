from __future__ import annotations


class RelayError(Exception):
    """API-level error -> {"error": {"code", "message", "details"}} with an HTTP status."""

    def __init__(self, code: str, message: str, status: int = 400, details=None):
        super().__init__(message)
        self.code, self.message, self.status, self.details = code, message, status, details

    def to_body(self):
        body = {"code": self.code, "message": self.message}
        if self.details is not None:
            body["details"] = self.details
        return {"error": body}


class StepError(Exception):
    """A node execution failed. TransientError -> retry with backoff; PermanentError -> fail the step."""

    def __init__(self, code: str, message: str, *, detail=None, tokens=None, meta=None):
        super().__init__(message)
        self.code, self.message, self.detail, self.tokens, self.meta = code, message, detail, tokens, meta or {}

    def to_dict(self):
        out = {"code": self.code, "message": self.message, "retryable": isinstance(self, TransientError)}
        if self.detail is not None:
            out["detail"] = self.detail
        return out


class TransientError(StepError):
    pass


class PermanentError(StepError):
    pass
