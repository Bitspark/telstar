from .com.pw import StagedMessage

__all__ = ["staging"]


class _staging:
    repository = StagedMessage

    def __getattribute__(self, name):
        return _staging.repository

    def __setattr__(self, name, value):
        _staging.repository = value


staging = _staging()
