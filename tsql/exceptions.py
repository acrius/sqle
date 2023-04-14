from __future__ import annotations


class TSQLException(Exception):
    ...


class TSQLValueError(TSQLException, ValueError):
    ...


class SerizlierNotFound(TSQLException, ValueError):
    _message = "Serializer not found for {param_name}({param_type})"

    def __init__(self, param_name, param_type) -> None:
        message = self._message.format(param_name=param_name, param_type=param_type)
        super().__init__(message)
