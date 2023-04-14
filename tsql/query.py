from __future__ import annotations

from inspect import stack
from os.path import isabs
from pathlib import Path
from typing import Type, TypeVar

from jinja2 import Environment, FileSystemLoader
from .contrib.text import clear_text

from .exceptions import TSQLValueError

T = TypeVar("T", bound="Query")


class Query:
    TEXT_OR_PATH_MUST_BE_PASSER_ERROR = TSQLValueError(
        f"A text or path must be passed to the Query."
    )

    def __init__(
        self,
        text: str | None = None,
        path: str | None = None,
        template_environment: Environment | None = None,
    ) -> Query:
        if not text and not path:
            raise self.TEXT_OR_PATH_MUST_BE_PASSER_ERROR

        self._text = text
        self._path = path
        self._calling_path = self._get_calling_path()
        self._template_environment = (
            template_environment or self.create_template_environment()
        )

    def create_template_environment(self) -> Environment:
        return Environment(
            loader=FileSystemLoader(self._calling_path),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )

    @classmethod
    def from_file(
        cls: Type[T],
        path: str | Path,
        template_environment: Environment | None = None,
    ) -> T:
        return cls(path=path, template_environment=template_environment)

    @staticmethod
    def _get_calling_path() -> Path:
        calling_context = next(
            context for context in stack() if context.filename != __file__
        )
        return Path(calling_context.filename).parent

    def render(self, envs: dict | None = None, params: dict | None = None) -> str:
        envs = envs or {}
        rendered_template = self.render_template()

        if params:
            rendered_text = rendered_template.format(**params)
        else:
            rendered_text = rendered_template

        return clear_text(rendered_text)

    def render_template(self, **envs):
        if self._text:
            template = self._template_environment.from_string(self._text)
        elif self._path:
            template = self._template_environment.get_template(self._path)
        else:
            raise self.TEXT_OR_PATH_MUST_BE_PASSER_ERROR

        return template.render(**envs)
