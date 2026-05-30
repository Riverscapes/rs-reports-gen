"""Utilities to handle interactive prompts consistently."""

from __future__ import annotations

import os

import questionary
from termcolor import colored


def is_truthy(value: str | None) -> bool:
    """Returns True if the string represents a truthy value.
    Accepts "1", "true", "yes" (case-insensitive, ignores whitespace)
    Any other value, including None, returns False
    """
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def get_env_or_select(env_var: str, choices: list[str], message: str, default: str) -> str | None:
    """Handles multiple-choice string selections."""
    val = os.environ.get(env_var)
    if val:
        if val not in choices:
            raise RuntimeError(colored(f"\n{env_var} must be one of: {', '.join(choices)}.\nPlease fix or unset the variable to choose manually.\n", "red"))
        return val

    return questionary.select(message=message, choices=choices, default=default).ask()


def get_env_or_confirm(env_var: str, message: str, default: bool = True) -> bool | None:
    """Handles yes/no boolean env var or user confirmation."""
    val = os.environ.get(env_var)
    if val is not None:
        return is_truthy(val)
    return questionary.confirm(message=message, default=default).ask()


def get_unit_system() -> str | None:
    """Unit System can be set with ENV:UNIT_SYSTEM or user prompt. Allowed values are always SI or imperial."""
    return get_env_or_select(env_var="UNIT_SYSTEM", choices=["SI", "imperial"], message="Select a unit system to use:", default="SI")


def get_include_pdf() -> bool | None:
    """Include PDF can be set with ENV:UNIT_SYSTEM or user prompt. Allowed values are always SI or imperial."""
    return get_env_or_confirm(env_var="INCLUDE_PDF", message="Include a PDF version of the report?", default="false")
