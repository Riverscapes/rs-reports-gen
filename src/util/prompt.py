"""Utilities to handle interactive prompts consistently."""
from __future__ import annotations

from typing import Any, Mapping, Sequence

import inquirer
from termcolor import colored


QuestionSequence = Sequence[Any]


def safe_prompt(
    questions: QuestionSequence,
    *,
    exit_message: str = "Input cancelled by user. Exiting.",
    exit_code: int = 0,
    message_color: str = "yellow",
) -> Mapping[str, Any]:
    """Run an inquirer prompt and exit cleanly if the user cancels."""

    try:
        answers = inquirer.prompt(list(questions))
    except KeyboardInterrupt as exc:
        print(colored(f"KeyboardInterrupt \n{exit_message}\n", message_color))
        raise SystemExit(exit_code) from exc

    if answers is None:
        print(colored(f"\n{exit_message}\n", message_color))
        raise SystemExit(exit_code)

    return answers


def prompt_for(
    questions: QuestionSequence,
    key: str,
    *,
    exit_message: str = "Input cancelled by user. Exiting.",
    exit_code: int = 0,
    message_color: str = "yellow",
) -> Any:
    """Shortcut to run a prompt and pull a single answer by key."""

    answers = safe_prompt(
        questions,
        exit_message=exit_message,
        exit_code=exit_code,
        message_color=message_color,
    )

    if key not in answers:
        raise KeyError(f"Prompt did not return an answer for '{key}'.")

    return answers[key]
