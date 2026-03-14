import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import click

CONFIG_DIR = Path("~/.config/dbr-logs").expanduser()
CONFIG_PATH = CONFIG_DIR / "config.toml"


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def save_config(config: dict[str, Any]) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    lines = _serialize_toml(config)
    CONFIG_PATH.write_text(lines)


def get_default_profile(config: dict[str, Any]) -> str | None:
    profile: str | None = config.get("profile", {}).get("default")
    return profile


def get_default_env(config: dict[str, Any]) -> str:
    env: str = config.get("defaults", {}).get("env", "prod")
    return env


def list_databricks_profiles() -> list[str]:
    try:
        result = subprocess.run(
            ["databricks", "auth", "profiles"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return _parse_profiles_output(result.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []


def _parse_profiles_output(output: str) -> list[str]:
    profiles = []
    lines = output.strip().splitlines()
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        name = parts[0]
        if name in ("Name", "----", ""):
            continue
        profiles.append(name)
    return profiles


def interactive_profile_setup(available_profiles: list[str]) -> str:
    if not sys.stdin.isatty():
        raise click.UsageError(
            "Multiple Databricks profiles found but stdin is not a terminal. "
            f"Use --dbr-profile with one of: {', '.join(available_profiles)}"
        )

    click.echo("Available Databricks profiles:", err=True)
    for i, profile in enumerate(available_profiles, 1):
        click.echo(f"  {i}. {profile}", err=True)

    while True:
        try:
            choice = input(f"Select default profile [1-{len(available_profiles)}]: ")
            idx = int(choice) - 1
            if 0 <= idx < len(available_profiles):
                selected = available_profiles[idx]
                config = load_config()
                config.setdefault("profile", {})["default"] = selected
                save_config(config)
                click.echo(f"Saved default profile: {selected}", err=True)
                return selected
        except (ValueError, EOFError, KeyboardInterrupt) as err:
            raise click.UsageError(
                "Profile selection cancelled. "
                f"Use --dbr-profile with one of: {', '.join(available_profiles)}"
            ) from err
        click.echo("Invalid selection, try again.", err=True)


def _serialize_toml(data: dict[str, Any], prefix: str = "") -> str:
    lines: list[str] = []
    scalars = {k: v for k, v in data.items() if not isinstance(v, dict)}
    tables = {k: v for k, v in data.items() if isinstance(v, dict)}

    for k, v in scalars.items():
        lines.append(f"{k} = {_toml_value(v)}")

    for section, values in tables.items():
        section_key = f"{prefix}.{section}" if prefix else section
        lines.append(f"\n[{section_key}]")
        for k, v in values.items():
            if isinstance(v, dict):
                lines.append(_serialize_toml({k: v}, section_key))
            else:
                lines.append(f"{k} = {_toml_value(v)}")

    return "\n".join(lines) + "\n"


def _toml_value(v: object) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return f'"{v}"'
    return str(v)
