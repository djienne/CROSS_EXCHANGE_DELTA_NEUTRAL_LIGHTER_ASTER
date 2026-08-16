"""Configuration loading must never silently substitute defaults.

The regression these guard against: `load_from_file` filtered comment keys with
`not k.startswith('comment')`, so the `_comment_*` keys in config.json survived, reached
`BotConfig(**data)`, raised TypeError, and were swallowed by a broad `except` that
returned an all-defaults config. The bot then ran at 3x leverage, an 8h hold and a 5%
APR gate while config.json asked for 1x, 24h and 60% - i.e. it traded a materially
different, structurally unprofitable strategy and reported nothing wrong.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lighter_aster_hedge import BotConfig, ConfigError  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REAL_CONFIG = os.path.join(REPO_ROOT, "config.json")


def write(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


@pytest.fixture
def base():
    with open(REAL_CONFIG, encoding="utf-8") as fh:
        return json.load(fh)


def test_real_config_loads_the_values_it_states(base):
    """The shipped config.json must produce the strategy it describes."""
    config = BotConfig.load_from_file(REAL_CONFIG)

    assert config.leverage == base["leverage"]
    assert config.notional_per_position == base["notional_per_position"]
    assert config.hold_duration_hours == base["hold_duration_hours"]
    assert config.min_net_apr_threshold == base["min_net_apr_threshold"]
    assert config.symbols_to_monitor == base["symbols_to_monitor"]


def test_underscore_comment_keys_are_ignored_not_fatal(tmp_path, base):
    """`_comment_*` is the convention that broke the old parser."""
    base["_comment_anything"] = "prose"
    base["comment_legacy_style"] = "also prose"
    base["_COMMENT_UPPER"] = "case-insensitive"

    config = BotConfig.load_from_file(write(tmp_path, base))
    assert config.hold_duration_hours == base["hold_duration_hours"]


def test_unknown_key_is_fatal(tmp_path, base):
    """A typo in a risk parameter must not silently fall back to a default."""
    base["min_net_apr_threshhold"] = 60.0          # note the typo
    with pytest.raises(ConfigError, match="unknown configuration key"):
        BotConfig.load_from_file(write(tmp_path, base))


def test_missing_file_is_fatal():
    with pytest.raises(ConfigError, match="not found"):
        BotConfig.load_from_file("no_such_config_file.json")


def test_malformed_json_is_fatal(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ConfigError):
        BotConfig.load_from_file(str(path))


@pytest.mark.parametrize("key,value", [
    ("hold_duration_hours", 0),         # divides by zero in break_even_apr_pct
    ("hold_duration_hours", -1),
    ("leverage", 0),
    ("leverage", -3),
    ("notional_per_position", 0),
    ("capital_safety_margin", 0),
    ("capital_safety_margin", 1.5),
    ("max_spread_pct", 0),
    ("check_interval_seconds", 0),
    ("min_net_apr_threshold", -1),
])
def test_out_of_range_values_are_fatal(tmp_path, base, key, value):
    base[key] = value
    with pytest.raises(ConfigError, match="invalid configuration"):
        BotConfig.load_from_file(write(tmp_path, base))


def test_empty_symbol_list_is_fatal(tmp_path, base):
    base["symbols_to_monitor"] = []
    with pytest.raises(ConfigError):
        BotConfig.load_from_file(write(tmp_path, base))


def test_omitted_optional_key_uses_the_dataclass_default(tmp_path, base):
    del base["capital_safety_margin"]
    config = BotConfig.load_from_file(write(tmp_path, base))
    assert config.capital_safety_margin == 0.95


def test_reload_is_atomic_and_keeps_old_values_on_a_bad_edit(tmp_path, base):
    """A half-saved config must not leave the bot on a mixture of old and new."""
    path = write(tmp_path, base)
    config = BotConfig.load_from_file(path)
    original_leverage = config.leverage
    original_hold = config.hold_duration_hours

    # A file that changes leverage AND is invalid must apply neither.
    bad = dict(base, leverage=5, hold_duration_hours=-1)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(bad, fh)

    assert config.reload(path) is False
    assert config.leverage == original_leverage
    assert config.hold_duration_hours == original_hold


def test_reload_applies_a_valid_edit(tmp_path, base):
    path = write(tmp_path, base)
    config = BotConfig.load_from_file(path)

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(dict(base, min_net_apr_threshold=75.0), fh)

    assert config.reload(path) is True
    assert config.min_net_apr_threshold == 75.0
