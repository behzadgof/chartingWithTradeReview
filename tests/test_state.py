"""Tests for server-side UI state persistence."""

from __future__ import annotations

import json
import threading

import pytest

from charts.server.state import (
    delete_state,
    load_all_state,
    load_state,
    save_state,
)


@pytest.fixture()
def state_dir(tmp_path):
    """Provide a temporary state directory."""
    return tmp_path / "chart_state"


class TestSaveAndLoad:
    def test_round_trip(self, state_dir):
        data = {"watchlist": ["AAPL", "MSFT"]}
        assert save_state(state_dir, "my_key", data) is True
        assert load_state(state_dir, "my_key") == data

    def test_load_missing_key(self, state_dir):
        assert load_state(state_dir, "nonexistent") is None

    def test_overwrite(self, state_dir):
        save_state(state_dir, "k", {"v": 1})
        save_state(state_dir, "k", {"v": 2})
        assert load_state(state_dir, "k") == {"v": 2}

    def test_various_json_types(self, state_dir):
        for val in [42, "hello", [1, 2, 3], True, None]:
            save_state(state_dir, "t", val)
            assert load_state(state_dir, "t") == val


class TestLoadAll:
    def test_load_all_empty(self, state_dir):
        assert load_all_state(state_dir) == {}

    def test_load_all_multiple(self, state_dir):
        save_state(state_dir, "alpha", {"a": 1})
        save_state(state_dir, "beta", {"b": 2})
        result = load_all_state(state_dir)
        assert result == {"alpha": {"a": 1}, "beta": {"b": 2}}

    def test_load_all_skips_corrupt(self, state_dir):
        save_state(state_dir, "good", [1])
        # Write corrupt JSON manually
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "bad.json").write_text("{invalid", encoding="utf-8")
        result = load_all_state(state_dir)
        assert result == {"good": [1]}


class TestDelete:
    def test_delete_existing(self, state_dir):
        save_state(state_dir, "doomed", "bye")
        assert delete_state(state_dir, "doomed") is True
        assert load_state(state_dir, "doomed") is None

    def test_delete_missing(self, state_dir):
        # missing_ok=True means this should still return True
        assert delete_state(state_dir, "nope") is True


class TestKeySanitization:
    @pytest.mark.parametrize("bad_key", [
        "../etc/passwd", "a/b", "a\\b", "foo bar", "key.json", "a+b",
    ])
    def test_reject_bad_keys(self, state_dir, bad_key):
        assert save_state(state_dir, bad_key, "x") is False
        assert load_state(state_dir, bad_key) is None
        assert delete_state(state_dir, bad_key) is False

    @pytest.mark.parametrize("good_key", [
        "orb_layout", "watchlist_1", "MyKey", "ABC123", "a", "A_B_C",
    ])
    def test_accept_good_keys(self, state_dir, good_key):
        assert save_state(state_dir, good_key, "ok") is True
        assert load_state(state_dir, good_key) == "ok"


class TestAtomicWrite:
    def test_file_exists_after_save(self, state_dir):
        save_state(state_dir, "test_key", {"data": True})
        path = state_dir / "test_key.json"
        assert path.exists()
        assert json.loads(path.read_text(encoding="utf-8")) == {"data": True}

    def test_no_tmp_file_left(self, state_dir):
        save_state(state_dir, "clean", 1)
        tmp = state_dir / "clean.tmp"
        assert not tmp.exists()

    def test_concurrent_writes(self, state_dir):
        """Multiple threads writing different keys should not corrupt."""
        errors = []

        def writer(key, value):
            try:
                save_state(state_dir, key, value)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, args=(f"key_{i}", i))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        for i in range(20):
            assert load_state(state_dir, f"key_{i}") == i
