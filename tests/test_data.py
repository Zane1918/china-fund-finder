from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from china_fund_finder.data import ensure_cache_fresh, fetch_funds, get_db_connection
from china_fund_finder.models import FundFilter, FundInfo, FundManager


def _make_in_memory_db() -> sqlite3.Connection:
    """Return a fresh in-memory DB with all tables created."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    with patch("china_fund_finder.data.get_db_connection", return_value=conn):
        # Re-run table creation SQL directly
        pass
    # Run DDL directly
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS funds (
            ts_code        TEXT PRIMARY KEY,
            name           TEXT,
            fund_type      TEXT,
            inception_date TEXT,
            fund_size      REAL,
            management_fee REAL,
            custodian_fee  REAL,
            cached_at      TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code    TEXT,
            name       TEXT,
            start_date TEXT,
            end_date   TEXT,
            cached_at  TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nav_cache (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code   TEXT,
            nav_date  TEXT,
            unit_nav  REAL,
            accum_nav REAL,
            cached_at TEXT,
            UNIQUE(ts_code, nav_date)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fund_share (
            ts_code  TEXT PRIMARY KEY,
            fd_share REAL,
            cached_at TEXT
        )
        """
    )
    conn.commit()
    return conn


def test_get_db_connection_creates_tables() -> None:
    """get_db_connection creates all 4 expected tables."""
    mem_conn = sqlite3.connect(":memory:")
    mem_conn.row_factory = sqlite3.Row

    with patch("china_fund_finder.data.sqlite3.connect", return_value=mem_conn):
        conn = get_db_connection()

    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "funds" in tables
    assert "managers" in tables
    assert "nav_cache" in tables
    assert "fund_share" in tables


def test_ensure_cache_fresh_empty_table() -> None:
    """ensure_cache_fresh returns False when funds table has no rows."""
    mem_conn = _make_in_memory_db()

    with patch("china_fund_finder.data.get_db_connection", return_value=mem_conn):
        result = ensure_cache_fresh("funds", 24)

    assert result is False


def test_ensure_cache_fresh_fresh_data() -> None:
    """ensure_cache_fresh returns True when cached_at is now."""
    mem_conn = _make_in_memory_db()
    now_iso = datetime.now().isoformat()
    mem_conn.execute(
        "INSERT INTO funds (ts_code, name, fund_type, cached_at) VALUES (?, ?, ?, ?)",
        ("001001.OF", "Test Fund", "股票型", now_iso),
    )
    mem_conn.commit()

    with patch("china_fund_finder.data.get_db_connection", return_value=mem_conn):
        result = ensure_cache_fresh("funds", 24)

    assert result is True


def test_ensure_cache_fresh_stale_data() -> None:
    """ensure_cache_fresh returns False when cached_at is 25 hours ago."""
    mem_conn = _make_in_memory_db()
    stale_iso = (datetime.now() - timedelta(hours=25)).isoformat()
    mem_conn.execute(
        "INSERT INTO funds (ts_code, name, fund_type, cached_at) VALUES (?, ?, ?, ?)",
        ("001001.OF", "Test Fund", "股票型", stale_iso),
    )
    mem_conn.commit()

    with patch("china_fund_finder.data.get_db_connection", return_value=mem_conn):
        result = ensure_cache_fresh("funds", 24)

    assert result is False


def _sample_fund_df() -> pd.DataFrame:
    """Return a sample DataFrame mimicking Tushare fund_basic output."""
    return pd.DataFrame(
        {
            "ts_code": ["001001.OF", "001002.OF", "001003.OF"],
            "name": ["华夏成长", "华夏大盘", "嘉实增长"],
            "fund_type": ["股票型", "股票型", "混合型"],
            "found_date": ["20010801", "20040812", "20030801"],
            "management": ["华夏基金", "华夏基金", "嘉实基金"],
            "custodian": ["建设银行", "建设银行", "工商银行"],
            "m_fee": [1.5, 1.5, 1.5],
            "c_fee": [0.25, 0.25, 0.25],
        }
    )


class _NoCloseConn:
    """Wrap a sqlite3.Connection so that close() is a no-op."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def close(self) -> None:
        pass

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


def _no_close(conn: sqlite3.Connection) -> _NoCloseConn:
    return _NoCloseConn(conn)


def test_fetch_funds_returns_fundinfo_list() -> None:
    """fetch_funds returns a non-empty list[FundInfo] with correct types."""
    mem_conn = _make_in_memory_db()
    mock_pro = MagicMock()
    mock_pro.fund_basic.return_value = _sample_fund_df()
    wrapped = _no_close(mem_conn)

    with (
        patch("china_fund_finder.data._get_pro", return_value=mock_pro),
        patch("china_fund_finder.data.get_db_connection", return_value=wrapped),
    ):
        results = fetch_funds(FundFilter())

    assert len(results) > 0
    assert all(isinstance(f, FundInfo) for f in results)
    assert all(isinstance(f.ts_code, str) for f in results)
    assert all(isinstance(f.name, str) for f in results)
    assert all(isinstance(f.fund_type, str) for f in results)


def test_fetch_funds_filter_by_fund_type() -> None:
    """fetch_funds returns only funds matching fund_type filter."""
    mem_conn = _make_in_memory_db()
    mock_pro = MagicMock()
    mock_pro.fund_basic.return_value = _sample_fund_df()
    wrapped = _no_close(mem_conn)

    with (
        patch("china_fund_finder.data._get_pro", return_value=mock_pro),
        patch("china_fund_finder.data.get_db_connection", return_value=wrapped),
    ):
        results = fetch_funds(FundFilter(fund_type="股票型"))

    assert len(results) > 0
    assert all(f.fund_type == "股票型" for f in results)


def test_fund_manager_tenure_years() -> None:
    """FundManager.tenure_years is > 4.0 for a start_date of 2020-01-01."""
    manager = FundManager(
        name="Zhang Wei", ts_code="001001.OF", start_date=date(2020, 1, 1)
    )
    assert manager.tenure_years > 4.0


def test_fund_manager_tenure_years_recent() -> None:
    """FundManager.tenure_years is approximately 0 for start_date = today."""
    manager = FundManager(name="Li Na", ts_code="001002.OF", start_date=date.today())
    assert manager.tenure_years < 0.01
