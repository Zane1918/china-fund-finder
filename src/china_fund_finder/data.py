from __future__ import annotations

import math
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any

import tushare as ts
from dotenv import load_dotenv

from china_fund_finder.models import FundFilter, FundInfo, FundManager, FundPerformance

load_dotenv()

DB_PATH = "fund_cache.db"
_token = os.environ.get("TUSHARE_TOKEN", "")
if _token:
    ts.set_token(_token)

_pro: Any = None


def _get_pro() -> Any:
    """Return (and lazily initialise) the Tushare pro API client."""
    global _pro
    if _pro is None:
        _pro = ts.pro_api()
    return _pro


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

_CREATE_FUNDS = """
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

_CREATE_MANAGERS = """
CREATE TABLE IF NOT EXISTS managers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code    TEXT,
    name       TEXT,
    start_date TEXT,
    end_date   TEXT,
    cached_at  TEXT
)
"""

_CREATE_NAV_CACHE = """
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

_CREATE_FUND_SHARE = """
CREATE TABLE IF NOT EXISTS fund_share (
    ts_code  TEXT PRIMARY KEY,
    fd_share REAL,
    cached_at TEXT
)
"""


def get_db_connection() -> sqlite3.Connection:
    """Return a connection to the SQLite cache DB, creating tables if needed."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(_CREATE_FUNDS)
    cur.execute(_CREATE_MANAGERS)
    cur.execute(_CREATE_NAV_CACHE)
    cur.execute(_CREATE_FUND_SHARE)
    conn.commit()
    return conn


def ensure_cache_fresh(table: str, max_age_hours: int = 24) -> bool:
    """Return True if cache is fresh (youngest record < max_age_hours old).

    Returns False if the table is empty or all records are stale.
    """
    conn = get_db_connection()
    try:
        row = conn.execute(
            f"SELECT MAX(cached_at) AS newest FROM {table}"  # noqa: S608
        ).fetchone()
    finally:
        conn.close()

    if row is None or row["newest"] is None:
        return False

    newest = datetime.fromisoformat(row["newest"])
    age = datetime.now() - newest
    return age < timedelta(hours=max_age_hours)


# ---------------------------------------------------------------------------
# Fund list + basic info
# ---------------------------------------------------------------------------


def _refresh_funds_cache() -> None:
    """Fetch fund_basic from Tushare and upsert into local cache."""
    pro = _get_pro()
    df = pro.fund_basic(
        market="E",
        fields="ts_code,name,fund_type,found_date,management,custodian,m_fee,c_fee",
    )

    # Also pull latest fund share (size) in one batch where possible
    now_iso = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO funds
                    (ts_code, name, fund_type, inception_date,
                     management_fee, custodian_fee, cached_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ts_code) DO UPDATE SET
                    name           = excluded.name,
                    fund_type      = excluded.fund_type,
                    inception_date = excluded.inception_date,
                    management_fee = excluded.management_fee,
                    custodian_fee  = excluded.custodian_fee,
                    cached_at      = excluded.cached_at
                """,
                (
                    row["ts_code"],
                    row["name"],
                    row["fund_type"],
                    row.get("found_date") or None,
                    row.get("m_fee") if not _is_nan(row.get("m_fee")) else None,
                    row.get("c_fee") if not _is_nan(row.get("c_fee")) else None,
                    now_iso,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _is_nan(value: Any) -> bool:
    """Return True if value is a float NaN."""
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return False


def _refresh_fund_sizes() -> None:
    """Pull latest fund share (size) for all cached funds."""
    conn = get_db_connection()
    try:
        codes = [r[0] for r in conn.execute("SELECT ts_code FROM funds").fetchall()]
    finally:
        conn.close()

    if not codes:
        return

    pro = _get_pro()
    now_iso = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        for code in codes:
            try:
                df = pro.fund_share(ts_code=code, fields="ts_code,fd_share")
                if df.empty:
                    continue
                latest = df.sort_values("fd_share", ascending=False).iloc[0]
                fd_share = (
                    float(latest["fd_share"])
                    if not _is_nan(latest["fd_share"])
                    else None
                )
                conn.execute(
                    """
                    INSERT INTO fund_share (ts_code, fd_share, cached_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(ts_code) DO UPDATE SET
                        fd_share  = excluded.fd_share,
                        cached_at = excluded.cached_at
                    """,
                    (code, fd_share, now_iso),
                )
                # Propagate size back to funds table
                conn.execute(
                    "UPDATE funds SET fund_size = ? WHERE ts_code = ?",
                    (fd_share, code),
                )
            except Exception:
                continue
        conn.commit()
    finally:
        conn.close()


def fetch_funds(filters: FundFilter) -> list[FundInfo]:
    """Query cached fund data applying filters.

    Refreshes the cache if stale (>24 h).  Filters for fund_type,
    fund_size, and management_fee are applied in SQL; performance-based
    filters are left to the caller after enriching with FundPerformance.
    """
    if not ensure_cache_fresh("funds"):
        _refresh_funds_cache()

    conditions: list[str] = []
    params: list[Any] = []

    if filters.fund_type is not None:
        conditions.append("fund_type = ?")
        params.append(filters.fund_type)
    if filters.min_size is not None:
        conditions.append("fund_size >= ?")
        params.append(filters.min_size)
    if filters.max_management_fee is not None:
        conditions.append("(management_fee IS NULL OR management_fee <= ?)")
        params.append(filters.max_management_fee)
    if filters.min_listing_years is not None:
        cutoff = (
            date.today() - timedelta(days=filters.min_listing_years * 365.25)
        ).isoformat()
        conditions.append("inception_date <= ?")
        params.append(cutoff)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT * FROM funds {where}"  # noqa: S608

    conn = get_db_connection()
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    results: list[FundInfo] = []
    for row in rows:
        inception: date | None = None
        if row["inception_date"]:
            try:
                inception = date.fromisoformat(row["inception_date"])
            except ValueError:
                pass
        results.append(
            FundInfo(
                ts_code=row["ts_code"],
                name=row["name"],
                fund_type=row["fund_type"],
                inception_date=inception,
                fund_size=row["fund_size"],
                management_fee=row["management_fee"],
                custodian_fee=row["custodian_fee"],
            )
        )
    return results


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def _compute_return(
    navs: list[float], dates: list[date], days_back: int
) -> float | None:
    """Return percentage return over the last `days_back` calendar days."""
    if not navs:
        return None
    latest_date = dates[-1]
    target = latest_date - timedelta(days=days_back)
    # Find closest date at or before target
    candidates = [(d, n) for d, n in zip(dates, navs) if d <= target]
    if not candidates:
        return None
    _, past_nav = candidates[-1]
    if past_nav == 0:
        return None
    return (navs[-1] - past_nav) / past_nav * 100


def _compute_max_drawdown(accum_navs: list[float]) -> float | None:
    """Compute peak-to-trough max drawdown (negative value)."""
    if len(accum_navs) < 2:
        return None
    peak = accum_navs[0]
    max_dd = 0.0
    for nav in accum_navs[1:]:
        if nav > peak:
            peak = nav
        dd = (nav - peak) / peak
        if dd < max_dd:
            max_dd = dd
    return max_dd * 100  # as percentage


def _compute_annualized_return(navs: list[float], dates: list[date]) -> float | None:
    """Compute annualized return as percentage over full NAV history."""
    if len(navs) < 2 or navs[0] == 0:
        return None
    total_days = (dates[-1] - dates[0]).days
    if total_days < 1:
        return None
    ratio = navs[-1] / navs[0]
    if ratio <= 0:
        return None
    return (ratio ** (365 / total_days) - 1) * 100


def _compute_avg_annualized_return(
    navs: list[float], dates: list[date]
) -> float | None:
    """Geometric mean of each full calendar year's annualized return.

    Skips partial years with fewer than 200 data points.
    """
    if len(navs) < 2:
        return None

    # Group by calendar year
    from collections import defaultdict

    year_data: dict[int, list[tuple[date, float]]] = defaultdict(list)
    for d, n in zip(dates, navs):
        year_data[d.year].append((d, n))

    annual_returns: list[float] = []
    for year, points in sorted(year_data.items()):
        if len(points) < 200:
            continue
        points.sort()
        start_nav = points[0][1]
        end_nav = points[-1][1]
        if start_nav <= 0:
            continue
        days = (points[-1][0] - points[0][0]).days
        if days < 1:
            continue
        ann = (end_nav / start_nav) ** (365 / days) - 1
        annual_returns.append(ann)

    if not annual_returns:
        return None

    # Geometric mean
    product = 1.0
    for r in annual_returns:
        product *= 1 + r
    geo_mean = product ** (1 / len(annual_returns)) - 1
    return geo_mean * 100


def _compute_sharpe(navs: list[float], annualized_return: float | None) -> float | None:
    """Compute Sharpe ratio using daily returns and a 3% risk-free rate."""
    if annualized_return is None or len(navs) < 30:
        return None

    daily_returns: list[float] = []
    for i in range(1, len(navs)):
        if navs[i - 1] == 0:
            continue
        daily_returns.append((navs[i] - navs[i - 1]) / navs[i - 1])

    if len(daily_returns) < 20:
        return None

    n = len(daily_returns)
    mean_dr = sum(daily_returns) / n
    variance = sum((r - mean_dr) ** 2 for r in daily_returns) / (n - 1)
    std_daily = math.sqrt(variance)
    if std_daily == 0:
        return None

    annualized_std = std_daily * math.sqrt(252)
    return (annualized_return - 3.0) / annualized_std


def _store_nav(
    conn: sqlite3.Connection,
    ts_code: str,
    nav_date: str,
    unit_nav: float | None,
    accum_nav: float | None,
    now_iso: str,
) -> None:
    conn.execute(
        """
        INSERT INTO nav_cache (ts_code, nav_date, unit_nav, accum_nav, cached_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ts_code, nav_date) DO UPDATE SET
            unit_nav  = excluded.unit_nav,
            accum_nav = excluded.accum_nav,
            cached_at = excluded.cached_at
        """,
        (ts_code, nav_date, unit_nav, accum_nav, now_iso),
    )


def fetch_fund_performance(ts_code: str) -> FundPerformance:
    """Fetch NAV history from Tushare and compute performance metrics."""
    pro = _get_pro()
    now_iso = datetime.now().isoformat()

    df = pro.fund_nav(
        ts_code=ts_code,
        fields="ts_code,nav_date,unit_nav,accum_nav",
    )

    conn = get_db_connection()
    try:
        if not df.empty:
            for _, row in df.iterrows():
                _store_nav(
                    conn,
                    ts_code,
                    row["nav_date"],
                    float(row["unit_nav"])
                    if not _is_nan(row.get("unit_nav"))
                    else None,
                    float(row["accum_nav"])
                    if not _is_nan(row.get("accum_nav"))
                    else None,
                    now_iso,
                )
            conn.commit()

        rows = conn.execute(
            "SELECT nav_date, unit_nav, accum_nav FROM nav_cache "
            "WHERE ts_code = ? ORDER BY nav_date ASC",
            (ts_code,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return FundPerformance(ts_code=ts_code)

    dates: list[date] = []
    navs: list[float] = []
    accum_navs: list[float] = []

    for r in rows:
        try:
            d = date.fromisoformat(r["nav_date"])
        except (ValueError, TypeError):
            continue
        if r["unit_nav"] is None:
            continue
        dates.append(d)
        navs.append(float(r["unit_nav"]))
        accum_val = r["accum_nav"]
        accum_navs.append(
            float(accum_val) if accum_val is not None else float(r["unit_nav"])
        )

    if not navs:
        return FundPerformance(ts_code=ts_code)

    annualized = _compute_annualized_return(navs, dates)

    return FundPerformance(
        ts_code=ts_code,
        return_1m=_compute_return(navs, dates, 30),
        return_3m=_compute_return(navs, dates, 90),
        return_6m=_compute_return(navs, dates, 180),
        return_1y=_compute_return(navs, dates, 365),
        return_3y=_compute_return(navs, dates, 3 * 365),
        return_5y=_compute_return(navs, dates, 5 * 365),
        max_drawdown=_compute_max_drawdown(accum_navs),
        annualized_return=annualized,
        avg_annualized_return=_compute_avg_annualized_return(navs, dates),
        sharpe=_compute_sharpe(navs, annualized),
    )


# ---------------------------------------------------------------------------
# Manager info
# ---------------------------------------------------------------------------


def fetch_manager_info(ts_code: str) -> list[FundManager]:
    """Fetch manager info for a fund.

    Uses cached data if fresh (<24 h), otherwise fetches from Tushare.
    """
    conn = get_db_connection()

    def _load_from_db() -> list[FundManager]:
        db_rows = conn.execute(
            "SELECT name, ts_code, start_date FROM managers "
            "WHERE ts_code = ? AND end_date IS NULL OR end_date = ''",
            (ts_code,),
        ).fetchall()
        managers: list[FundManager] = []
        for r in db_rows:
            try:
                sd = date.fromisoformat(r["start_date"])
            except (ValueError, TypeError):
                continue
            managers.append(
                FundManager(name=r["name"], ts_code=r["ts_code"], start_date=sd)
            )
        return managers

    # Check freshness for this fund's manager records
    row = conn.execute(
        "SELECT MAX(cached_at) as newest FROM managers WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()

    fresh = False
    if row and row["newest"]:
        age = datetime.now() - datetime.fromisoformat(row["newest"])
        fresh = age < timedelta(hours=24)

    if fresh:
        result = _load_from_db()
        conn.close()
        return result

    # Fetch from Tushare
    pro = _get_pro()
    now_iso = datetime.now().isoformat()
    try:
        df = pro.fund_manager(
            ts_code=ts_code,
            fields="ts_code,ann_date,name,start_date,end_date",
        )
    except Exception:
        result = _load_from_db()
        conn.close()
        return result

    try:
        # Remove old records for this fund and re-insert
        conn.execute("DELETE FROM managers WHERE ts_code = ?", (ts_code,))
        for _, r in df.iterrows():
            end_date = r.get("end_date")
            if _is_nan(end_date) if end_date is not None else False:
                end_date = None
            conn.execute(
                "INSERT INTO managers (ts_code, name, start_date, end_date, cached_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    ts_code,
                    r["name"],
                    r.get("start_date") or None,
                    end_date or None,
                    now_iso,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    # Re-open to load active managers (end_date null/empty)
    conn = get_db_connection()
    try:
        result = _load_from_db()
    finally:
        conn.close()
    return result
