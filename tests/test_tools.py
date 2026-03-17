from __future__ import annotations

from datetime import date
from unittest.mock import patch

from china_fund_finder.models import FundFilter, FundInfo, FundManager, FundPerformance
from china_fund_finder.tools import get_fund_detail, get_manager_info, search_funds


def _make_fund_info(ts_code: str = "001001.OF") -> FundInfo:
    return FundInfo(ts_code=ts_code, name="华夏成长", fund_type="股票型")


def test_search_funds_returns_list() -> None:
    """search_funds.invoke returns a list with one FundInfo."""
    expected = [_make_fund_info()]
    with patch("china_fund_finder.tools.fetch_funds", return_value=expected):
        result = search_funds.invoke({"filters": FundFilter()})
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], FundInfo)


def test_get_fund_detail_returns_performance() -> None:
    """get_fund_detail.invoke returns FundPerformance with correct ts_code."""
    expected = FundPerformance(ts_code="001001.OF")
    with patch("china_fund_finder.tools.fetch_fund_performance", return_value=expected):
        result = get_fund_detail.invoke({"ts_code": "001001.OF"})
    assert isinstance(result, FundPerformance)
    assert result.ts_code == "001001.OF"


def test_get_manager_info_returns_list() -> None:
    """get_manager_info.invoke returns a list with one FundManager."""
    expected = [
        FundManager(name="Zhang Wei", ts_code="001001.OF", start_date=date(2020, 1, 1))
    ]
    with patch("china_fund_finder.tools.fetch_manager_info", return_value=expected):
        result = get_manager_info.invoke({"ts_code": "001001.OF"})
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], FundManager)


def test_search_funds_empty_filter() -> None:
    """search_funds.invoke returns [] when fetch_funds returns empty list."""
    with patch("china_fund_finder.tools.fetch_funds", return_value=[]):
        result = search_funds.invoke({"filters": FundFilter()})
    assert result == []


def test_search_funds_with_fund_type_filter() -> None:
    """search_funds passes fund_type filter through to fetch_funds."""
    captured: list[FundFilter] = []

    def _capture(filters: FundFilter) -> list[FundInfo]:
        captured.append(filters)
        return []

    with patch("china_fund_finder.tools.fetch_funds", side_effect=_capture):
        search_funds.invoke({"filters": FundFilter(fund_type="混合型")})

    assert len(captured) == 1
    assert captured[0].fund_type == "混合型"
