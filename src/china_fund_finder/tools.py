from __future__ import annotations

from langchain_core.tools import tool

from china_fund_finder.data import (
    fetch_fund_performance,
    fetch_funds,
    fetch_manager_info,
)
from china_fund_finder.models import FundFilter, FundInfo, FundManager, FundPerformance


@tool
def search_funds(filters: FundFilter) -> list[FundInfo]:
    """Search for funds matching the given filter criteria.

    Use this tool when the user wants to find funds based on criteria like
    fund type, returns, drawdown, size, fees, manager tenure, or listing age.
    """
    return fetch_funds(filters)


@tool
def get_fund_detail(ts_code: str) -> FundPerformance:
    """Get detailed performance metrics for a specific fund by its Tushare code.

    Use this tool when the user asks for detailed performance data about a
    specific fund, including returns over different periods, drawdown, and Sharpe ratio.
    """
    return fetch_fund_performance(ts_code)


@tool
def get_manager_info(ts_code: str) -> list[FundManager]:
    """Get information about the manager(s) of a specific fund.

    Use this tool when the user asks about who manages a fund, the manager's
    tenure, or wants to filter by management experience.
    """
    return fetch_manager_info(ts_code)
