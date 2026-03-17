from __future__ import annotations

from datetime import date

from pydantic import BaseModel, computed_field


class FundInfo(BaseModel):
    """Basic information about a Chinese mutual fund."""

    ts_code: str
    name: str
    fund_type: str
    inception_date: date | None = None
    fund_size: float | None = None
    management_fee: float | None = None
    custodian_fee: float | None = None


class FundManager(BaseModel):
    """Information about a fund manager and their tenure."""

    name: str
    ts_code: str
    start_date: date

    @computed_field  # type: ignore[prop-decorator]
    @property
    def tenure_years(self) -> float:
        """Compute manager tenure in years from start_date to today."""
        delta = date.today() - self.start_date
        return round(delta.days / 365.25, 2)


class FundPerformance(BaseModel):
    """Performance metrics for a fund over various time horizons."""

    ts_code: str
    return_1m: float | None = None
    return_3m: float | None = None
    return_6m: float | None = None
    return_1y: float | None = None
    return_3y: float | None = None
    return_5y: float | None = None
    max_drawdown: float | None = None
    sharpe: float | None = None
    annualized_return: float | None = None
    avg_annualized_return: float | None = None


class FundFilter(BaseModel):
    """Filter criteria for screening funds."""

    fund_type: str | None = None
    min_return_1y: float | None = None
    min_return_3y: float | None = None
    min_return_5y: float | None = None
    max_drawdown_limit: float | None = None
    min_sharpe: float | None = None
    min_size: float | None = None
    max_management_fee: float | None = None
    min_manager_tenure_years: float | None = None
    min_listing_years: float | None = None
    min_annualized_return: float | None = None
    min_avg_annualized_return: float | None = None
