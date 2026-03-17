from __future__ import annotations

import pandas as pd
import streamlit as st

from china_fund_finder.agent import build_agent
from china_fund_finder.data import fetch_funds
from china_fund_finder.models import FundFilter, FundInfo


def _funds_to_dataframe(funds: list[FundInfo]) -> pd.DataFrame:
    """Convert a list of FundInfo objects to a DataFrame with Chinese column names."""
    df = pd.DataFrame([f.model_dump() for f in funds])
    df.columns = [
        "代码",
        "名称",
        "类型",
        "成立日期",
        "规模(亿)",
        "管理费(%)",
        "托管费(%)",
    ]
    return df


def _build_filter(
    fund_type: str,
    min_return_1y: float,
    min_return_3y: float,
    max_drawdown_limit: float,
    min_sharpe: float,
    min_size: float,
    max_management_fee: float,
    min_manager_tenure_years: float,
    min_listing_years: float,
) -> FundFilter:
    """Build a FundFilter from sidebar values, converting minimums to None."""
    return FundFilter(
        fund_type=None if fund_type == "全部" else fund_type,
        min_return_1y=None if min_return_1y <= -50.0 else min_return_1y,
        min_return_3y=None if min_return_3y <= -50.0 else min_return_3y,
        max_drawdown_limit=None if max_drawdown_limit <= -80.0 else max_drawdown_limit,
        min_sharpe=None if min_sharpe <= -2.0 else min_sharpe,
        min_size=None if min_size <= 0.0 else min_size,
        max_management_fee=None if max_management_fee >= 3.0 else max_management_fee,
        min_manager_tenure_years=(
            None if min_manager_tenure_years <= 0.0 else min_manager_tenure_years
        ),
        min_listing_years=None if min_listing_years <= 0.0 else min_listing_years,
    )


def _init_session_state() -> None:
    """Initialize Streamlit session state defaults."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent" not in st.session_state:
        st.session_state.agent = build_agent()
    if "filter_results" not in st.session_state:
        st.session_state.filter_results = None


def _render_sidebar() -> tuple[FundFilter, bool]:
    """Render sidebar filters and return (FundFilter, apply_clicked)."""
    st.sidebar.title("筛选条件")

    fund_type = st.sidebar.selectbox(
        "基金类型", ["全部", "股票型", "债券型", "混合型", "货币型", "指数型"]
    )

    min_return_1y = st.sidebar.slider(
        "近1年收益率下限 (%)", -50.0, 100.0, -50.0, step=0.5
    )
    min_return_3y = st.sidebar.slider(
        "近3年收益率下限 (%)", -50.0, 200.0, -50.0, step=1.0
    )

    max_drawdown_limit = st.sidebar.slider(
        "最大回撤上限 (%)", -80.0, 0.0, -80.0, step=1.0
    )
    min_sharpe = st.sidebar.slider("最低夏普比率", -2.0, 5.0, -2.0, step=0.1)

    min_size = st.sidebar.number_input(
        "最小规模 (亿元)", min_value=0.0, value=0.0, step=0.5
    )

    max_management_fee = st.sidebar.slider("最高管理费 (%)", 0.0, 3.0, 3.0, step=0.1)

    min_manager_tenure_years = st.sidebar.slider(
        "基金经理最短任职 (年)", 0.0, 20.0, 0.0, step=0.5
    )

    min_listing_years = st.sidebar.slider("基金最短上市 (年)", 0.0, 20.0, 0.0, step=0.5)

    apply_filters = st.sidebar.button("应用筛选")

    filters = _build_filter(
        fund_type=fund_type,
        min_return_1y=min_return_1y,
        min_return_3y=min_return_3y,
        max_drawdown_limit=max_drawdown_limit,
        min_sharpe=min_sharpe,
        min_size=min_size,
        max_management_fee=max_management_fee,
        min_manager_tenure_years=min_manager_tenure_years,
        min_listing_years=min_listing_years,
    )

    return filters, apply_filters


def _handle_filter_mode(filters: FundFilter) -> None:
    """Fetch and display funds using structured sidebar filters."""
    with st.spinner("正在筛选基金..."):
        funds = fetch_funds(filters)
    st.session_state.filter_results = funds

    if funds:
        st.success(f"找到 {len(funds)} 只基金")
        st.dataframe(_funds_to_dataframe(funds), use_container_width=True)
    else:
        st.info("未找到符合条件的基金，请调整筛选条件。")


def _handle_chat_mode(user_message: str) -> None:
    """Pass user message to the agent and display the response."""
    st.session_state.messages.append({"role": "user", "content": user_message})

    with st.chat_message("user"):
        st.markdown(user_message)

    with st.chat_message("assistant"):
        with st.spinner("思考中..."):
            result = st.session_state.agent.invoke(
                {"messages": [("human", user_message)]}
            )
            response_text = result["messages"][-1].content

        st.markdown(response_text)

    st.session_state.messages.append({"role": "assistant", "content": response_text})


def main() -> None:
    """Entry point for the Streamlit app."""
    st.title("中国基金发现工具")

    _init_session_state()

    filters, apply_filters = _render_sidebar()

    if apply_filters:
        _handle_filter_mode(filters)
    else:
        # Replay chat history
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        # Display previously fetched filter results if any
        if st.session_state.filter_results:
            funds = st.session_state.filter_results
            st.dataframe(_funds_to_dataframe(funds), use_container_width=True)

        user_input = st.chat_input(
            "请输入您的问题，例如：推荐近1年收益超过20%的股票型基金"
        )
        if user_input:
            _handle_chat_mode(user_input)


if __name__ == "__main__":
    main()
