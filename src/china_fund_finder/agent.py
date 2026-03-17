from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent

from china_fund_finder.tools import (
    get_fund_detail,
    get_manager_info,
    search_funds,
)

SYSTEM_PROMPT = (
    "你是一位专业的基金投资顾问助手，帮助用户查找和筛选适合的中国公募基金。\n"
    "你可以根据用户的投资需求（收益率、风险偏好、基金规模、基金经理任职年限等）"
    "来搜索和推荐基金。\n"
    "请使用可用的工具来获取实时基金数据，并以清晰、专业的方式为用户提供投资建议。"
)


def build_agent() -> CompiledStateGraph:
    """Build and return the fund advisor ReAct agent.

    Creates a ReAct agent with the Claude Sonnet 4.6 model and provides
    access to fund search, detailed fund information, and fund manager data.
    """
    model = ChatAnthropic(model="claude-sonnet-4-6")
    tools = [search_funds, get_fund_detail, get_manager_info]

    agent = create_react_agent(
        model,
        tools=tools,
        prompt=SYSTEM_PROMPT,
    )

    return agent
