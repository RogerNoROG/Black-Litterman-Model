"""入口：拉取或读入数据 → Black-Litterman → 回测作图。"""
import sys

from structures import *
from black_litterman import BlackLitterman
from back_test import BackTest
from data_providers import fetch_price_market_pair

if __name__ == "__main__":
    # 非 TTY / 部分 IDE 运行时会块缓冲 stdout，长时间拉数看起来像「没输出」
    try:
        sys.stdout.reconfigure(line_buffering=True)  # py3.7+
    except (AttributeError, OSError, ValueError):
        pass

    print("-" * 30, "Black-Litterman", "-" * 30, flush=True)
    print("View type:", VIEW_TYPE_NAME[VIEW_TYPE], flush=True)
    print(
        "正在加载行情（主源可能较慢或重试；失败将按 DATA_SOURCE_FALLBACK 切换）…",
        flush=True,
    )

    price_df, mv_df, src_used = fetch_price_market_pair()
    print("Data source:", src_used, end="", flush=True)
    if (DATA_SOURCE or "akshare").strip().lower() != src_used:
        print(f"（配置主源: {DATA_SOURCE!r}）", end="", flush=True)
    print(flush=True)
    print(
        f"数据就绪: price {price_df.shape}, mv {mv_df.shape}",
        flush=True,
    )
    bl = BlackLitterman(price_df=price_df, mv_df=mv_df)
    bl.get_cc_return()
    bl.get_market_value_weight()

    print("-" * 30, "Back test", "-" * 30, flush=True)
    if BACK_TEST_YEAR is not None:
        s_ix, e_ix = bl.backtest_iloc_range_for_year(BACK_TEST_YEAR)
        print(f"Year {BACK_TEST_YEAR}, iloc [{s_ix}, {e_ix}]", flush=True)
        bt = BackTest(start_index=s_ix, end_index=e_ix)
    else:
        bt = BackTest()
    bt.back_test(bl)
    print("完成。", flush=True)
