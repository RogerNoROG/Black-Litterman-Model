"""入口：拉取或读入数据 → Black-Litterman → 回测作图。"""
from structures import *
from black_litterman import BlackLitterman
from back_test import BackTest


if __name__ == "__main__":
    print("-" * 30, "Black-Litterman", "-" * 30)
    print("View type:", VIEW_TYPE_NAME[VIEW_TYPE])

    price_df, mv_df = None, None
    if DATA_SOURCE == "akshare":
        import os
        from data_akshare import fetch_bl_tables
        from data_json import save_bl_tables_to_json

        price_df, mv_df = fetch_bl_tables(
            AKSHARE_START_DATE,
            AKSHARE_END_DATE,
            adjust=AKSHARE_ADJUST,
        )
        if AKSHARE_SAVE_JSON_AFTER_FETCH:
            ddir = os.path.dirname(os.path.abspath(PRICE_DATA_PATH))
            if ddir:
                os.makedirs(ddir, exist_ok=True)
            save_bl_tables_to_json(price_df, mv_df, PRICE_DATA_PATH, MV_DATA_PATH)
            print("AkShare → JSON:", PRICE_DATA_PATH, MV_DATA_PATH)

    bl = BlackLitterman(price_df=price_df, mv_df=mv_df)
    bl.get_cc_return()
    bl.get_market_value_weight()

    print("-" * 30, "Back test", "-" * 30)
    if BACK_TEST_YEAR is not None:
        s_ix, e_ix = bl.backtest_iloc_range_for_year(BACK_TEST_YEAR)
        print(f"Year {BACK_TEST_YEAR}, iloc [{s_ix}, {e_ix}]")
        bt = BackTest(start_index=s_ix, end_index=e_ix)
    else:
        bt = BackTest()
    bt.back_test(bl)
