"""Black-Litterman 模型：数据读入、先验/观点、后验收益与权重、等权对照收益。"""
import pandas as pd
import numpy as np
from data_json import load_market_value_dataframe, load_price_dataframe
from structures import *


class BlackLitterman:

    def __init__(self, price_df=None, mv_df=None):
        self._inline_price_df = price_df
        self._inline_mv_df = mv_df
        self.price_path = PRICE_DATA_PATH
        self.mv_path = MV_DATA_PATH
        self.tau = TAU
        self.back_test_T = BACK_TEST_T
        self.view_type = VIEW_TYPE
        self.view_T = VIEW_T
        self.stock_cc_ret = 0
        self.stock_names = 0
        self.stock_number = 0
        self.market_value_weight = 0
        self.index_num = INDEX_NUMBER
        self.index_name = 0
        self.index_cc_ret = 0
        self._price_row_dates = None

    def read_data(self, filepath):
        if self._inline_price_df is not None and filepath == self.price_path:
            df = self._inline_price_df.copy()
        elif self._inline_mv_df is not None and filepath == self.mv_path:
            df = self._inline_mv_df.copy()
        elif filepath == self.price_path:
            df = load_price_dataframe(filepath)
        elif filepath == self.mv_path:
            df = load_market_value_dataframe(filepath)
        else:
            raise ValueError(f"未知数据路径: {filepath}")
        if filepath == self.price_path:
            self._price_row_dates = pd.to_datetime(df["Date"]).reset_index(drop=True)
        df.set_index("Date", inplace=True)
        df.index = range(len(df))
        df = df.astype("float64")
        return df

    def backtest_iloc_range_for_year(self, year: int) -> tuple:
        if self._price_row_dates is None or len(self._price_row_dates) == 0:
            raise RuntimeError("缺少价格日历：请先调用 get_cc_return()")
        cal = self._price_row_dates
        end_dates = pd.to_datetime(cal.iloc[1:]).reset_index(drop=True)
        mask = end_dates.dt.year == int(year)
        idx = np.flatnonzero(mask.to_numpy())
        if len(idx) == 0:
            raise ValueError(f"数据中不包含自然年 {year} 的周度收益")
        start_k, end_k = int(idx[0]), int(idx[-1])
        t = self.back_test_T
        if start_k < t:
            raise ValueError(
                f"该年首个可用 iloc={start_k} 小于估计窗口 BACK_TEST_T={t}，"
                f"请提早 AKSHARE_START_DATE（或改小 BACK_TEST_T）"
            )
        return start_k, end_k

    def get_cc_return(self):
        index_num = self.index_num
        df = self.read_data(self.price_path)
        log_ret = np.log(df / df.shift())
        log_ret = log_ret.drop(index=[0])
        names = log_ret.columns.tolist()
        index_name = names[index_num]
        stock_names = names[3:]
        self.index_cc_ret = log_ret[index_name]
        self.stock_cc_ret = log_ret[stock_names]
        self.index_name = index_name
        self.stock_names = stock_names
        self.stock_number = len(stock_names)

    def get_market_value_weight(self):
        mv = self.read_data(self.mv_path)
        stock_names = mv.columns.tolist()[0:-1]
        for n in stock_names:
            mv[n] = mv[n] / mv["Total"]
        mv = mv.drop(index=[0])
        mv = mv[stock_names]
        self.market_value_weight = np.array(mv)

    def get_implied_excess_equilibrium_return(self, stock_cc_ret, w_mkt):
        rf = float(np.log(1.0 + 0.025) / 52.0)
        mkt_cov = np.array(stock_cc_ret.cov())
        lambd = ((np.dot(w_mkt, stock_cc_ret.mean())) - rf) / np.dot(
            np.dot(w_mkt, mkt_cov), w_mkt.T
        )
        implied_ret = lambd * np.dot(mkt_cov, w_mkt)
        return implied_ret, lambd

    def get_views_P_Q_matrix(self, view_type, stock_cc_ret):
        N = self.stock_number
        if view_type == 0 or view_type == 1:
            # 0/1 共用 P,Q；0 在 get_post_weight 中改用市值权
            # 列顺序见 data_akshare.STOCKS
            P = np.zeros([3, N])
            P[0, 8] = 1
            P[0, 9] = -1
            P[1, 1] = 1
            P[1, 3] = -1
            P[2, 3] = 0.1
            P[2, 5] = 0.9
            P[2, 7] = -0.1
            P[2, 6] = -0.9
            Q = np.array([0.0001, 0.00025, 0.0001])
        elif view_type == 2:
            P = np.zeros([1, N])
            P[0, 1] = 1
            P[0, 3] = -1
            Q = [0.017]
        elif view_type == 3:
            T_near = self.view_T
            P = np.identity(N)
            stock_cc_ret_near = stock_cc_ret.iloc[-T_near:]
            Q = np.array(stock_cc_ret_near.mean())
        else:
            raise ValueError(f"未知 view_type: {view_type}")
        return P, Q

    def get_views_omega(self, mkt_cov, P):
        tau = self.tau
        K = len(P)
        omega = np.identity(K)
        for i in range(K):
            P_i = P[i]
            omg_i = np.dot(np.dot(P_i, mkt_cov), P_i.T) * tau
            omega[i][i] = omg_i
        return omega

    def get_posterior_combined_return(self, implied_ret, mkt_cov, P, Q, omega):
        tau = self.tau
        k = np.linalg.inv(
            np.linalg.inv(tau * mkt_cov)
            + np.dot(np.dot(P.T, np.linalg.inv(omega)), P)
        )
        posterior_ret = np.dot(
            k,
            np.dot(np.linalg.inv(tau * mkt_cov), implied_ret)
            + np.dot(np.dot(P.T, np.linalg.inv(omega)), Q),
        )
        return posterior_ret

    def get_weight_bl(self, posterior_ret, mkt_cov, lambd):
        return np.dot(np.linalg.inv(lambd * mkt_cov), posterior_ret)

    def get_post_weight(self, start_idx):
        T = self.back_test_T
        view_type = self.view_type
        index_cc_ret, stock_cc_ret = self.index_cc_ret, self.stock_cc_ret
        real_ret = np.array(stock_cc_ret.iloc[start_idx])
        stock_cc_ret = stock_cc_ret.iloc[start_idx - T : start_idx]
        index_cc_ret = index_cc_ret.iloc[start_idx - T : start_idx]
        mkt_cov = np.array(stock_cc_ret.cov())
        mv_i = self.market_value_weight[start_idx - 1]
        implied_ret, lambd = self.get_implied_excess_equilibrium_return(
            stock_cc_ret, mv_i
        )
        P, Q = self.get_views_P_Q_matrix(view_type, stock_cc_ret)
        omega = self.get_views_omega(mkt_cov, P)
        posterior_ret = self.get_posterior_combined_return(
            implied_ret, mkt_cov, P, Q, omega
        )
        if view_type == 0:
            weight_bl = np.array(mv_i)
        elif view_type in (1, 2, 3):
            weight_bl = self.get_weight_bl(posterior_ret, mkt_cov, lambd)
        else:
            raise ValueError(f"未知 view_type: {view_type}")
        return weight_bl, real_ret

    def get_post_weight_with_sentiment_views(
        self,
        start_idx,
        P,
        Q,
        omega,
    ):
        """
        使用情绪/LLM 生成的绝对观点 (P, Q, Omega) 替代 ``get_views_P_Q_matrix``，
        其余与 ``get_post_weight`` 相同：同一估计窗内的 ``implied_ret``、``mkt_cov``、``lambd``。

        ``omega`` 可为长度 k 的一维对角元，或 k×k 矩阵。
        若 k==0（无观点），退化为当期市值权重（与 ``view_type==0`` 一致）。
        """
        T = self.back_test_T
        stock_cc_ret = self.stock_cc_ret.iloc[start_idx - T : start_idx]
        mkt_cov = np.array(stock_cc_ret.cov())
        mv_i = self.market_value_weight[start_idx - 1]
        implied_ret, lambd = self.get_implied_excess_equilibrium_return(
            stock_cc_ret, mv_i
        )
        real_ret = np.array(self.stock_cc_ret.iloc[start_idx])

        P = np.asarray(P, dtype=float)
        Q = np.asarray(Q, dtype=float).reshape(-1)
        omega = np.asarray(omega, dtype=float)
        if P.size == 0 or Q.size == 0:
            return np.array(mv_i), real_ret
        if omega.ndim == 1:
            omega_mat = np.diag(omega)
        else:
            omega_mat = omega

        posterior_ret = self.get_posterior_combined_return(
            implied_ret, mkt_cov, P, Q, omega_mat
        )
        weight_bl = self.get_weight_bl(posterior_ret, mkt_cov, lambd)
        return weight_bl, real_ret

    def calculate_comparative_return(self, start_idx, end_index):
        stock_names = self.stock_names
        stock_cc_ret = self.stock_cc_ret.iloc[start_idx : end_index + 1]
        stock_cc_ret = stock_cc_ret.copy()
        stock_cc_ret["mean"] = stock_cc_ret.loc[:, stock_names].mean(axis=1)
        eq_acc = [0]
        for r in np.array(stock_cc_ret["mean"]):
            eq_acc.append(eq_acc[-1] + r)
        return eq_acc
