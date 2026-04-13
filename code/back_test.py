"""周频回测：BL 权重 × 实现收益累加，并与等权对照绘图。"""
import os
import numpy as np
import matplotlib.pyplot as plt
from structures import *


class BackTest:
    def __init__(self, start_index=None, end_index=None):
        self.start_index = START_INDEX if start_index is None else start_index
        self.end_index = END_INDEX if end_index is None else end_index

    def back_test(self, bl):
        start_index = self.start_index
        end_index = self.end_index
        ret_port_set = []
        for i in range(end_index - start_index):
            cur_idx = start_index + i
            weight_bl, real_ret = bl.get_post_weight(cur_idx)
            ret_port_set.append(np.dot(weight_bl, real_ret.T))
        acc_ret_port_set = self.get_accumulate_return(ret_port_set)
        eq_acc = bl.calculate_comparative_return(start_index, end_index)
        self.plot_return(acc_ret_port_set, eq_acc)

    def get_accumulate_return(self, ret_port_set):
        acc_ret_port_set = [0]
        for ret in ret_port_set:
            acc_ret_port_set.append(acc_ret_port_set[-1] + ret)
        return acc_ret_port_set

    def plot_return(self, acc_ret_port_set, eq_acc):
        n = len(acc_ret_port_set)
        x = np.arange(0, n, 1)
        type_name = VIEW_TYPE_NAME[VIEW_TYPE]
        plt.plot(x, eq_acc[:n], color="blue", label="Equal weight")
        plt.plot(x, acc_ret_port_set, color="red", label=str(type_name))
        plt.title(
            "BL Return Back Test_" + str(type_name) + "_Year " + BACK_TEST_PERIOD_NAME
        )
        plt.xlabel(BACK_TEST_X_LABEL)
        plt.ylabel(BACK_TEST_Y_LABEL)
        plt.legend()
        os.makedirs("./Plot", exist_ok=True)
        plt.savefig(
            "./Plot/"
            + "BL Return Back Test_"
            + str(type_name)
            + "_Year "
            + BACK_TEST_PERIOD_NAME
            + ".png"
        )
