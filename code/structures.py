# 原始数据
## 数据源：'akshare' 在线拉取；'json' 从本地 JSON 读盘
DATA_SOURCE = "akshare"

## 周频股价 / 市值（JSON，UTF-8；DATA_SOURCE='json' 时使用）
PRICE_DATA_PATH = "./Data/price_data.json"
MV_DATA_PATH = "./Data/market_value_data.json"

## AkShare 拉取区间（需覆盖 BACK_TEST_YEAR 且留出 BACK_TEST_T 周估计窗；约 200 周≈4 年）
AKSHARE_START_DATE = "2020-01-01"
AKSHARE_END_DATE = "2025-12-31"
AKSHARE_ADJUST = "qfq"
## 为 True 时在拉取后写入 PRICE_DATA_PATH / MV_DATA_PATH
AKSHARE_SAVE_JSON_AFTER_FETCH = False

# 模型参数
TAU = 0.3           # 后验期望收益率协方差矩阵的放缩尺度，取值在0~1之间

# 模型回测
## 回测参数
BACK_TEST_T = 200   # 回测时间T窗口：200期
## 指定自然年时，main 会根据周度日期自动解析 iloc，忽略下一行的 START_INDEX / END_INDEX
BACK_TEST_YEAR = 2025
START_INDEX = 273   # 仅当 BACK_TEST_YEAR 为 None 时生效（兼容旧配置）
END_INDEX = 324
INDEX_NUMBER = 0    # 股指列索引：0 沪深300，1 上证综指，2 深证成指

## 绘图参数
BACK_TEST_X_LABEL = 'Week'
BACK_TEST_Y_LABEL = 'Accumulated Return(log)'
BACK_TEST_PERIOD_NAME = '2025'

# 观点参数
VIEW_TYPE = 2       # 对观点列表进行索引
VIEW_TYPE_NAME = ['Market value as view', "Arbitrary views", "Reasonable views", "Near period return as view"]
VIEW_T = 10         # 当观点为"Near period return as view"时，需要定义近期参数，即取VIEW_T期历史收益率求平均值，作为预期收益率
