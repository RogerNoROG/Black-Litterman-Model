# 原始数据
## 默认：AkShare 拉取沪深300指数（CSI300.GI）+ 上证综指、深证成指 + 沪深300 权重前列成分股周频行情/市值；
## 列顺序与 INDEX_NUMBER、Black-Litterman 观点矩阵下标一致（见 data_akshare.INDEX_SINA / STOCKS）。
## 数据源：akshare | json | csv | baostock | tushare（见 data_providers.fetch_price_market_pair）
DATA_SOURCE = "akshare"
## 主源异常（超时、断连、缺文件等）时依次尝试备用源；空列表则仅使用 DATA_SOURCE（固定 AkShare，失败即报错）
DATA_SOURCE_FALLBACK = []

## 周频股价 / 市值（JSON，UTF-8；DATA_SOURCE='json' 时使用）
PRICE_DATA_PATH = "./Data/price_data.json"
MV_DATA_PATH = "./Data/market_value_data.json"

## CSV（DATA_SOURCE='csv'）：与 JSON 相同列，UTF-8（建议带 BOM 用 utf-8-sig）
PRICE_CSV_PATH = "./Data/price_weekly.csv"
MV_CSV_PATH = "./Data/market_value_weekly.csv"

## AkShare 拉取区间（需覆盖 BACK_TEST_YEAR 且留出 BACK_TEST_T 周估计窗；约 200 周≈4 年）
AKSHARE_START_DATE = "2020-01-01"
AKSHARE_END_DATE = "2025-12-31"
AKSHARE_ADJUST = "qfq"
## 为 True 时在拉取后写入 PRICE_DATA_PATH / MV_DATA_PATH
AKSHARE_SAVE_JSON_AFTER_FETCH = False
## 东财/新浪接口易偶发断连：单次请求失败时的重试次数与指数退避（秒）
AKSHARE_HTTP_RETRIES = 6
AKSHARE_RETRY_BASE_SEC = 1.0
## A 股日 K：``AKSHARE_STOCK_HIST_SOURCES`` 为**优先级列表**；同一次 ``fetch_bl_tables`` 内
## 首次探测到可用源后会**持续优先该源**，该源整段失败后再按列表重新探测下一可用源。
## 取值：``eastmoney`` / ``em``、``tencent`` / ``tx`` / ``qq``；也可写逗号分隔字符串 ``"eastmoney,tencent"``。
## 仅影响个股；指数仍用 stock_zh_index_daily_em / 新浪备用。
AKSHARE_STOCK_HIST_SOURCES = ("eastmoney", "tencent")

## baostock（DATA_SOURCE='baostock'）：复权 1 不复权 2 前复权 3 后复权；区间与 AkShare 共用下列日期
BAOSTOCK_ADJUSTFLAG = "2"

## Tushare（DATA_SOURCE='tushare'）：需 [Tushare Pro](https://tushare.pro) token；区间与复权与 AkShare 共用上述日期/AKSHARE_ADJUST
TUSHARE_TOKEN = ""  # 优先使用环境变量 TUSHARE_TOKEN；此处可填本地默认值（勿提交真实 token）
TUSHARE_REQUEST_PAUSE_SEC = 0.2  # Pro 接口限频，每次请求后休眠秒数

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
