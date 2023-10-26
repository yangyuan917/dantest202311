from flask import Blueprint
from fin_store.sql_adapter import query_table

HOLDING = 'ads_holding'
REAL_ESTATE = 'ads_real_estate'
app_list = Blueprint('list', __name__)

catergory_list = ['城投债券', '同业存单', '金融债', '利率债', '非标', '同业借款', '债券型基金', '混合型基金', '股票型基金',
                  '股票', '城投abs', 'REITs', '货币市场型基金', '货币市场工具', '存款', '现金']
industry_list = ['农林牧渔', '基础化工', '钢铁', '有色金属', '电子', '家用电器', '食品饮料', '纺织服饰', '轻工制造', '医药生物',
                 '公用事业', '交通运输', '房地产', '商贸零售', '社会服务', '综合', '建筑材料', '建筑装饰',
                 '电力设备', '国防军工', '计算机', '传媒', '通信', '银行', '非银金融', '汽车', '机械设备',
                 '煤炭', '石油石化', '环保', '美容护理']
index_list = ['上证50指数', '沪深300指数', '中证800指数', '中证1000指数', 'not_in_index']


@app_list.route('/sep', methods=['GET', 'OPTIONS'])
def show_separate_list():
    res = query_table("select distinct `归属资管计划/自主投资基金` from ads_asset_concentrate_separate", HOLDING).get_df()
    return dict(code=200, data=res['归属资管计划/自主投资基金'].to_list())


@app_list.route('/cat', methods=['GET', 'OPTIONS'])
def show_catergory_list():
    res = ['城投债券', '同业存单', '金融债', '利率债', '非标', '同业借款', '债券型基金', '混合型基金', '股票型基金',
           '股票', '城投abs', 'REITs', '货币市场型基金', '货币市场工具', '存款', '现金']
    return dict(code=200, data=res)


@app_list.route('/indus', methods=['GET', 'OPTIONS'])
def show_industry_list():
    res = ['农林牧渔', '基础化工', '钢铁', '有色金属', '电子', '家用电器', '食品饮料', '纺织服饰', '轻工制造', '医药生物',
           '公用事业', '交通运输', '房地产', '商贸零售', '社会服务', '综合', '建筑材料', '建筑装饰',
           '电力设备', '国防军工', '计算机', '传媒', '通信', '银行', '非银金融', '汽车', '机械设备',
           '煤炭', '石油石化', '环保', '美容护理']
    return dict(code=200, data=res)


@app_list.route('/index', methods=['GET', 'OPTIONS'])
def show_index_list():
    res = ['上证50指数', '沪深300指数', '中证800指数', '中证1000指数', 'not_in_index']
    return dict(code=200, data=res)


@app_list.route('/city_onsale', methods=['GET', 'OPTIONS'])
def show_city_onsale_list():
    res = query_table("select distinct 地区 from ods_bk_onsalecount_daily where 业务日期 = '2023-9-17'", REAL_ESTATE).get_df()
    return dict(code=200, data=res['地区'].to_list())


@app_list.route('/city_register', methods=['GET', 'OPTIONS'])
def show_city_register_list():
    res = ['北京', '深圳', '成都']
    return dict(code=200, data=res)


@app_list.route('/city_price', methods=['GET', 'OPTIONS'])
def show_city_price_list():
    res = query_table("select distinct 板块2 from ads_estate_listp_chg where 业务日期 = '2023-10-22'", REAL_ESTATE).get_df()
    return dict(code=200, data=res['板块2'].to_list())

