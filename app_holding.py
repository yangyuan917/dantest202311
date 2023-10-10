import pandas as pd
from flask import Flask, request
from flask_cors import CORS
# from flask_socketio import SocketIO
from fin_store.sql_adapter import query_table

# SOURCE = 'holding'

SOURCE = 'ads_holding'

app_holding = Flask(__name__)
CORS(app_holding, resources={r"/*": {"origins": "*"}})
# socketio = SocketIO(app_holding, cors_allowed_origins="*")

catergory_list = ['城投债券', '同业存单', '金融债', '利率债', '非标', '同业借款', '债券型基金', '混合型基金', '股票型基金',
           '股票', '城投abs', 'REITs', '货币市场型基金', '货币市场工具', '存款', '现金']
industry_list = ['农林牧渔', '基础化工', '钢铁', '有色金属', '电子', '家用电器', '食品饮料', '纺织服饰', '轻工制造', '医药生物',
           '公用事业', '交通运输', '房地产', '商贸零售', '社会服务', '综合', '建筑材料', '建筑装饰',
           '电力设备', '国防军工', '计算机', '传媒', '通信', '银行', '非银金融', '汽车', '机械设备',
           '煤炭', '石油石化', '环保', '美容护理']
index_list = ['上证50指数', '沪深300指数', '中证800指数', '中证1000指数', 'not_in_index']


@app_holding.route('/catergory_list', methods=['GET', 'OPTIONS'])
def show_catergory_list():
    res = ['城投债券', '同业存单', '金融债', '利率债', '非标', '同业借款', '债券型基金', '混合型基金', '股票型基金',
           '股票', '城投abs', 'REITs', '货币市场型基金', '货币市场工具', '存款', '现金']
    return dict(code=200, data=res)


@app_holding.route('/industry_list', methods=['GET', 'OPTIONS'])
def show_industry_list():
    res = ['农林牧渔', '基础化工', '钢铁', '有色金属', '电子', '家用电器', '食品饮料', '纺织服饰', '轻工制造', '医药生物',
           '公用事业', '交通运输', '房地产', '商贸零售', '社会服务', '综合', '建筑材料', '建筑装饰',
           '电力设备', '国防军工', '计算机', '传媒', '通信', '银行', '非银金融', '汽车', '机械设备',
           '煤炭', '石油石化', '环保', '美容护理']
    return dict(code=200, data=res)


@app_holding.route('/index_list', methods=['GET', 'OPTIONS'])
def show_index_list():
    res = ['上证50指数', '沪深300指数', '中证800指数', '中证1000指数', 'not_in_index']
    return dict(code=200, data=res)


# 1.全公司总计 ads_asset_concentrate
@app_holding.route('/asset_concentrate', methods=['GET', 'OPTIONS'])
def show_asset_concentrate():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    # decoded_catergory = urllib.parse.unquote(catergory)
    res = query_table(f"select 业务日期, 类别1, 团队_pct from ads_asset_concentrate "
                      f"where 业务日期 in ('{start_date}', '{end_date}') and 团队='总计'", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res['类别1'] = pd.Categorical(res['类别1'], categories=catergory_list, ordered=True)
    res = res[res.类别1.notnull()].sort_values('类别1')
    res = res.pivot(index='类别1', columns='业务日期', values='团队_pct')
    res = res.fillna(0)
    res = res.to_dict()
    print('res', res)
    # res = res.fillna(0)
    # res = {str(date_): res[res.业务日期 == date_].set_index('类别1')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_holding.route('/asset_concentrate_timeseries', methods=['GET', 'OPTIONS'])
def show_asset_concentrate_timeseries():
    catergory = request.args.get('catergory')
    catergory = "'" + catergory + "'"
    res = query_table(f"select 类别1, 业务日期, 团队_pct from ads_asset_concentrate where 团队='总计' and "
                      f"类别1 in ({catergory})", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')['团队_pct'].to_dict() for catergory in res.类别1.unique()}
    # res = res.fillna(0)
    return dict(code=200, data=res)


@app_holding.route('/asset_indicator', methods=['GET', 'OPTIONS'])
def show_asset_indicator():
    indicator = request.args.get('indicator')
    catergory = request.args.get('catergory')
    catergory = "'" + catergory + "'"
    # decoded_catergory = urllib.parse.unquote(catergory)
    res = query_table(f"select 类别1, 业务日期, {indicator} from ads_asset_concentrate "
                      f"where 团队='总计' and 类别1 in ({catergory})", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')[f'{indicator}'].to_dict() for catergory in res.类别1.unique()}
    # res = res.fillna(0)
    return dict(code=200, data=res)


# 1.2.1 股票行业分布 ads_stock_industry_grouped_detail
@app_holding.route('/prtindustry', methods=['GET', 'OPTIONS'])
def show_prtindustry():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='全部' and "
                      f"target_col not in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()

    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='target_col', columns='业务日期', values='团队占比')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_holding.route('/prtindex', methods=['GET', 'OPTIONS'])
def show_prtindex():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='全部' and "
                      f"target_col in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='target_col', columns='业务日期', values='团队占比')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_holding.route('/prtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_prtindustry_timeseries():
    sector = request.args.get('sector')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='全部' and "
                      f"target_col in ('{sector}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.target_col == f'{sector}'].set_index('业务日期')['团队占比'].to_dict() for sector in res.target_col.unique()}
    return dict(code=200, data=res)


# 1.3.1 基金行业分布 ads_fund_industry_grouped_detail
@app_holding.route('/fundprtindustry', methods=['GET', 'OPTIONS'])
def show_fundprtindustry():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='全部' and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='行业名称', columns='业务日期', values='团队_pct')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('行业名称')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


# 1.3.2
@app_holding.route('/fundprtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_fundprtindustry_timeseries():
    sector = request.args.get('sector')
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='全部' and "
                      f"行业名称 in ('{sector}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.行业名称 == f'{sector}'].set_index('业务日期')['团队_pct'].to_dict() for sector in res.行业名称.unique()}
    return dict(code=200, data=res)


# 资管计划
# 2.1.1 大类资产配置 ads_asset_concentrate_separate
@app_holding.route('/asset_concentrate_separate', methods=['GET', 'OPTIONS'])
def show_asset_concentrate_separate():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, 类别1, 团队_pct from ads_asset_concentrate_separate "
                      f"where 业务日期 in ('{start_date}', '{end_date}') and "
                      f"`归属资管计划/自主投资基金`='{separate_name}'", SOURCE).get_df()
    res = {str(date_): res[res.业务日期 == date_].set_index('类别1')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_holding.route('/asset_concentrate_timeseries_separate', methods=['GET', 'OPTIONS'])
def show_asset_concentrate_timeseries_separate():
    separate_name = request.args.get('separate_name')
    catergory = request.args.get('catergory')
    print(catergory)
    catergory = "'" + catergory + "'"
    res = query_table(f"select 类别1, 业务日期, 团队_pct from ads_asset_concentrate_separate "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"类别1 in ({catergory})", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')['团队_pct'].to_dict() for catergory in res.类别1.unique()}
    return dict(code=200, data=res)


@app_holding.route('/asset_indicator_separate', methods=['GET', 'OPTIONS'])
def show_asset_indicator_separate():
    separate_name = request.args.get('separate_name')
    catergory = request.args.get('catergory')
    indicator = request.args.get('indicator')
    catergory = "'" + catergory + "'"
    # decoded_catergory = urllib.parse.unquote(catergory)
    res = query_table(f"select 类别1, 业务日期, {indicator} from ads_asset_concentrate_separate "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"类别1 in ({catergory})", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')[f'{indicator}'].to_dict() for catergory in res.类别1.unique()}
    # res = res.fillna(0)
    return dict(code=200, data=res)


# 2.2.1 股票行业分布 ads_stock_industry_grouped_detail
@app_holding.route('/separate_prtindustry', methods=['GET', 'OPTIONS'])
def show_separate_prtindustry():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"target_col not in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


# 2.2.2
@app_holding.route('/separate_prtindex', methods=['GET', 'OPTIONS'])
def show_separate_prtindex():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"target_col in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


# 2.2.3
@app_holding.route('/separate_prtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_separate_prtindustry_timeseries():
    separate_name = request.args.get('separate_name')
    sector = request.args.get('sector')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"target_col in ('{sector}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.target_col == f'{sector}'].set_index('业务日期')['团队占比'].to_dict() for sector in res.target_col.unique()}
    return dict(code=200, data=res)


# 2.3.1 基金行业分布 ads_fund_industry_grouped_detail
@app_holding.route('/separate_fundprtindustry', methods=['GET', 'OPTIONS'])
def show_separate_fundprtindustry():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"业务日期 in ('{start_date}', '{end_date}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {date_: res[res.业务日期 == date_].set_index('行业名称')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_holding.route('/separate_fundprtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_separate_fundprtindustry_timeseries():
    separate_name = request.args.get('separate_name')
    sector = request.args.get('sector')
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"行业名称 in ('{sector}')", SOURCE).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.行业名称 == f'{sector}'].set_index('业务日期')['团队_pct'].to_dict() for sector in res.行业名称.unique()}
    return dict(code=200, data=res)


# 3.1 交易
@app_holding.route('/transaction_bondfund', methods=['GET', 'OPTIONS'])
def show_ads_transaction_bondfund():
    indicator = request.args.get('indicator')
    catergory = request.args.get('catergory')
    res = query_table(f"select 业务日期, 类别1, `市值(元)`, {indicator} from ads_transaction_bondfund "
                      f"where 类别1 = '{catergory}' and 加减仓3='交易'", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {col: res.set_index('业务日期')[col].to_dict() for col in ['市值(元)', f'{indicator}']}
    # res = {col: res[['业务日期', col]].values.tolist() for col in ['市值(元)', f'{indicator}']}
    return dict(code=200, data=res)


@app_holding.route('/transaction_stock', methods=['GET', 'OPTIONS'])
def show_ads_transaction_stock():
    sector1 = request.args.get('sector1')
    sector2 = request.args.get('sector2')
    res = query_table(f"select 业务日期, 加减仓2, 申万行业一级, `市值(元)` from ads_transaction_stock "
                      f"where 申万行业一级 in ('{sector1}', '{sector2}')", SOURCE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='业务日期', columns=['申万行业一级', '加减仓2'], values='市值(元)')
    res.columns = res.columns.map(lambda x: f'{x[0]}_{x[1]}')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {sector: {direction: res[(res.加减仓2 == direction) & (res.申万行业一级 == sector)].
    #     set_index('业务日期')['市值(元)'].to_dict() for direction in ['加仓', '减仓']} for sector in ['总计', f'{sector}']}
    return dict(code=200, data=res)


if __name__ == '__main__':
    app_holding.run(host='0.0.0.0', port=8803, debug=True)
