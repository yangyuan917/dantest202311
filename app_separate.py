# 资管计划
from flask import Blueprint
from fin_store.sql_adapter import query_table
from flask import request
import pandas as pd
from app_list import catergory_list
from database import HOLDING

app_separate = Blueprint('separate', __name__)


# 2.1.1 大类资产配置 ads_asset_concentrate_separate
# 置顶行：总规模&大类资产配置
@app_separate.route('/total_asset', methods=['GET', 'OPTIONS'])
def show_asset_total():
    separate_name = request.args.get('separate_name')
    res = query_table(f"select 业务日期, `市值(元)` from ads_seperate_grouped where `归属资管计划/自主投资基金` = '{separate_name}'",
                      HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    return dict(code=200, data=res.to_dict())


@app_separate.route('/total_asset1', methods=['GET', 'OPTIONS'])
def show_asset_total1():
    separate_name = request.args.get('separate_name')
    res = query_table(f"select 业务日期, `市值(元)` from ads_seperate_grouped where `归属资管计划/自主投资基金` = '{separate_name}'",
                      HOLDING).get_df()
    return dict(code=200, data=res.values.tolist())


@app_separate.route('/asset_concentrate', methods=['GET', 'OPTIONS'])
def show_asset_concentrate_separate():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, 类别1, 团队_pct from ads_asset_concentrate_separate "
                      f"where 业务日期 in ('{start_date}', '{end_date}') and "
                      f"`归属资管计划/自主投资基金`='{separate_name}'", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res['类别1'] = pd.Categorical(res['类别1'], categories=catergory_list, ordered=True)
    res = res[res.类别1.notnull()].sort_values('类别1')
    res = res.pivot(index='类别1', columns='业务日期', values='团队_pct')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {str(date_): res[res.业务日期 == date_].set_index('类别1')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_separate.route('/asset_concentrate_timeseries', methods=['GET', 'OPTIONS'])
def show_asset_concentrate_timeseries_separate():
    separate_name = request.args.get('separate_name')
    catergory_string = request.args.get('catergory')
    catergory_tuple = tuple(catergory_string.split(','))
    if len(catergory_tuple) > 1:
        condition = f"类别1 in {catergory_tuple}"
    else:
        condition = f"类别1 = '{catergory_string}'"
    res = query_table(f"select 类别1, 业务日期, 团队_pct from ads_asset_concentrate_separate "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"{condition}", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')['团队_pct'].to_dict() for catergory in res.类别1.unique()}
    return dict(code=200, data=res)


@app_separate.route('/asset_indicator', methods=['GET', 'OPTIONS'])
def show_asset_indicator_separate():
    separate_name = request.args.get('separate_name')
    indicator = request.args.get('indicator')
    catergory_string = request.args.get('catergory')
    catergory_tuple = tuple(catergory_string.split(','))
    if len(catergory_tuple) > 1:
        condition = f"类别1 in {catergory_tuple}"
    else:
        condition = f"类别1 = '{catergory_string}'"
    res = query_table(f"select 类别1, 业务日期, {indicator} from ads_asset_concentrate_separate "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and {condition}", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {catergory: res[res.类别1 == catergory].set_index('业务日期')[f'{indicator}'].to_dict() for catergory in
           res.类别1.unique()}
    # res = res.fillna(0)
    return dict(code=200, data=res)


# 2.2.1 股票行业分布 ads_stock_industry_grouped_detail
@app_separate.route('/prtindustry', methods=['GET', 'OPTIONS'])
def show_separate_prtindustry():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"target_col not in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='target_col', columns='业务日期', values='团队占比')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


# 2.2.2
@app_separate.route('/prtindex', methods=['GET', 'OPTIONS'])
def show_separate_prtindex():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"target_col in ('not_in_index', '上证50指数', '沪深300指数', '中证1000指数', '中证800指数') and "
                      f"业务日期 in ('{start_date}', '{end_date}')", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='target_col', columns='业务日期', values='团队占比')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('target_col')['团队占比'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


# 2.2.3
@app_separate.route('/prtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_separate_prtindustry_timeseries():
    separate_name = request.args.get('separate_name')
    sector_string = request.args.get('sector')
    sector_tuple = tuple(sector_string.split(','))
    if len(sector_tuple) > 1:
        condition = f"target_col in {sector_tuple}"
    else:
        condition = f"target_col = '{sector_string}'"
    res = query_table(f"select 业务日期, target_col, 团队占比 from ads_stock_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and {condition}", HOLDING).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.target_col == f'{sector}'].set_index('业务日期')['团队占比'].to_dict() for sector in
           res.target_col.unique()}
    return dict(code=200, data=res)


# 2.3.1 基金行业分布 ads_fund_industry_grouped_detail
@app_separate.route('/fundprtindustry', methods=['GET', 'OPTIONS'])
def show_separate_fundprtindustry():
    separate_name = request.args.get('separate_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and "
                      f"业务日期 in ('{start_date}', '{end_date}')", HOLDING).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = res.pivot(index='行业名称', columns='业务日期', values='团队_pct')
    res = res.fillna(0)
    res = res.to_dict()
    # res = {date_: res[res.业务日期 == date_].set_index('行业名称')['团队_pct'].to_dict() for date_ in res.业务日期.unique()}
    return dict(code=200, data=res)


@app_separate.route('/fundprtindustry_timeseries', methods=['GET', 'OPTIONS'])
def show_separate_fundprtindustry_timeseries():
    separate_name = request.args.get('separate_name')
    sector_string = request.args.get('sector')
    sector_tuple = tuple(sector_string.split(','))
    if len(sector_tuple) > 1:
        condition = f"行业名称 in {sector_tuple}"
    else:
        condition = f"行业名称 = '{sector_string}'"
    res = query_table(f"select 业务日期, 行业名称, 团队_pct from ads_fund_industry_grouped_detail "
                      f"where `归属资管计划/自主投资基金`='{separate_name}' and {condition}", HOLDING).get_df()
    # res = res.fillna(0)
    res.业务日期 = res.业务日期.astype('str')
    res = {sector: res[res.行业名称 == f'{sector}'].set_index('业务日期')['团队_pct'].to_dict() for sector in res.行业名称.unique()}
    return dict(code=200, data=res)
