from flask import Blueprint, request
from fin_store.sql_adapter import query_table
from database import BOND_MARKET, STOCK_MARKET

app_macro = Blueprint('macro', __name__)


# 宏观利率
@app_macro.route('/operation', methods=['GET', 'OPTIONS'])
def show_macro():
    res = query_table("select * from ads_maro_daily", BOND_MARKET).get_df()
    res = res.fillna(0)
    return dict(code=200, data=res.to_dict())


@app_macro.route('/maro_rate', methods=['GET', 'OPTIONS'])
def show_macro_rate():
    res = query_table("select * from ads_maro_rate", BOND_MARKET).get_df()
    res = res.drop(columns='update_time').fillna(0)
    ind_id = query_table("select * from ods_ind_id", BOND_MARKET).get_df()
    ind_id_map = ind_id.set_index('代码')['指标'].to_dict()
    res.columns = res.columns.to_series().replace(ind_id_map)
    return dict(code=200, data=res.to_dict())


@app_macro.route('/index_cnbd', methods=['GET', 'OPTIONS'])
def show_index_cnbd():
    res = query_table("select * from ads_index_cnbd_daily", BOND_MARKET).get_df()
    res = res.fillna(0)
    # response = jsonify(dict(code=200, data=res.to_dict()))
    # response.headers['Content-Type'] = 'application/json; charset=utf-8'
    return dict(code=200, data=res.to_dict())


@app_macro.route('/m2', methods=['GET', 'OPTIONS'])
def show_m2():
    res = query_table("select * from ads_m2_daily", BOND_MARKET).get_df()
    res = res.fillna(0)
    # response = jsonify(dict(code=200, data=res.to_dict()))
    # response.headers['Content-Type'] = 'application/json; charset=utf-8'
    return dict(code=200, data=res.to_dict())


# 资金流向
# 1. ETF份额变动
@app_macro.route('/fund_flow', methods=['GET', 'OPTIONS'])
def show_fund_flow():
    target = request.args.get('tag')
    res = query_table(f"select * from ads_fund_flow where tag='{target}'", STOCK_MARKET).get_df()
    return dict(code=200, data=res.fillna(0.0).to_dict())


# 2. 新成立基金情况
@app_macro.route('/new_fund_flow', methods=['GET', 'OPTIONS'])
def show_new_fund_flow():
    res = query_table(f"select * from ads_new_fund_flow", STOCK_MARKET).get_df()
    return dict(code=200, data=res.to_dict())


# 3. 北向资金
@app_macro.route('/north_flow', methods=['GET', 'OPTIONS'])
def show_north_flow():
    res = query_table(f"select * from ads_north_flow", STOCK_MARKET).get_df()
    return dict(code=200, data=res.to_dict())

