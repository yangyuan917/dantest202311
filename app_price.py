from flask import Blueprint, request
from fin_store.sql_adapter import query_table
from database import HOLDING
from src.fun import cal_price_asset, cal_price_product
import pandas as pd
import numpy as np

app_bond_price = Blueprint('bp', __name__)
app_market_price = Blueprint('mp', __name__)


@app_bond_price.route('/curve_cnbd', methods=['GET', 'OPTIONS'])
def show_ads_curve_cnbd():
    res = query_table(f"select * from ads_curve_cnbd ", HOLDING).get_df()
    return dict(code=200, data={'data': res[['term', 'value']].values.tolist()})


@app_bond_price.route('/fixincome_price', methods=['GET', 'OPTIONS'])
def show_ads_fixincome_price():
    res = query_table(f"select * from ads_fixincome_price where term >= 30 limit 500", HOLDING).get_df()
    return dict(code=200, data=[{'name': type_, 'data': res[res.类别1 == type_][['term', 'yield']].values.tolist(),
                                 'code': res[res.类别1 == type_]['symbol2'].to_list(),
                                 'bond_name': res[res.类别1 == type_]['名称'].to_list()} for type_ in res.类别1.unique()])


@app_market_price.route('/asset_price', methods=['GET', 'OPTIONS'])
def show_ads_asset_price():
    cat = request.args.get('cat')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    r = cal_price_asset(cat, start_date, end_date)
    bins = [-np.inf, -20, -15] + list(range(-10, 11)) + [15, 20, np.inf]
    r['bin'] = pd.cut(r.chg, bins)

    res = r.groupby('bin', as_index=False).agg({'symbol2': 'count', '市值(元)_投组': 'sum'})
    total_market_value = res['市值(元)_投组'].sum()
    res['市值占比'] = res['市值(元)_投组'] / total_market_value
    res['bin'] = res['bin'].astype(str)
    return dict(code=200, data=res[['bin', '市值占比']].to_dict())


@app_market_price.route('/prod_price', methods=['GET', 'OPTIONS'])
def show_ads_prod_price():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    r = cal_price_product(start_date, end_date)
    import pandas as pd
    import numpy as np
    bins = [-np.inf, -0.2] + list(np.arange(-0.1, 0.11, 0.01)) + [0.2, np.inf]
    r['bin'] = pd.cut(r.chg, bins)
    res = r.groupby('bin', as_index=False).agg({'投组单元编号': 'nunique', '总资产净值(元)': 'sum'})
    total_market_value = res['总资产净值(元)'].sum()
    res['市值占比'] = res['总资产净值(元)'] / total_market_value
    res['bin'] = res['bin'].astype(str)
    return dict(code=200, data=res[['bin', '市值占比']].to_dict())
