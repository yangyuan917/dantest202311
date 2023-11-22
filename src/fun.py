import pandas as pd

import fin_store.sql_adapter as sa
from database import HOLDING


def cal_price_product(start_date, end_date):
    price = sa.query_table(f"select 业务日期, `产品代码`, 投组单元编号, `单位净值(元)`, `总资产净值(元)` from dws_product_ledger", HOLDING).get_df()
    res_tmp = price[price.业务日期 == end_date]. \
        merge(price[price.业务日期 == start_date][['产品代码', '单位净值(元)']], on='产品代码',
              suffixes=('_last', '_first'))
    res_tmp['chg'] = res_tmp['单位净值(元)' + '_last'] / res_tmp['单位净值(元)' + '_first'] * 100 - 100
    return res_tmp


def cal_price_asset(cat, start_date, end_date):
    print(cat)
    cat_tuple = tuple(cat.split(','))
    if len(cat_tuple) > 1:
        condition = f"类别1 in {cat_tuple}"
    else:
        condition = f"类别1 = '{cat}'"
    target_df = sa.query_table(f"select 业务日期, symbol2, `市值(元)_投组` from dwd_asset_holding "
                               f"where 业务日期 in ('{start_date}', '{end_date}') and {condition}", HOLDING).get_df()

    value_col = 'close' if cat == '股票' else \
        'nav_adj' if '基金' in cat else 'net'
    dw_tb = 'dw_stock_price' if cat == '股票' else \
        'dw_fund_price' if '基金' in cat else 'dw_bond_price'
    price = sa.query_table(f"select * from {dw_tb} where trade_date in ('{start_date}', '{end_date}')", HOLDING).get_df()
    first_date, last_date = pd.to_datetime(start_date), pd.to_datetime(end_date)

    # 分析stock.symbol既在首日，又在末日的
    price = price[
        price.symbol2.isin(target_df[target_df.业务日期 == first_date]['symbol2']) & price.symbol2.isin(
            target_df[target_df.业务日期 == last_date]['symbol2']) &
        (price.trade_date >= first_date) & (price.trade_date <= last_date)]

    res_tmp = price[price.trade_date == last_date]. \
        merge(price[price.trade_date == first_date][['symbol2', value_col]], on='symbol2',
              suffixes=('_last', '_first')).\
        merge(target_df[target_df.业务日期 == last_date])
    res_tmp['chg'] = res_tmp[value_col + '_last'] / res_tmp[value_col + '_first'] * 100 - 100

    return res_tmp
