import fin_store.sql_adapter as sa
from database import HOLDING


def cal_price_product(start_date, end_date):
    price = sa.query_table(f"select 业务日期, `投组代码/产品代码`, 投组单元编号, `单位净值(元)` from product_ledger", HOLDING).get_df()
    res_tmp = price[price.业务日期 == end_date]. \
        merge(price[price.业务日期 == start_date][['投组代码/产品代码', '单位净值(元)']], on='投组代码/产品代码',
              suffixes=('_last', '_first'))
    res_tmp['chg'] = res_tmp['单位净值(元)' + '_last'] / res_tmp['单位净值(元)' + '_first'] * 100 - 100
    return res_tmp


def cal_price_asset(cat, start_date, end_date):
    target_df = sa.query_table()
    value_col = '' if '' else ''
    dw_tb = '' if '' else ''
    first_date, last_date = target_df.业务日期.min(), target_df.业务日期.max()
    price = sa.query_table(f"select * from {dw_tb}", HOLDING).get_df()

    # 分析stock.symbol既在首日，又在末日的
    price = price[
        price.symbol2.isin(target_df[target_df.业务日期 == first_date]['symbol2']) & price.symbol2.isin(
            target_df[target_df.业务日期 == last_date]['symbol2']) &
        (price.trade_date >= first_date) & (price.trade_date <= last_date)]

    res_tmp = price[price.trade_date == last_date]. \
        merge(price[price.trade_date == first_date][['symbol2', value_col]], on='symbol2',
              suffixes=('_last', '_first'))
    res_tmp['chg'] = res_tmp[value_col + '_last'] / res_tmp[value_col + '_first'] * 100 - 100
    return res_tmp
