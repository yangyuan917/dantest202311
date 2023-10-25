from flask import Blueprint
from fin_store.sql_adapter import query_table
from flask import request
import pandas as pd

SOURCE = 'ads_holding'
app_txn = Blueprint('txn', __name__)


@app_txn.route('/atp', methods=['GET', 'OPTIONS'])
def show_ads_curve_cnbd():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sep_name = request.args.get('sep_name', default='')
    cat = request.args.get('cat', default='')
    inter_trade = request.args.get('inter_trade', default=0)
    param4 = request.args.get('param4', default=False)  # todo 是否区分买卖
    if param4:
        tb_name = 'tmp_separate_tag'
    else:
        tb_name = 'tmp_separate_tag'

    sql_query = f"SELECT * FROM {tb_name}  WHERE 业务日期 >= '{start_date}' AND 业务日期 <= '{end_date}'"

    if sep_name:
        sql_query += f" AND `归属资管计划/自主投资基金` = '{sep_name}'"
    if cat:
        sql_query += f" AND 类别1 = '{cat}'"
    if inter_trade:
        sql_query += f" AND inside_trading_tag = {inter_trade}"  # todo : inside_trading
    df = query_table(sql_query, SOURCE).get_df()
    if sep_name:
        res = df.groupby(['类别1']).agg({'symbol2': 'count', '市值(元)': 'sum'}).reset_index()
        res.index.name = 'index'
    if cat:
        res = df.groupby(['归属资管计划/自主投资基金']).agg({'symbol2': 'count', '市值(元)': 'sum'})
        res.index.name = 'index'
    if not sep_name and not cat:
        df1 = df.groupby(['类别1']).agg({'symbol2': 'count', '市值(元)': 'sum'})
        # df1 = df.pivot_table(index='类别1', columns='加减仓', aggfunc={'symbol2': 'count', '市值(元)': 'sum'})
        df2 = df.groupby(['归属资管计划/自主投资基金']).agg({'symbol2': 'count', '市值(元)': 'sum'})
        #
        # total = df.groupby('加减仓').agg({'symbol2': 'count', '市值(元)': 'sum'}).reset_index()
        # total['加减仓'] = '总计'
        # df_with_total = pd.concat([df, total])
        # result = df_with_total.pivot_table(index='类别1', columns='加减仓', values=['symbol2', '市值(元)'],
        #                                    aggfunc={'symbol2': 'count', '市值(元)': 'sum'}, fill_value=0)
        res = pd.concat([df1, df2])
    return dict(code=200, data=res.reset_index().to_dict(orient='records'))
    # jsonify(res.to_dict(orient="records"))


# @app_txn.route('/sale', methods=['GET', 'OPTIONS'])
# def show_ads_curve_cnbd():
#     res = query_table(f"select * from ads_curve_cnbd ", SOURCE).get_df()
#     return dict(code=200, data={'xaixs': res.term.to_list(), 'series': {'name': '收益率曲线', 'data': res.value.to_list()}})
#
#
# @app_txn.route('/lp', methods=['GET', 'OPTIONS'])
# def show_ads_curve_cnbd():
#     res = query_table(f"select * from ads_curve_cnbd ", SOURCE).get_df()
#     return dict(code=200, data={'xaixs': res.term.to_list(), 'series': {'name': '收益率曲线', 'data': res.value.to_list()}})