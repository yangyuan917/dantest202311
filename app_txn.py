from flask import Blueprint
from fin_store.sql_adapter import query_table
from flask import request
import pandas as pd
from database import HOLDING

app_txn = Blueprint('txn', __name__)


@app_txn.route('/atp', methods=['GET', 'OPTIONS'])
def show_ads_txn_atp():
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
    sql_query += f" AND inside_trading_tag = {inter_trade}"  # todo : inside_trading

    if sep_name:
        sql_query += f" AND `归属资管计划/自主投资基金` = '{sep_name}'"
    if cat:
        sql_query += f" AND 类别1 = '{cat}'"

    df = query_table(sql_query, HOLDING).get_df()
    if sep_name:
        col = '类别1'
    if cat:
        col = '归属资管计划/自主投资基金'
    if not sep_name and not cat:
        col = '类别1'

    df1 = df.groupby([col]).agg({'symbol2': 'count', '市值(元)': lambda x: x.sum() / 10000})
    df1_ = df.pivot_table(index=col, columns='加减仓3', aggfunc={'symbol2': 'count', '市值(元)': lambda x: x.sum() / 10000})
    df1_ = df1_.swaplevel(0, 1, axis=1).sort_index(axis=1)
    for col in ['加仓', '减仓', '到期']:
        if col not in df1_.columns.get_level_values(0):
            df1_[col, 'symbol2'] = 0
            df1_[col, '市值(元)'] = 0.0
    res = pd.concat([df1, df1_['加仓'], df1_['减仓'], df1_['到期']], axis=1)
    cols = ['symbol2', '市值(元)']
    res.columns = cols + ['加仓_' + col for col in cols] + ['减仓_' + col for col in cols] + ['到期_' + col for col in cols]
    res.index.name = '归属资管计划'
    # res = pd.concat([pd.concat([df1, df2]), pd.concat([df1_, df2_])], axis=1)
    return dict(code=200, data=res.fillna(0.0).reset_index().to_dict(orient='records'))
    # jsonify(res.to_dict(orient="records"))


@app_txn.route('/atp2', methods=['GET', 'OPTIONS'])
def show_ads_txn_atp2():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    inter_trade = request.args.get('inter_trade', default=0)
    tb_name = 'tmp_separate_tag'
    sql_query = f"SELECT * FROM {tb_name}  WHERE 业务日期 >= '{start_date}' AND 业务日期 <= '{end_date}'"
    sql_query += f" AND inside_trading_tag = {inter_trade}"  # todo : inside_trading
    df = query_table(sql_query, HOLDING).get_df()
    df2 = df.groupby(['归属资管计划/自主投资基金']).agg({'symbol2': 'count', '市值(元)': lambda x: x.sum() / 10000})
    df2_ = df.pivot_table(index='归属资管计划/自主投资基金', columns='加减仓2', aggfunc={'symbol2': 'count', '市值(元)': lambda x: x.sum() / 10000})
    df2_ = df2_.swaplevel(0, 1, axis=1).sort_index(axis=1)
    # print(df2_.columns.get_level_values(0))
    # print(df2_)
    for col in ['加仓', '减仓', '到期']:
        if col not in df2_.columns.get_level_values(0):
            df2_[col, 'symbol2'] = 0
            df2_[col, '市值(元)'] = 0.0
    res = pd.concat([df2, df2_['加仓'], df2_['减仓'], df2_['到期']], axis=1)
    cols = ['symbol2', '市值(元)']
    res.columns = cols + ['加仓_' + col for col in cols] + ['减仓_' + col for col in cols] + ['到期_' + col for col in cols]
    res.index.name = '归属资管计划'
    # print(res.fillna(0.0).reset_index().iloc[0,:])
    return dict(code=200, data=res.fillna(0.0).reset_index().to_dict(orient='records'))


@app_txn.route('/stock_prt', methods=['GET', 'OPTIONS'])
def show_ads_txn_stock_prt():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sep_name = request.args.get('sep_name', default='')
    cat = request.args.get('cat', default='')
    param1 = request.args.get('param1', default='')
    param4 = request.args.get('param4', default=False)  # todo 是否区分买卖
    tb_name = 'ads_txn_stock_industry_grouped'

    sql_query = f"SELECT * FROM {tb_name}  WHERE 业务日期 >= '{start_date}' AND 业务日期 <= '{end_date}' AND `归属资管计划/自主投资基金` != '总计'"

    if sep_name:
        sql_query += f" AND `归属资管计划/自主投资基金` = '{sep_name}'"
    if cat:
        sql_query += f" AND target_col = '{cat}'"

    df = query_table(sql_query, HOLDING).get_df()
    if param1 == '0' or sep_name:
        col = 'target_col'
        target_col = '归属资管计划/自主投资基金'
    if param1 == '1' or cat:
        col = '归属资管计划/自主投资基金'
        target_col = 'target_col'

    df1 = df.groupby([col]).agg({target_col: 'count', '市值(万)': lambda x: x.sum()})
    df1_ = df.pivot_table(index=col, columns='加减仓3', aggfunc={target_col: 'count', '市值(万)': lambda x: x.sum()})
    df1_ = df1_.swaplevel(0, 1, axis=1).sort_index(axis=1)
    for col in ['加仓', '减仓', '到期']:
        if col not in df1_.columns.get_level_values(0):
            df1_[col, target_col] = 0
            df1_[col, '市值(万)'] = 0.0

    res = pd.concat([df1, df1_['加仓'], df1_['减仓'], df1_['到期']], axis=1)

    res.columns = df1.columns.tolist() + ['加仓_' + col for col in df1_['加仓'].columns] + \
                  ['减仓_' + col for col in df1_['减仓'].columns] + \
                  ['到期_' + col for col in df1_['到期'].columns]
    res.index.name = '归属资管计划'
    # res = pd.concat([pd.concat([df1, df2]), pd.concat([df1_, df2_])], axis=1)
    return dict(code=200, data=res.fillna(0.0).reset_index().to_dict(orient='records'))


@app_txn.route('/fund_prt', methods=['GET', 'OPTIONS'])
def show_ads_txn_stock_prt():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sep_name = request.args.get('sep_name', default='')
    cat = request.args.get('cat', default='')
    param1 = request.args.get('param1', default='')
    param4 = request.args.get('param4', default=False)  # todo 是否区分买卖
    tb_name = 'ads_txn_fund_industry_grouped'

    sql_query = f"SELECT * FROM {tb_name}  WHERE 业务日期 >= '{start_date}' AND 业务日期 <= '{end_date}' AND `归属资管计划/自主投资基金` != '总计'"

    if sep_name:
        sql_query += f" AND `归属资管计划/自主投资基金` = '{sep_name}'"
    if cat:
        sql_query += f" AND 行业名称 = '{cat}'"

    df = query_table(sql_query, HOLDING).get_df()
    if param1 == '0' or sep_name:
        col = '行业名称'
        target_col = '归属资管计划/自主投资基金'
    if param1 == '1' or cat:
        col = '归属资管计划/自主投资基金'
        target_col = '行业名称'

    df1 = df.groupby([col]).agg({target_col: 'count', '市值(万)': lambda x: x.sum()})
    df1_ = df.pivot_table(index=col, columns='加减仓3', aggfunc={target_col: 'count', '市值(万)': lambda x: x.sum()})
    df1_ = df1_.swaplevel(0, 1, axis=1).sort_index(axis=1)
    for col in ['加仓', '减仓', '到期']:
        if col not in df1_.columns.get_level_values(0):
            df1_[col, target_col] = 0
            df1_[col, '市值(万)'] = 0.0

    res = pd.concat([df1, df1_['加仓'], df1_['减仓'], df1_['到期']], axis=1)

    res.columns = df1.columns.tolist() + ['加仓_' + col for col in df1_['加仓'].columns] + \
                  ['减仓_' + col for col in df1_['减仓'].columns] + \
                  ['到期_' + col for col in df1_['到期'].columns]
    res.index.name = '归属资管计划'
    # res = pd.concat([pd.concat([df1, df2]), pd.concat([df1_, df2_])], axis=1)
    return dict(code=200, data=res.fillna(0.0).reset_index().to_dict(orient='records'))


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
