from flask import Blueprint
from fin_store.sql_adapter import query_table
from flask import request
import pandas as pd

SOURCE = 'real_estate'
app_real_estate = Blueprint('estate', __name__)


@app_real_estate.route('/register', methods=['GET', 'OPTIONS'])
def show_ads_estate_register():
    param = request.args.get('dt')
    dt_list = param.split(',')
    res = []
    for dt in dt_list:
        res_dt = query_table(f"select * from ods_{dt}_ershoufang_register ", SOURCE).get_df()
        if dt == '北京':
            tmp = res_dt[res_dt.指标.str.contains('住宅签约套数') & ~res_dt.业务日期.str.contains('月')][['业务日期', 'value', '指标']]
            tmp['业务日期'] = pd.to_datetime(tmp['业务日期'])
        if dt == '深圳':
            tmp = res_dt[res_dt.指标.str.contains('住宅') & res_dt.业务日期.str.contains('日')][['业务日期', '成交套数', '指标']]
            tmp['业务日期'] = pd.to_datetime(tmp['业务日期'], format='%Y年%m月%d日')
        if dt == '成都':
            tmp = res_dt[res_dt.指标.str.contains('一手房中心城区|二手房中心城区')][['业务日期', '住宅套数', '指标']]
        res += [{'name': f'{dt}:{ind}',
                 'data': tmp[tmp.指标 == ind].drop(columns='指标').values.tolist()}
                for ind in tmp.指标.unique()]
    return dict(code=200, data=res)


@app_real_estate.route('/onsale', methods=['GET', 'OPTIONS'])
def show_ads_estate_onsalecount():
    dt = request.args.get('dt')
    # dt = "'" + "','".join(dt.split(',')) + "'"
    res = query_table(f"select * from ods_bk_onsalecount_daily where 地区 = '{dt}'", SOURCE).get_df()
    return dict(code=200, data={'xaixs': res.业务日期.to_list(), 'series': {'name': f'{dt}', 'data': res.在售套数.to_list()}})


@app_real_estate.route('/listp', methods=['GET', 'OPTIONS'])
def show_ads_estate_listp_chg():
    dt = request.args.get('dt')
    dt = "'" + "','".join(dt.split(',')) + "'"
    res = query_table(f"select * from ads_estate_listp_chg where 板块2 in ({dt})", SOURCE).get_df()
    res = res[res.tag.isin(['涨', '降'])]
    res = res.pivot(index='业务日期', columns=['板块2', 'tag'], values='tag_pct')
    res = res.fillna(0.0)
    return dict(code=200, data={'xaixs': res.index.to_list(),
                                'series': [{'name': category + '_' + tag, 'data': res[category][tag].to_list()}
                                           for tag in res.columns.levels[1]
                                           for category in res.columns.levels[0]]})


@app_real_estate.route('/listp2', methods=['GET', 'OPTIONS'])
def show_ads_estate_listp_chg2():
    dt = request.args.get('dt')
    dt_list = dt.split(',')
    dt = "'" + "','".join(dt_list) + "'"
    res = query_table(f"select 板块2, 业务日期, down_pct from ads_estate_listp_chg2 where 板块2 in ({dt})", SOURCE).get_df()
    res = res[res.down_pct.notnull()]
    return dict(code=200, data=[{'name': dt,
                                 'data': res[res.板块2 == dt][['业务日期', 'down_pct']].to_dict(orient='records')} for dt in dt_list])
