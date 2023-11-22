from flask import Blueprint
from fin_store.sql_adapter import query_table
from flask import request
import pandas as pd
from database import REAL_ESTATE

app_real_estate = Blueprint('estate', __name__)
app_real_estate_detail = Blueprint('estate_detail', __name__)


@app_real_estate.route('/register', methods=['GET', 'OPTIONS'])
def show_ads_estate_register():
    param = request.args.get('dt')
    dt_list = param.split(',')
    res = []
    for dt in dt_list:
        res_dt = query_table(f"select * from ods_{dt}_ershoufang_register ", REAL_ESTATE).get_df()
        if dt == '北京':
            tmp = res_dt[res_dt.指标.str.contains('住宅签约套数') & ~res_dt.业务日期.str.contains('月')][['业务日期', 'value', '指标']]
            tmp['业务日期'] = pd.to_datetime(tmp['业务日期'])
        if dt == '深圳':
            tmp = res_dt[res_dt.指标.str.contains('住宅') & res_dt.业务日期.str.contains('日')][['业务日期', '成交套数', '指标']]
            tmp['业务日期'] = pd.to_datetime(tmp['业务日期'], format='%Y年%m月%d日')
        if dt == '成都':
            tmp = res_dt[res_dt.指标.str.contains('一手房中心城区|二手房中心城区')][['业务日期', '住宅套数', '指标']]
        if dt == '上海':
            tmp = res_dt[['业务日期', '套数']]
            tmp['指标'] = ''
        # if dt == '武汉':
        #     tmp = res_dt[res_dt.指标.str.contains('一手房中心城区|二手房中心城区')][['业务日期', '住宅套数', '指标']]
        if dt == '苏州':
            tmp = res_dt[res_dt.指标.str.contains('市区合计') & res_dt.类型.str.contains('住宅')][['业务日期', '套数', '指标']]
        res += [{'name': f'{dt}:{ind}',
                 'data': tmp[tmp.指标 == ind].drop(columns='指标').values.tolist()}
                for ind in tmp.指标.unique()]
    return dict(code=200, data=res)


@app_real_estate.route('/onsale', methods=['GET', 'OPTIONS'])
def show_ads_estate_onsalecount():
    dt_string = request.args.get('dt')
    dt_tuple = tuple(dt_string.split(','))
    if len(dt_tuple) > 1:
        condition = f"地区 in {dt_tuple}"
    else:
        condition = f"地区 = '{dt_string}'"
    res = query_table(f"select * from ods_bk_onsalecount_daily where {condition}", REAL_ESTATE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = [{dt: res[res.地区 == f'{dt}'][['业务日期', '在售套数']].values.tolist()} for dt in res.地区.unique()]
    return dict(code=200, data=res)


@app_real_estate.route('/onsaletest', methods=['GET', 'OPTIONS'])
def show_ads_estate_onsalecount1():
    dt_string = request.args.get('dt')
    dt_tuple = tuple(dt_string.split(','))
    if len(dt_tuple) > 1:
        condition = f"地区 in {dt_tuple}"
    else:
        condition = f"地区 = '{dt_string}'"
    res = query_table(f"select * from ods_bk_onsalecount_daily where {condition}", REAL_ESTATE).get_df()
    res.业务日期 = res.业务日期.astype('str')
    res = {dt: res[res.地区 == f'{dt}'][['业务日期', '在售套数']].values.tolist() for dt in res.地区.unique()}
    return dict(code=200, data=res)


@app_real_estate.route('/listp', methods=['GET', 'OPTIONS'])
def show_ads_estate_listp_chg():
    dt = request.args.get('dt')
    dt = "'" + "','".join(dt.split(',')) + "'"
    res = query_table(f"select * from ads_estate_listp_chg where 板块2 in ({dt})", REAL_ESTATE).get_df()
    res = res[res.tag.isin(['涨', '降'])]
    res = res.pivot(index='业务日期', columns=['板块2', 'tag'], values='tag_pct')
    res = res.fillna(0.0)
    return dict(code=200, data={'xaixs': res.index.to_list(),
                                'series': [{'name': category, 'tag': tag, 'data': res[category][tag].to_list()}
                                           for tag in res.columns.levels[1]
                                           for category in res.columns.levels[0]]})


@app_real_estate.route('/listp2', methods=['GET', 'OPTIONS'])
def show_ads_estate_listp_chg2():
    dt = request.args.get('dt')
    dt_list = dt.split(',')
    dt = "'" + "','".join(dt_list) + "'"
    res = query_table(f"select 板块2, 业务日期, down_pct from ads_estate_listp_chg2 where 板块2 in ({dt})", REAL_ESTATE).get_df()
    res = res[res.down_pct.notnull()]
    return dict(code=200, data=[{'name': dt,
                                 'data': res[res.板块2 == dt][['业务日期', 'down_pct']].to_dict(orient='records')} for dt in dt_list])


@app_real_estate.route('/onsale_price', methods=['GET', 'OPTIONS'])
def show_ads_estate_onsale_price():
    dt = request.args.get('dt')
    dt_list = dt.split(',')
    dt = "'" + "','".join(dt_list) + "'"
    res = query_table(f"select * from ads_onsale_price where 板块2 in ({dt})", REAL_ESTATE).get_df()
    return dict(code=200, data=[{'name': dt,
                                 'data': res[res.板块2 == dt][['业务日期', 'unitprice1']].to_dict(orient='records')} for dt in dt_list])

    # return dict(code=200, data={'xaixs': res.业务日期.to_list(), 'series': {'name': f'{dt}', 'data': res.unitprice1.to_list() for dt in dt_list}})


@app_real_estate_detail.route('/chg', methods=['GET', 'OPTIONS'])
def show_ads_estate_detail():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    city = request.args.get('city', default='')
    district1 = request.args.get('district1', default='')
    district2 = request.args.get('district2', default='')
    lp_name = request.args.get('lp_name', default='')

    floor = request.args.get('floor', default='')
    built_year_start = request.args.get('built_year_start', default='')
    built_year_end = request.args.get('built_year_end', default='')
    num_of_room = request.args.get('num_of_room', default='')
    area_start = request.args.get('area_start', default='')
    area_end = request.args.get('area_end', default='')
    orientation = request.args.get('orientation', default='')

    # conditions = ["a.业务日期 >= %s", "a.业务日期 <= %s"]
    # params = [start_date, end_date]
    #
    # if city:
    #     conditions.append("b.city = %s")
    #     params.append(city)
    # sql_query = "select * from ods_bk_hs_price_sample a left join ods_bk_lp_list b on a.name = b.title where " + " AND ".join(conditions)
    # res = query_table("SELECT * FROM ods_bk_hs_price_sample WHERE 业务日期 >= :start_date AND 业务日期 <= :end_date", REAL_ESTATE, start_date = start_date, end_date= end_date).get_df()
    # res = query_table("SELECT * FROM ods_bk_hs_price_sample WHERE 业务日期 >= {} AND 业务日期 <= {}", REAL_ESTATE, [start_date, end_date]).get_df()

    sql_query = f"select * from ods_bk_hs_price_sample a left join ods_bk_lp_list b " \
                f"on a.name = b.title where a.业务日期 >= '{start_date}' and a.业务日期 <= '{end_date}'"

    if city != '':
        sql_query += f" AND b.city = '{city}'"
    if district1 != '':
        sql_query += f" AND b.positionInfo1 = '{district1}'"
    if district2 != '':
        sql_query += f" AND b.positionInfo2 = '{district2}'"
    if lp_name != '':
        sql_query += f" AND a.name = '{lp_name}'"

    if floor != '':
        sql_query += f" AND a.houseIcon like '%{floor}%'"
    if built_year_start != '':
        sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9]+)年') AS UNSIGNED) >= {built_year_start}"
        # sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9]{{4}})年') AS UNSIGNED) >= {built_year_start}" todo:为什么不对
    if built_year_end != '':
        sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9]+)年') AS UNSIGNED) <= {built_year_end}"
    if num_of_room != '':
        sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9])室') AS UNSIGNED) = {num_of_room}"
    if area_start != '':
        sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9]+)平米') AS UNSIGNED) >= {area_start}"
    if area_end != '':
        sql_query += f" AND CAST(REGEXP_SUBSTR(a.houseIcon, '([0-9]+)平米') AS UNSIGNED) >= {area_end}"
    if orientation != '':
        sql_query += f" AND a.houseIcon = {orientation}"
    res = query_table(sql_query, REAL_ESTATE).get_df()
    return dict(code=200, data=res.to_dict())
