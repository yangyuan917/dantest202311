from flask import Blueprint
from fin_store.sql_adapter import query_table

SOURCE = 'ads_holding'

app_bond_price = Blueprint('bp', __name__)


@app_bond_price.route('/curve_cnbd', methods=['GET', 'OPTIONS'])
def show_ads_curve_cnbd():
    res = query_table(f"select * from ads_curve_cnbd ", SOURCE).get_df()
    return dict(code=200, data={'data': res[['term', 'value']].values.tolist()})


@app_bond_price.route('/fixincome_price', methods=['GET', 'OPTIONS'])
def show_ads_fixincome_price():
    res = query_table(f"select * from ads_fixincome_price where term >= 30 limit 100", SOURCE).get_df()
    return dict(code=200, data=[{'name': type_, 'data': res[res.类别1 == type_][['term', 'yield']].values.tolist(),
                                 'code': res[res.类别1 == type_]['symbol2'].to_list(),
                                 'bond_name': res[res.类别1 == type_]['名称'].to_list()} for type_ in res.类别1])