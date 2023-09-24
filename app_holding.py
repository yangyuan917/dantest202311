import pandas as pd
from flask import Flask
from flask_cors import CORS
# from flask_socketio import SocketIO
from fin_store.sql_adapter import query_table

SOURCE = 'ads_holding'

app_holding = Flask(__name__)
CORS(app_holding, resources={r"/*": {"origins": "*"}})
# socketio = SocketIO(app_holding, cors_allowed_origins="*")


@app_holding.route('/asset_concentrate', methods=['GET', 'OPTIONS'])
def show_macro():
    # catergory = request.args.get('catergory')
    # decoded_catergory = urllib.parse.unquote(catergory)
    res = query_table(f"select * from ads_asset_concentrate", SOURCE).get_df()
    # res = res.fillna(0)
    return dict(code=200, data=res.to_dict())


@app_holding.route('/catergory_list', methods=['GET', 'OPTIONS'])
def show_catergory_list():
    res = ['城投债券', '同业存单', '金融债', '利率债', '非标', '同业借款', '债券类公募基金', '混合类公募基金', '股票类公募基金',
           '股票', '城投abs', 'REITs', '存款', '现金']
    return dict(code=200, data=res)


if __name__ == '__main__':
    app_holding.run(host='0.0.0.0', port=8803, debug=True)
