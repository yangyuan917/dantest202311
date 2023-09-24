from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO
from fin_store.sql_adapter import query_table

SOURCE = 'holding'

app_holding = Flask(__name__)
CORS(app_holding, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app_holding, cors_allowed_origins="*")


@app_holding.route('/asset_concentrate', methods=['GET', 'OPTIONS'])
def show_macro():
    # catergory = request.args.get('catergory')
    # decoded_catergory = urllib.parse.unquote(catergory)
    res = query_table(f"select * from ads_asset_concentrate", SOURCE).get_df()
    # res = res.fillna(0)
    return dict(code=200, data=res.to_dict())


if __name__ == '__main__':
    app_holding.run(host='0.0.0.0', port=8803, debug=True)
