import platform

import pandas as pd
from WindPy import w

from fin_store import write_error, Timer, get_section

"""
    万德API文档： https://www.windquant.com/qntcloud/apiRefHelp/id-b89ae6bf-17db-40d6-8f7c-123702a30755#
"""

"""每次查询最大分页数字"""
__MAX_PAGING_SIZE__ = 5000

error_dict = {-40520007: 'EmptyData'}


def wind_login():
    sys_ = platform.system()

    if w.isconnected():
        return
    if sys_ == 'Windows':
        w.start()
        return
    s_ = get_section('wind')
    w.start(f'username={s_.get("username", "")};password={s_.get("password", "")};sitename=AT')


def wind_wsd(code, fields, begin_time=None, end_time=None, **options):
    """
        获取日时间序列函数WSD
        w.wsd（codes, fields, beginTime, endTime, options）
        支持股票、债券、基金、期货、指数等多种证券的基本资料、股东信息、市场行情、证券分析、预测评级、财务数据等各种数据。
        wsd可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据。

        参数	        类型	            可选	默认值	说明
        codes	    str或list	    否	无
            证券代码，支持获取单品种或多品种，如“600030.SH”或[“600010.SH”,“000001.SZ”]

        fields	    str或list	    否	无
            指标列表,支持获取单指标或多指标，，如“CLOSE,HIGH,LOW,OPEN”

        beginTime	str或datetime	是	endTime
            起始日期，为空默认为截止日期，如: "2016-01-01"、“20160101”、“2016/01/01”、"-5D"(当前日期前推5个交易日)、datetime/date类型

        endTime	    str或datetime	是	系统当前日期
            如: "2016-01-05"、“20160105”、“2016/01/05”、"-2D"(当前日期前推2个交易日) 、datetime/date类型

        options	    str	            是	“”
            options以字符串的形式集成多个参数，具体见代码生成器。如无相关参数设置，可以不给option赋值或者使用options=""

        options参数表
        参数	    类型	    可选	    默认值	    说明
        Days	str	    是	    'Trading'	日期选项，参数值含义如下：
                                            Weekdays: 工作日，
                                            Alldays: 日历日，
                                            Trading: 交易日

        Fill	str	    是	    'Blank'	    空值填充方式。参数值含义如下：
                                            Previous：沿用前值，
                                            Blank：返回空值
                                            如需选择自设数值填充，在options添加“ShowBlank=X", 其中X为自设数。

        Order	str	    是	    'A'	        日期排序，“A”：升序，“D”：降序

        Period	str	    是	    'D'	        取值周期。 D：天， W：周， M：月， Q：季度， S：半年， Y：年

        TradingCalendar	str	是	'SSE'	    交易日对应的交易所。参数值含义如下：
                                            SSE ：上海证券交易所， SZSE：深圳证券交易所，
                                            CFFE：中金所，DCE：大商所， CZCE：郑商所，SHFE：上期所，
                                            TWSE：台湾证券交易所， HKEX：香港交易所，
                                            NYSE：纽约证券交易所， COMEX：纽约金属交易所， NYBOT：纽约期货交易所，
                                            CME：芝加哥商业交易所， Nasdaq：纳斯达克证券交易所， NYMEX：纽约商品交易所，
                                            CBOT：芝加哥商品交易所， LME：伦敦金属交易所， IPE：伦敦国际石油交易所

        Currency	str	是	    'Original'	输入币种。Original：“原始货币”， HKD：“港币”， USD：“美元”， CNY：“人民币”

        PriceAdj	str	是	    不复权	    股票和基金(复权方式)。参数值含义如下：
                                            F：前复权， B：后复权，
                                            T：定点复权；债券(价格类型) CP：净价， DP：全价， MP：市价，
                                            YTM：收益率

        注:
        1. Fields和Parameter也可以传入list，比如可以用[“CLOSE”,“HIGH”,“LOW”,“OPEN”]替代“CLOSE,HIGH,LOW,OPEN”;
        2. 获取多个证券数据时，Fields只能选择一个。
        3. 日期支持相对日期宏表达方式，日期宏具体使用方式参考'日期宏’部分内容
        4. options为可选参数，可选参数多个，在参数说明详细罗列。
        5. wsd函数支持输出DataFrame数据格式，需要函数添加参数usedf=True，可以使用usedfdt=True来填充DataFrame输出NaT的日期。
    """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_wsd read {code}, {fields}, {begin_time}, {end_time}, {options_}"):
        error_code, df = w.wsd(code, fields, begin_time, end_time, options_, usedf=True)

    if _has_error('wind_wsd', error_code=error_code, codes=code, fields=fields,
                  begin_time=begin_time, end_time=end_time, options=options_):
        return pd.DataFrame()

    return df.rename(columns=str.lower)


def wind_wsd1(code, fields, begin_time=None, end_time=None, **options):
    """
        获取日时间序列函数WSD
        w.wsd（codes, fields, beginTime, endTime, options）
        支持股票、债券、基金、期货、指数等多种证券的基本资料、股东信息、市场行情、证券分析、预测评级、财务数据等各种数据。
        wsd可以支持取 多品种单指标 或者 单品种多指标 的时间序列数据。

        参数	        类型	            可选	默认值	说明
        codes	    str或list	    否	无
            证券代码，支持获取单品种或多品种，如“600030.SH”或[“600010.SH”,“000001.SZ”]

        fields	    str或list	    否	无
            指标列表,支持获取单指标或多指标，，如“CLOSE,HIGH,LOW,OPEN”

        beginTime	str或datetime	是	endTime
            起始日期，为空默认为截止日期，如: "2016-01-01"、“20160101”、“2016/01/01”、"-5D"(当前日期前推5个交易日)、datetime/date类型

        endTime	    str或datetime	是	系统当前日期
            如: "2016-01-05"、“20160105”、“2016/01/05”、"-2D"(当前日期前推2个交易日) 、datetime/date类型

        options	    str	            是	“”
            options以字符串的形式集成多个参数，具体见代码生成器。如无相关参数设置，可以不给option赋值或者使用options=""

        options参数表
        参数	    类型	    可选	    默认值	    说明
        Days	str	    是	    'Trading'	日期选项，参数值含义如下：
                                            Weekdays: 工作日，
                                            Alldays: 日历日，
                                            Trading: 交易日

        Fill	str	    是	    'Blank'	    空值填充方式。参数值含义如下：
                                            Previous：沿用前值，
                                            Blank：返回空值
                                            如需选择自设数值填充，在options添加“ShowBlank=X", 其中X为自设数。

        Order	str	    是	    'A'	        日期排序，“A”：升序，“D”：降序

        Period	str	    是	    'D'	        取值周期。 D：天， W：周， M：月， Q：季度， S：半年， Y：年

        TradingCalendar	str	是	'SSE'	    交易日对应的交易所。参数值含义如下：
                                            SSE ：上海证券交易所， SZSE：深圳证券交易所，
                                            CFFE：中金所，DCE：大商所， CZCE：郑商所，SHFE：上期所，
                                            TWSE：台湾证券交易所， HKEX：香港交易所，
                                            NYSE：纽约证券交易所， COMEX：纽约金属交易所， NYBOT：纽约期货交易所，
                                            CME：芝加哥商业交易所， Nasdaq：纳斯达克证券交易所， NYMEX：纽约商品交易所，
                                            CBOT：芝加哥商品交易所， LME：伦敦金属交易所， IPE：伦敦国际石油交易所

        Currency	str	是	    'Original'	输入币种。Original：“原始货币”， HKD：“港币”， USD：“美元”， CNY：“人民币”

        PriceAdj	str	是	    不复权	    股票和基金(复权方式)。参数值含义如下：
                                            F：前复权， B：后复权，
                                            T：定点复权；债券(价格类型) CP：净价， DP：全价， MP：市价，
                                            YTM：收益率

        注:
        1. Fields和Parameter也可以传入list，比如可以用[“CLOSE”,“HIGH”,“LOW”,“OPEN”]替代“CLOSE,HIGH,LOW,OPEN”;
        2. 获取多个证券数据时，Fields只能选择一个。
        3. 日期支持相对日期宏表达方式，日期宏具体使用方式参考'日期宏’部分内容
        4. options为可选参数，可选参数多个，在参数说明详细罗列。
        5. wsd函数支持输出DataFrame数据格式，需要函数添加参数usedf=True，可以使用usedfdt=True来填充DataFrame输出NaT的日期。
    """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_wsd read {code}, {fields}, {begin_time}, {end_time}, {options_}"):
        df = w.wsd(code, fields, begin_time, end_time, options_, usedf=False)
        error_code = df.ErrorCode
        df = pd.DataFrame(df.Data, index=df.Fields, columns=df.Times)
        df = df.T

    if _has_error('wind_wsd', error_code=error_code, codes=code, fields=fields,
                  begin_time=begin_time, end_time=end_time, options=options_):
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df, columns=df.Fields, index=df.Times)
    return df.rename(columns=str.lower)


def wind_wss(codes, fields, **options):
    """
        获取日截面数据函数WSS
        w.wss（codes, fields, option）
        同样支持股票、债券、基金、期货、指数等多种证券的基本资料、股东信息、市场行情、证券分析、预测评级、财务数据等各种数据。
        但是WSS支持取多品种多指标某个时间点的截面数据。

        参数	        类型	        可选	默认值	说明
        windCodes	str或list	否	无
            证券代码，支持获取单品种或多品种如'600030.SH'或['600010.SH','000001.SZ']
        Fields	    str或list	否	无
            指标列表，支持获取多指标如'CLOSE,HIGH,LOW,OPEN'
        options	    str	        是	""
            options以字符串的形式集成多个参数，具体见代码生成器。如无相关参数设置，可以不给option赋值或者使用options=""

        注:
        1. wss函数一次只能提取一个交易日或报告期数据，但可以提取多个品种和多个指标；
        2. wss函数可选参数有很多，rptDate，currencyType，rptType等可借助代码生成器获取；
        3. wss函数支持输出DataFrame数据格式，需要函数添加参数usedf=True，可以使用usedfdt=True来填充DataFrame输出NaT的日期。
    """

    wind_login()
    options_ = _str_opts(options)
    dfs = []

    for codes_ in _paging(codes, limit=3000):
        # print(len(codes_))
        with Timer(f"wind_wss read {codes_[:4]} {len(codes_)}, {fields}, {options_}"):
            error_code, df = w.wss(codes_, fields, options_, usedf=True)

        if _has_error('wind_wss', error_code=error_code, codes=codes_, fields=fields,
                      options=options_):
            return pd.DataFrame()
        dfs.append(df)
    return _merge_df(dfs)


def wind_wsi(codes, fields, begin_time=None, end_time=None, **options):
    """
        获取分钟序列数据函数WSI
        w.wsi(codes, fields, beginTime, endTime, options)
        用来获取国内六大交易所（上海交易所、深圳交易所、郑商所、上金所、上期所、大商所）证券品种的分钟线数据，
        包含基本行情和部分技术指标的分钟数据，分钟周期为1-60min，技术指标参数可以自定义设置。

        参数	        类型	            可选	默认值	    说明
        codes	    str或list	    否	无	        证券代码，支持获取单品种或多品种，如'600030.SH'或['600010.SH','000001.SZ']
        fields	    str或list	    否	无	        指标列表,支持获取单指标或多指标，，如'CLOSE,HIGH,LOW,OPEN'
        beginTime	str或datetime	是	endTime	    分钟数据的起始时间，支持字符串、datetime/date如: "2016-01-01 09:00:00"
        endTime	    str或datetime	是	当前系统时间	分钟数据的截止时间，支持字符串、datetime/date如: "2016-01-01 15:00:00"，缺省默认当前时间
        options	    str	            是	""	        options以字符串的形式集成多个参数，具体见代码生成器。

        options参数表

        参数	        类型	可选	默认值	    说明
        BarSize	    str	是	“1”	        BarSize在1-60间选择输入整数数字，代表分钟数
        Fill	    str	是	'Blank'	    空值填充方式。参数值含义如下：
                                            Previous：沿用前值， Blank：返回空值
                                        如需选择自设数值填充，在options添加“ShowBlank=X", 其中X为自设数。
        PriceAdj	str	是	U	        股票和基金(复权方式)。参数值含义如下：
                                            U：不复权, F：前复权，B：后复权。

        注：
        1. wsi一次支持提取单品种或多品种，并且品种名带有“.SH”等后缀；
        2. wsi提取的指标fields和可选参数option可以用list实现；
        3. wsi支持国内六大交易(上交所、深交所、大商所、中金所、上期所、郑商所)近三年的分钟数据;
        4. wsi函数支持输出DataFrame数据格式，需要函数添加参数usedf=True，如例2.
        5. wsi支持多品种多指标,单次提取一个品种支持近三年数据，若单次提多个品种,则品种数*天数≤100。

    f"BarSize={bar_size};Fill=Previous"
    """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_wps read {codes[:4]} {len(codes)}, {fields}, {begin_time}, {end_time}, {options_}"):
        error_code, df = w.wsi(codes, fields, begin_time, end_time, options_, usedf=True)

    if _has_error('wind_wps', error_code=error_code, codes=codes, fields=fields,
                  begin_time=begin_time, end_time=end_time, options=options_):
        return pd.DataFrame()
    return _merge_df([df])


def wind_wset(table_name, **options):
    """
        获取报表数据函数WSET
        w.wset(tableName, options)
        用来获取数据集信息，包括板块成分、指数成分、ETF申赎成分信息、分级基金明细、融资标的、融券标的、
        融资融券担保品、回购担保品、停牌股票、复牌股票、分红送转等报表数据。

        参数	        类型	可选	默认值	说明
        tableName	str	否	无      输入获取数据的报表名称，可借助代码生成器生成如：" SectorConstituent "
        options	    str	是	“”          options以字符串的形式集成多个参数，具体见代码生成器

        注:
        1. 数据集涉及内容较多，并且每个报表名称均不同，建议使用代码生成器生成代码，更方便地获取数据；
        2. wset函数支持输出DataFrame数据格式，需要函数添加参数usedf = True, 如例2。可以使用usedfdt=True来填充DataFrame输出NaT的日期。
    """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_wset read {table_name}, {options_}"):
        error_code, df = w.wset(table_name, options_, usedf=True)

    if _has_error('wind_wset', error_code=error_code, table_name=table_name,
                  options=options_):
        return pd.DataFrame()
    return _merge_df([df])


# 热门概念板块 ID 1000044751000000 , sectorid=1000044751000000 windcode=884091.WI


def wind_wpf(product_name, fields, **options):
    """
        获取组合报表数据函数WPF
        w.wpf(productname, tablename, options)
        用来获取资产管理系统PMS以及组合管理系统AMS某一段时间组合的业绩和市场表现的报表数据。

        参数	        类型	可选	默认值	说明
        productName	str	否	无	    输入组合ID或组合名称, 取自Wind终端PMS或AMS模块.如: "全球投资组合管理演示"
        tablename	str	否	否	    输入报表的指标名称如: NetHoldingValue、
        options	    str	是	""	    options以字符串的形式集成多个参数，具体见代码生成器

        options参数表

        参数	        类型	可选	默认值      	说明
        view	    str	是	否   	    选择组合管理模块：“AMS” 或“PMS”
        Owner	    str	是	否	        可选参数, view=PMS且组合是别人共享的时，应给出组合创建人的Wind帐号, 如"Owner=W0817573"
        date	    str	是	否	        选择获取数据中截面指标的日期, 如: "date = 20180302"、
        startDate	str	是	否	        选择获取数据中区间指标的起始日期如: "startDate = 20180531"
        endDate	    str	是	否	        选择获取数据中区间指标的截止日期如: " endDate = 20180731"
        Currency	str	是	"ORIGINAL"	选择获取数据的货币类型，默认"Currency = ORIGINAL"。参数值含义如下：
                                            ORIGINAL：原始货币，HKD：港币，USD：美元，CNY：人民币
        sectorcode	str	是	否	        选择组合按资产或总市值进行分类如: "sectorcode=101"
        MarketCap	str	是	否       	选择组合按总市值分类, 此时"sectorcode=208"
                                            如: " sectorcode=208, MarketCap=1000,500,100,50"
        displaymode	str	是	否	        选择报表展示方式。参数值含义如下：
                                            1：明细， 2：分类， 3：全部
    """

    wind_login()
    options_ = _str_opts(options)
    fields_ = ','.join(fields)

    with Timer(f"wind_wpf read {product_name}, {fields_}, {options_}"):
        error_code, df = w.wpf(product_name, fields_, options_, usedf=True)
        for i in range(5):
            if error_code in [-40521007, -40520007]:
                error_code, df = w.wpf(product_name, fields_, options_, usedf=True)
            else:
                break
    if _has_error('wind_wpf', error_code=error_code, product_name=product_name, fields=fields_,
                  options=options_):
        return pd.DataFrame()
    return _merge_df([df])


def wind_wps(portfolio_name, fields, **options):
    """
        获取组合多维数据函数WPS
        w.wps(PortfolioName, fields, options)
        获取资产管理系统PMS以及组合管理系统AMS某一天组合的基本信息、业绩、市场表现和交易统计等方面的截面数据。

        参数	            类型	可选	默认值	说明
        PortfolioName	str	否	无	    输入组合ID或组合名称, 取自Wind终端PMS或AMS模块.如: "全球投资组合管理演示"
        fields	        str	否	否   	输入报表的指标名称如: NetHoldingValue、
        options	        str	是	""	    options以字符串的形式集成多个参数，具体见代码生成器。

        options参数表

        参数	        类型	可选	默认值	    说明
        view	    str	是	否	        选择组合管理模块：“AMS” 或“PMS”
        Owner	    str	是	否	        可选参数, view=PMS且组合是别人共享的时，应给出组合创建人的Wind帐号, 如"Owner=W0817573"
        date	    str	是	否	        选择获取数据中截面指标的日期, 如: "date = 20180302"、
        startDate	str	是	否	        选择获取数据中区间指标的起始日期如: "startDate = 20180531"
        endDate	    str	是	否	        选择获取数据中区间指标的截止日期如: " endDate = 20180731"
        Currency	str	是	"ORIGINAL"	选择获取数据的货币类型，默认"Currency = ORIGINAL"。参数值含义如下：
                                            ORIGINAL：原始货币，HKD：港币，USD：美元，CNY：人民币

    """

    wind_login()
    options_ = _str_opts(options)
    fields_ = ','.join(fields)

    with Timer(f"wind_wps read {portfolio_name}, {fields_}, {options_}"):
        error_code, df = w.wps(portfolio_name, fields_, options_, usedf=True)

    if _has_error('wind_wps', error_code=error_code, portfolio_name=portfolio_name, fields=fields_,
                  options=options_):
        return pd.DataFrame()
    return _merge_df([df])


def wind_tdays(begin, end, **options):
    """
        获取区间内日期序列tdays
        w.tdays(beginTime , endTime, options)
        用来获取一个时间区间内的某种规则下的日期序列。

        参数	        类型	可选	默认值	    说明
        beginTime	str	是	endTime	    时间序列的起始日期，支持日期宏
        endTime	    str	是	系统当前时间	时间序列的结束日期，支持日期宏
        options	    str	是	”“	        options以字符串的形式集成多个参数，具体见代码生成器。

        options参数表

        参数	            类型	可选	默认值	说明
        Days	        str	是	'Trading'	日期选项。参数值含义如下：
                                                Weekdays: 工作日，Alldays：日历日，Trading：交易日
        Period	        str	是	'D'	        取值周期。参数值含义如下：
                                                D：天，W：周，M：月，Q：季度，S：半年，Y：年
        TradingCalendar	str	是	'SSE'	    选择不同交易所的交易日历，默认'SSE'上交所
    """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_tdays read {begin} {end}, {options_}"):
        wdata_ = w.tdays(begin, end, options_)
        error_code, result = wdata_.ErrorCode, wdata_.Data

    if _has_error('wind_tdays', error_code=error_code, begin=begin, end=end,
                  options=options_):
        return None
    return result


def wind_edb(codes, begin_time=None, end_time=None, **options):
    """
        获取全球宏观经济数据函数EDB
        w.edb(codes, beginTime, endTime, options)

        用来获取Wind宏观经济数据库中的数据信息，为用户提供了一个方便查看及导出宏观/行业板块数据的工具。宏观经济数据库现在包括中国宏观经济、全球宏观经济、行业经济数据、商品数据、利率数据这几大类。

        参数说明
        参数	类型	可选	默认值	说明
        codes	String/ List	否	无	输入获取数据的指标代码，可借助代码生成器生成格式如"M5567877,M5567878" ,["M5567877","M5567878"]
        beginTime	str	是	截止日期	为空默认为截止日期如: "2016-01-01"、“20160101”、“2016/01/01”、"-5D"(当前日期前推5个交易日)、datetime/date类型
        endTime	str	是	系统当前日期	如: "2016-01-05"、“20160105”、“2016/01/05”、"-2D"(当前日期前推2个交易日) 、datetime/date类型
        集成在options中的参数
        options以字符串的形式集成了多个参数。以下列举了一些常用的参数：

        参数	类型	可选	默认值	说明
        Fill	str	是	'Blank'	空值填充方式。参数值含义如下：
        Previous：沿用前值，
        Blank：返回空值

        如需选择自设数值填充，在options添加“ShowBlank=X", 其中X为自设数。
        注:

        edb函数对接Wind终端宏观经济数据库, 其中的指标一般都可以通过API下载；

        edb函数支持输出DataFrame数据格式，需要函数添加参数usedf = True, 如例1。可以使用usedfdt=True来填充DataFrame输出NaT的日期。

        返回说明
        如果不指定usedf=True，该函数将返回一个WindData对象，包含以下成员：

        返回码	解释	说明
        ErrorCode	错误ID	返回代码运行错误码，.ErrorCode =0表示代码运行正常。若为其他则需查找错误原因.
        Data	数据列表	返回函数获取的数据，比如读取 M5567878,M5567879从'2017-05-08'到'2017-05-18'的季度数据，返回值为.Data[[83128.5,85232.9,96176.7,77451.3,91847.2],[104097.2,107120.5,114167.0,112427.8,115147.8]]
        Codes	证券代码列表	返回获取数据的证券代码列表.Codes=[M5567878,M5567879]
        Field	指标列表	返回获取数据的指标列表.Fields=[CLOSE]
        Times	时间列表	返回获取数据的日期序列.Times=[20170508,20170509,20170510,20170511,20170512,20170515,20170516,20170517,20170518]
        示例说明
        # 提取我国近十年三大产业的GDP值
        from datetime import *
        w.edb("M0001395,M0001396,M0001397,M0001400,M0028610,M0045788","ED-10Y","2017-06-28","Fill=Previous",usedf = True)
        """

    wind_login()
    options_ = _str_opts(options)

    with Timer(f"wind_EDB read {codes[:4]} {len(codes)},  {begin_time}, {end_time}, {options_}"):
        error_code, df = w.edb(codes, begin_time, end_time, options_, usedf=True)

    if _has_error('wind_EDB', error_code=error_code, codes=codes,
                  begin_time=begin_time, end_time=end_time, options=options_):
        return pd.DataFrame()
    return _merge_df([df])


def _str_opts(options):
    return ';'.join([f'{k}={v}' for k, v in options.items()])


def _paging(codes, limit=None):
    limit = limit or __MAX_PAGING_SIZE__
    return [codes[i:i + limit] for i in range(0, len(codes), limit)]


def _has_error(name, error_code, codes=None, **kwargs):
    if error_code == 0:
        return False
    write_error(f'wind read error. {name} error code: {error_code} {error_dict.get(error_code)}. {kwargs}')
    if codes:
        write_error(f'wind read error. {name} codes: {codes}')
    return True


def _merge_df(dfs):
    return pd.concat([df.rename(columns=str.lower) for df in dfs], axis='rows')
