import datetime
import decimal
import re
from urllib.parse import quote_plus

import pandas as pd
import pandas.api.types as types
import sqlalchemy as sql
from sqlalchemy.dialects.mysql import insert

from fin_store import write_log, write_error, Timer, get_section


class Query(object):

    def __init__(self, sql_str, engine=None, **params):
        self.sql_str = sql_str
        self.engine = engine
        self.params = params

    def execute(self, sql_str):
        try:
            res = self.engine.execute(sql_str)
            return [r for r in res.mappings()]
        except:
            write_error(f'sql exception: {sql_str}')
            return []

    def get_table_name(self):
        names_ = parse_table_names(self.sql_str)
        if names_:
            return names_
        write_error(f'can not resolve table name. {self.sql_str}')
        return None

    def has_table(self):
        names_ = self.get_table_name()
        for name_ in names_:
            if not sql.inspect(self.engine).has_table(name_):
                write_error(f'table {name_} not exists. in {self.sql_str}')
                return False
        return True

    def get_dicts(self, **params):
        return self.get_df(**params).to_dict(orient='records')

    def get_df(self, **params):
        params_ = dict(self.params, **params)
        sql_ = self.sql_str.format(**params_)
        if not self.has_table():
            return pd.DataFrame()
        with Timer(f"run sql {sql_}"):
            res_ = pd.read_sql(sql_, con=self.engine.connect())
            return to_dtypes(res_)

    def get_first_dict(self, **params):
        res_ = self.get_dicts(**params)
        return res_[0] if res_ else dict()

    def get_first(self, default_value=None, **params):
        res_ = self.get_dicts(**params)
        if res_ and res_[0]:
            return list(res_[0].values())[0]
        return default_value


def query_table(sql_str, sections=None, engine=None, **params) -> Query:
    sections_ = [sections] if isinstance(sections, str) else sections
    sections_ = sections_ or ['mysql']
    engine_ = engine or create_engine(*sections_)
    return Query(sql_str=sql_str, engine=engine_, **params)


def replace_table(df: pd.DataFrame, table_name, primary_keys, *sections, engine=None, columns=None):
    engine_ = engine or create_engine(*sections)
    drop_table(table_name=table_name, engine=engine_)
    insert_table(df, table_name=table_name, primary_keys=primary_keys, engine=engine_, columns=columns)


def truncate_table_V2(table_name, *sections, engine=None):
    engine_ = engine or create_engine(*sections)
    truncate_table(table_name=table_name, engine=engine_)


def replace_data(df: pd.DataFrame, table_name, primary_keys, *sections, engine=None):
    engine_ = engine or create_engine(*sections)
    truncate_table(table_name=table_name, engine=engine_)
    insert_table(df, table_name=table_name, primary_keys=primary_keys, engine=engine_)


def delete_table(table_name, engine, **cond):
    with engine.connect() as conn:
        if not sql.inspect(engine).has_table(table_name):
            return
        _table = sql.Table(table_name, sql.MetaData(), autoload_with=engine)
        stmt = sql.delete(_table)
        for k, v in cond.items():
            stmt = stmt.where(_table.columns[k] == v)
        res_ = conn.execute(stmt, engine)
        return res_.rowcount


def insert_table(df: pd.DataFrame, table_name, primary_keys, *sections, engine=None, columns=None):
    if df.empty:
        return
    engine_ = engine or create_engine(*sections)
    table_ = df_to_table(df, table_name, primary_keys, columns=columns)
    inspect_ = sql.inspect(engine_)
    if not inspect_.has_table(table_name):
        table_.create(engine_)
        write_log(f'create table {table_}')

    items = to_entities(df)
    # df.to_sql(name=table_name, con=engine, index_label=primary_keys)
    with engine_.connect() as conn:
        result = conn.execute(sql.insert(table_), items)
    return result


def update_table(df, table_name, primary_keys, *sections, engine=None):
    if df.empty:
        return
    engine_ = engine or create_engine(*sections)

    df = df.reset_index().drop_duplicates(subset=primary_keys, keep="last")
    table_ = df_to_table(df, table_name, primary_keys)
    with engine_.connect() as conn:
        for item in to_entities(df):
            stmt = sql.update(table_)
            for pk in primary_keys:
                stmt = stmt.where(table_.columns[pk] == item[pk])
            conn.execute(stmt, item)


def upsert_table(df, table_name, primary_keys, *sections, engine=None, columns=None):
    if df.empty:
        return
    engine_ = engine or create_engine(*sections)
    table_ = df_to_table(df, table_name, primary_keys, columns=columns)
    inspect_ = sql.inspect(engine_)
    if not inspect_.has_table(table_name):
        table_.create(engine_)
        write_log(f'create table {table_}')

    with engine_.connect() as conn:
        for item in to_entities(df):
            stmt = insert(table_).values(**item)
            conn.execute(stmt.on_duplicate_key_update(**item))


def truncate_table(table_name, engine):
    with engine.connect() as conn:
        inspect_ = sql.inspect(engine)
        if inspect_.has_table(table_name):
            conn.execute(f'truncate {table_name}')


def drop_table(table_name, engine):
    with engine.connect() as conn:
        inspect_ = sql.inspect(engine)
        if inspect_.has_table(table_name):
            write_log(f'drop table {table_name}')
            conn.execute(f'drop table {table_name}')


def execute_sql(sql_str, *sections, engine=None):
    engine_ = engine or create_engine(*sections)
    with engine_.connect() as conn:
        write_log(f'run sql {sql_str}')
        conn.execute(sql_str)


def to_dtypes(df: pd.DataFrame):
    if df.empty:
        return df
    sample_ = df.sample(1).iloc[0]
    float_cols = [(k, float) for (k, v) in sample_.items() if isinstance(v, decimal.Decimal)]
    if float_cols:
        df = df.astype(dict(float_cols))
    return df.convert_dtypes()


def to_entities(df: pd.DataFrame):
    temp = df.astype(object)
    for col in df.columns:
        if types.is_datetime64_any_dtype(df[col].dtype) \
                or types.is_datetime64_dtype(df[col].dtype):
            temp[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    temp = temp.where(cond=pd.notna(temp), other=None)
    return temp.to_dict('records')


def create_engine(*sections):
    return mysql_engine(**(get_section(*sections)))


def mysql_engine(host, user, pwd, schema='', port=3306, **kwargs):
    url_ = f"mysql+pymysql://{user}:{quote_plus(pwd)}@{host}:{port}/{schema}?charset=utf8&ssl_disabled=True"
    write_log(f'create sql {url_}')
    return sql.create_engine(url_)


def to_count_sql(str_sql):
    a1, a2 = re.search(r'[sS][eE][lL][eE][cC][tT]', str_sql).span()
    b1, b2 = re.search(r'[fF][rR][oO][mM]', str_sql).span()
    return str_sql[:a2] + ' count(*) ' + str_sql[b1:]


def parse_table_names(str_sql):
    p_ = re.compile(r'([fF][rR][oO][mM]|[jJ][oO][iI][nN])\s+([`\w]+)\s*')
    return [x[1].strip('`') for x in (re.findall(p_, str_sql))]


def test_parse_table_names():
    sql_ = f"""
        SELECT * 
        FROM (select * from main_table) AS a
            INNER JOIN 
                some_table_2 AS b ON a.code = b.code
            LEFT JOIN
                (SELECT * FROM some_table_3) AS c ON a.code = c.code
            JOIN some_table_4 AS d ON a.code = d.code
        WHERE a.period_end = '2022-01-01'
        """
    names_ = parse_table_names(sql_)
    assert names_ == ['main_table', 'some_table_2', 'some_table_3', 'some_table_4']


"""
    Database Writer
"""


def df_to_table(df: pd.DataFrame, table_name, primary_keys=None, comment=None, log_time='update_time',
                columns=None) -> sql.Table:
    keys_ = primary_keys or []
    cols_ = dict([(k, dict(data_type=v, is_unique=(k in keys_))) for k, v in df.dtypes.items()])
    if log_time and log_time not in cols_.keys():
        cols_[log_time] = dict(data_type='datetime', doc='更新日期',
                               default=datetime.datetime.now())  # add by zkl@20221206 新增默认时间 default=datetime.datetime.now
    if isinstance(columns, dict):
        for k, v in columns.items():
            if k in cols_.keys():
                cols_[k].update(**v)
    return to_sql_table(table_name, columns=cols_, comment=comment)


def entity_to_table(entity, table_name, comment=None):
    table_name_ = table_name or _get_attr(entity, '__table_name__')
    comment_ = comment or _get_attr(entity, '__comment__') or entity.__doc__.strip()

    cols = dict([(x, getattr(entity, x)) for x in entity.__dir__() if not x.startswith('__')])

    return to_sql_table(table_name_, columns=cols, comment=comment_)


def to_sql_table(table_name: str, columns: dict, comment: str = None) -> sql.Table:
    cols_ = [to_sql_column(n, meta=e) for (n, e) in columns.items()]
    return sql.Table(table_name, sql.MetaData(), *cols_, mysql_charset='utf8', comment=comment)


def to_sql_column(name, data_type=None, is_unique=False, doc=None, length=None, precision=None, scale=None,
                  default=None,  # add by zkl@20221206 新增默认值
                  meta=None) -> sql.Column:
    dt_ = (data_type or _get_attr(meta, 'data_type') or _get_attr(meta, 'dt'))
    sql_type_ = to_sql_type(data_type=dt_,
                            length=(length or _get_attr(meta, 'length')),
                            precision=(precision or _get_attr(meta, 'precision')),
                            scale=(scale or _get_attr(meta, 'scale')))
    return sql.Column(name, sql_type_,
                      autoincrement=False,
                      primary_key=(is_unique or _get_attr(meta, 'is_unique')),
                      doc=(doc or _get_attr(meta, 'doc')),
                      default=default or _get_attr(meta, 'default'))  # add by zkl@20221206 新增默认时间


def to_sql_type(data_type, length=None, precision=None, scale=None):
    # todo 可根据数据库连接地址确定使用哪个类型的 adapter
    from fin_store.datatypes import MySQLDataTypeAdapter
    return MySQLDataTypeAdapter().to_sql_type(data_type, length, precision, scale)


def _get_attr(target, name, default_value=None):
    if isinstance(target, dict):
        return target.get(name, default_value)
    if name in target.__dir__():
        return getattr(target, name)
    return default_value


def test_model():
    class Entity(object):
        """说明"""
        table_name = ""
