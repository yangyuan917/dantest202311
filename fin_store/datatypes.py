import datetime

import pandas.api.types as types
import sqlalchemy as sql


class DataTypeAdapter:
    def to_sql_type(self, data_type, length=None, precision=None, scale=None):
        pass


class MySQLDataTypeAdapter(DataTypeAdapter):

    def to_sql_type(self, data_type, length=None, precision=None, scale=None):
        from sqlalchemy.dialects import mysql

        if types.is_float_dtype(data_type) \
                or isinstance(data_type, float):
            return sql.Numeric(precision=(precision or 20), scale=(scale or 6))
        if types.is_integer_dtype(data_type) \
                or isinstance(data_type, int):
            return sql.Integer()
        if types.is_datetime64_dtype(data_type) \
                or types.is_datetime64_any_dtype(data_type) \
                or types.is_datetime64tz_dtype(data_type) \
                or types.is_datetime64_ns_dtype(data_type) \
                or isinstance(data_type, datetime.datetime):
            return sql.DateTime()
        if isinstance(data_type, datetime.date):
            return sql.Date()
        if isinstance(data_type, datetime.time):
            return sql.Time()

        if isinstance(data_type, str) and data_type in ['float']:
            return sql.Numeric(precision=(precision or 20), scale=(scale or 6))
        if isinstance(data_type, str) and data_type in ['int']:
            return sql.Integer()
        if isinstance(data_type, str) and data_type in ['long']:
            return sql.BigInteger()
        if isinstance(data_type, str) and data_type in ['datetime']:
            return sql.DateTime()
        if isinstance(data_type, str) and data_type in ['date']:
            return sql.Date()
        if isinstance(data_type, str) and data_type in ['time']:
            return sql.Time()
        if isinstance(data_type, str) and data_type in ['text']:
            return sql.Text()
        if isinstance(data_type, str) and data_type in ['longtext']:
            return mysql.LONGTEXT()
        return sql.String(length or 200)
