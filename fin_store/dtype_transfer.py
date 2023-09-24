
# todo：这个函数检查
def dtype_trans(df):
    df_tmp = df.copy()
    for col in df_tmp.select_dtypes(include=['float']).columns:
        df_tmp[col] = df_tmp[col].fillna(0.0).astype('float64')
    for col in df_tmp.select_dtypes(include=['int']).columns:
        df_tmp[col] = df_tmp[col].fillna(0).astype('int')
    for col in df_tmp.select_dtypes(include=['string', 'object']).columns:
        df_tmp[col] = df_tmp[col].fillna('')
    return df_tmp
