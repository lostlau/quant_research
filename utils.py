import re
import pandas as pd
import numpy as np
import baostock as bs
import akshare as ak

"""
命名：驼峰转下划线
"""
def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

"""
兼容nan的加权平均
"""
def nan_wm(vec, weights=None):
    if weights is None:
        return np.nanmean(vec)
    else:
        return np.nansum(np.multiply(vec,weights))/np.sum(np.multiply(weights,~np.isnan(vec)))

"""
获取当日指数成分股票列表
    -- by akshare
"""
def get_index_components(index_code='399102'):
    stock_list = ak.index_stock_cons(index_code)["品种代码"].apply(ak.stock_a_code_to_symbol).apply(lambda s:s[:2]+'.'+s[2:]).values
    return np.unique(stock_list)

"""
获取当日中证指数成分股票列表
    -- by akshare
"""
def get_csindex_components(index_code='931643'):
    stock_list = ak.index_stock_cons_csindex(symbol=index_code)['成分券代码'].apply(ak.stock_a_code_to_symbol).apply(lambda s:s[:2]+'.'+s[2:]).values
    return np.unique(stock_list)


"""
获取股票列表的历史K线
    -- by baostock
"""
def get_history_k(stock_list, stt_date, end_date, freq='d', adjust='1'):
    bs.login()
    output_list = []
    for stk in stock_list:
        rs = bs.query_history_k_data_plus(stk, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST",
                start_date=stt_date, end_date=end_date,
                frequency=freq, adjustflag=adjust)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        output_list.append(pd.DataFrame(data_list, columns=rs.fields))
    df_output = pd.concat(output_list)
    # 格式转换
    df_output['date'] = df_output['date'].astype('datetime64[ns]')
    for col in np.setdiff1d(df_output.columns, ['code', 'date']):
        df_output[col] = pd.to_numeric(df_output[col])
    # 字段名转换
    df_output.columns = df_output.columns.map(camel_to_snake)
    bs.logout()
    return df_output


"""
获取股票列表的历史财务指标
    -- by baostock
"""
def df_multi_merge(df_list, on_cols, how='left'):
    for i, df in enumerate(df_list):
        if i > 0:
            df_output = pd.merge(df_output, df, on=on_cols, how=how)
        else:
            df_output = df.copy()
    return df_output


def get_rs_target(stock_list, year, quarter, target='profit'):
    if target == 'profit':
        api_query_target = bs.query_profit_data
    elif target == 'operation':
        api_query_target = bs.query_operation_data
    elif target == 'growth':
        api_query_target = bs.query_growth_data
    elif target == 'balance':
        api_query_target = bs.query_balance_data
    elif target == 'cashflow':
        api_query_target = bs.query_cash_flow_data
    elif target == 'dupont':
        api_query_target = bs.query_dupont_data
    else:
        raise Exception('this target is not implemented, pls change.')
    output_list = []
    # len_ = len(stock_list)
    for i, stk in enumerate(stock_list):
        # print("\r", end="")
        # print(f"   {target}:{100 * (1 + i) / len_:>0.2f}%", "▋" * (((i + 1) * 100) // len_), end="")
        # sys.stdout.flush()
        # 查询季频估值指标盈利能力
        target_list = []
        rs_target = api_query_target(code=stk, year=year, quarter=quarter)
        while (rs_target.error_code == '0') & rs_target.next():
            target_list.append(rs_target.get_row_data())
        output_list.append(pd.DataFrame(target_list, columns=rs_target.fields))
    return pd.concat(output_list)


def get_finance_indicator(stock_list, stt_date, end_date):
    bs.login()
    all_output_list = []
    yq_list = [(dd.year, dd.quarter) for dd in pd.date_range(stt_date, end_date, freq='Q')]
    for (y, q) in yq_list:
        t0 = time.time()
        print(f'-- 正在获取{y}Q{q}...')
        yq_output_list = []
        for target in ['profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont']:
            yq_output_list.append(get_rs_target(stock_list, y, q, target))
            # print('')
        all_output_list.append(df_multi_merge(yq_output_list, on_cols=['code', 'pubDate', 'statDate']))
        print(f"--     耗时：{(time.time()-t0)/60:>0.2f} mins")
    df_output = pd.concat(all_output_list)
    # 格式转换
    df_output['pubDate'] = df_output['pubDate'].astype('datetime64[ns]')
    df_output['statDate'] = df_output['statDate'].astype('datetime64[ns]')
    for col in np.setdiff1d(df_output.columns, ['code', 'pubDate', 'statDate']):
        df_output[col] = pd.to_numeric(df_output[col])
    # 字段名转换
    df_output.columns = df_output.columns.map(camel_to_snake)
    bs.logout()
    return df_output