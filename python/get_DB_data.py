from datetime import datetime, timedelta

import pandas as pd

from connect_DB import official_DB_connection, test_DB_connection

connection_official = official_DB_connection()
connection_test = test_DB_connection()

def get_original_data():
    
    # SQLQuery_original =f'''
    # select filename, comp_id, reals_date, od, subject, txt
    # from twn.push.p_u11_now
    # where fin_tablename = 'fin.estm_u11_board'
    # and subject !~ '代子公司'
    # and reals_date = '{date}'
    # '''
    
    # SQLQuery_original =f'''
    # select filename, comp_id, reals_date, od, subject, txt
    # from twn.push.p_u11_now
    # where fin_tablename = 'fin.estm_u11_board'
    # and subject !~ '代子公司'
    # and reals_date between '20240601' and '20241231'
    # '''

    SQLQuery_original = '''
    with keyin as (
    select zdate, lag(zdate, 2) over (order by zdate) as keyin_am, lag(zdate) over (order by zdate) as keyin_pm
    from twn.stk.attr_tradingday
    where tradeday_cno != 0
    and zdate between current_date - 20 and current_date
    order by zdate desc
    limit 1
    )
    select filename, comp_id, reals_date, od, subject, txt
    from twn.push.p_u11_now
    where fin_tablename = 'fin.estm_u11_board'
    and subject !~ '代子公司'
    and case when now() < current_date + interval '12 hours' then keyin >= ( select keyin_am from keyin ) else keyin >= ( select keyin_pm from keyin ) end
    '''
    
    original_data = pd.read_sql(SQLQuery_original, connection_official)
    print(f'本日重訊公告檔有 {len(original_data)} 筆資料')
    
    return original_data

# ================================================================================================================== 
def get_extract_data():
    
    '''
    取得過去 30 天的資料，用來判斷每天的重訊資料之前是否有做過
    '''
    date = datetime.today()
    pre_date = (date - timedelta(30)).strftime('%Y%m%d')
    
    SQLQuery_extract = f"""
    select *
    from twn.fin.estm_u11_board
    where reals_date >= '{pre_date}'
    """

    return pd.read_sql(SQLQuery_extract, connection_test)

# ==================================================================================================================
def get_pm_data():
    
    '''
    取得截字參數檔，用以建立各科目對應的正規表達式
    '''
    
    SQLQuery_pm = '''
    select *
    from twn.fin.ini_estm_u11_board 
    '''
    
    return pd.read_sql(SQLQuery_pm, connection_test)

# ==================================================================================================================   
def get_stdid_data():
    
    SQLQuery_stdid ='''
    select tej_comp_id, comp_id, fst_list_date
    from twn.basic.attr_stdid
    '''
    
    stdid_df = pd.read_sql(SQLQuery_stdid, connection_official)
    stdid_df['fst_list_date'] = stdid_df['fst_list_date'].astype(str)
    stdid_df['fst_list_date'] = [date.replace('-', '') for date in stdid_df['fst_list_date']] 
    
    return stdid_df

# ==================================================================================================================
def get_fininfo_data():    
    
    SQLQuery_fininfo ='''
    select tej_comp_id, fin_year, quarter, fin_aq, fin_type
    from twn.fin.fin_fininfo_m
    where fin_year >= '20200101'
    '''
    
    fininfo_df = pd.read_sql(SQLQuery_fininfo, connection_official)
    
    return fininfo_df

# ==================================================================================================================
def get_fiscal_data():
    
    SQLQuery_fiscal ='''
    select tej_comp_id, fin_end_date, fiscal_month
    from twn.fin.event_fiscal_month
    where fin_end_date = '2999-12-31'
    '''
    
    fiscal_df = pd.read_sql(SQLQuery_fiscal, connection_official)
    fiscal_df['fin_end_date'] = fiscal_df['fin_end_date'].astype(str)
    
    return fiscal_df 

# ==================================================================================================================
def get_sale_data():
    
    SQLQuery_sale ='''
    select tej_comp_id, zyymm, t8104
    from twn.fin.sale_stat_out
    where zyymm >= '20200101'
    '''

    sale_df = pd.read_sql(SQLQuery_sale, connection_official)
    sale_df['zyymm'] = sale_df['zyymm'].astype(str)
    sale_df['zyymm'] = [date.replace('-', '')[:6] for date in sale_df['zyymm']]
    
    return sale_df

# ==================================================================================================================
def get_fin_ind_data():
    
    SQLQuery_fin_ind ='''
    select tej_comp_id, fin_end_date, fin_ind
    from twn.fin.event_fin_ind
    where fin_end_date = '2999-12-31'
    '''

    fin_ind_df = pd.read_sql(SQLQuery_fin_ind, connection_official)
    
    return fin_ind_df