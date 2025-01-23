import pandas as pd

col_zname = [
    "filename", "comp_id", "reals_date", "od", "fin_type", "board_date", 
    "audit_committee_date", "fin_beg_date", "fin_end_date", "t3100_txt", 
    "t310c_txt", "t341c_txt", "t340a_txt", "t3295_txt", "t3395_txt", 
    "t3900_txt", "t3970_txt", "t3950_txt", "t3990_txt", "t0010_txt", 
    "t1000_txt", "t200e_txt", "rmk", "t3100", "t310c", "t341c", 
    "t340a", "t3295", "t3395", "t3900", "t3970", "t3950", "t3990", 
    "t0010", "t1000", "t200e", "pre_fin_type"
]

def upload_u11_board(extract, connection):

    extract_zcol = extract.copy()
    extract_zcol.columns = col_zname
    
    # 插入資料
    try:
        cursor = connection.cursor()

        # 生成 INSERT 語法的欄位部分
        columns = ", ".join(extract_zcol.columns)
        placeholders = ", ".join(["%s"] * len(extract_zcol.columns))  # 將每個值用 %s 佔位

        sql_query = f"""
        INSERT INTO twn.fin.estm_u11_board ({columns})
        VALUES ({placeholders})
        """

        # 將 DataFrame 的值轉換成元組格式並插入
        rows_to_insert = [tuple(row) for row in extract_zcol.to_numpy()]
        rows_to_insert = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in rows_to_insert
        ]
        cursor.executemany(sql_query, rows_to_insert)  # 批量插入

        # 提交變更
        connection.commit()

        print(f"成功插入 {cursor.rowcount} 行資料至 twn.fin.estm_u11_board！")

    except Exception as e:
        print(f"twn.fin.estm_u11_board 資料庫匯入時發生錯誤: {e}")
        connection.rollback()
        
# ==================================================================================================================
def upload_u11_type_board(extract, data_5330, connection):

    extract_zcol = extract.copy()
    extract_zcol.columns = col_zname
    
    pass_data = extract_zcol.drop(data_5330.index, errors='ignore')
    pass_data_col = [
        'comp_id', 'reals_date', 'od', 'fin_type', 'board_date',
        'audit_committee_date', 'fin_beg_date', 'fin_end_date',
        't3100', 't310c', 't341c', 't340a', 't3295', 't3395', 
        't3900', 't3970', 't3950', 't3990', 't0010', 't1000', 
        't200e', 'rmk'
    ]

    # 新增 currency 欄位並設置值為 'TWD'
    pass_data = pass_data[pass_data_col]
    fin_end_date_idx = pass_data_col.index('fin_end_date')
    pass_data.insert(fin_end_date_idx + 1, 'currency', 'TWD')
    
    # 插入資料
    try:
        cursor = connection.cursor()

        # 生成 INSERT 語法的欄位部分
        columns = ", ".join(pass_data.columns)
        placeholders = ", ".join(["%s"] * len(pass_data.columns))  # 將每個值用 %s 佔位

        sql_query = f"""
        INSERT INTO twn.fin.estm_u11_type_board ({columns})
        VALUES ({placeholders})
        """

        # 將 DataFrame 的值轉換成元組格式並插入
        rows_to_insert = [tuple(row) for row in pass_data.to_numpy()]
        rows_to_insert = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in rows_to_insert
        ]
        cursor.executemany(sql_query, rows_to_insert)  # 批量插入

        # 提交變更
        connection.commit()

        print(f"成功插入 {cursor.rowcount} 行資料至 twn.fin.estm_u11_type_board！")

    except Exception as e:
        print(f"twn.fin.estm_u11_type_board 資料庫匯入時發生錯誤: {e}")
        connection.rollback()
        