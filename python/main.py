import sys

from connect_DB import test_DB_connection
from get_DB_data import get_original_data, get_extract_data
from extract_data_process import set_original_extract, set_regex_fields, extract_original_data, handle_data, fill_empty_fin_type
from upload_DB import upload_u11_board, upload_u11_type_board
from check_rules_to_5330 import produce_5330, data_5330_to_excel

def main():

    df = get_original_data()
    if len(df) == 0:
        print('重訊公告無資料，結束程式！')
        sys.exit(1)
    else:
        pre_extract = get_extract_data()

        # 若重訊公告檔的 filename 與 estm_u11_board 的 filename 有重複 (即先前已做過)，將刪除這些重複的資料再進行截字
        df = df[~df['filename'].isin(pre_extract['filename'])]
    
    extract = set_original_extract(df)
    fields = set_regex_fields()
    extract_data, extract = extract_original_data(df, fields, extract)
    extract = handle_data(extract)
    extract = fill_empty_fin_type(extract)

    connection_test = test_DB_connection()
    upload_u11_board(extract, connection_test)

    data_5330 = produce_5330(extract, extract_data, df)
    if len(data_5330) == 0:
        print('本日無資料進入 5330！')
    else:
        print(f'本日共有 {len(data_5330)} 筆資料進入 5330。')
        data_5330_to_excel(data_5330)

    upload_u11_type_board(extract, data_5330, connection_test)

if __name__ == '__main__':
    main()
