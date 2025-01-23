import re
from datetime import datetime

import pandas as pd

from get_DB_data import get_pm_data, get_stdid_data, get_fininfo_data

pm_data = get_pm_data()
col_cname = [
    "檔名", "公司碼", "發言日期", "則次", "合併M/個別A", "董事會決議日", "審計委員會通過日", 
    "財務起日", "財務迄日", "營業收入_TXT", "營業收益_證券業_TXT", "利息淨收益_銀行業_TXT", 
    "淨收益_銀行業_TXT", "營業毛利_TXT", "營業利益_TXT", "稅前淨利_TXT", "本期淨利_TXT", 
    "歸屬於母公司業主淨利_TXT", "基本每股盈餘_TXT", "期末總資產_TXT", "期末總負債_TXT", 
    "歸屬於母公司權益_TXT", "其他應敘明事項", "營業收入", "營業收益_證券業", "利息淨收益_銀行業", 
    "淨收益_銀行業", "營業毛利", "營業利益", "稅前淨利", "本期淨利", "歸屬於母公司業主淨利", 
    "基本每股盈餘", "期末總資產", "期末總負債", "歸屬於母公司權益", "截字fin_type"
]

# 設定最初的 extract 資料表: 先將欄位名稱設為 col_cname，並填入原始資料庫 df 中的檔名、公司碼、發言日期、則次
def set_original_extract(df):
    
    extract = pd.DataFrame(columns=col_cname)
    extract['檔名'] = df['filename']
    extract['公司碼'] = df['comp_id']
    extract['發言日期'] = df['reals_date']
    extract['則次'] = df['od']
        
    return extract

# ==================================================================================================================
# 設定截字欄位及對應的regex
def set_regex_fields():

    fields = {}

    # 讀取截字參數檔的中文欄位名稱和 substring_beg，並設立對應欄位需要的正規表達式，放入 fields 字典
    for idx in range(len(pm_data)):
        col = pm_data['col_cname'][idx]
        substring_beg = pm_data['substring_beg'][idx]
        
        substring_beg = re.sub(r'\(', r'\\s*[(（]', substring_beg)
        substring_beg = re.sub(r'\)', r'[)）]', substring_beg)
        # 修改 ':' 為指定格式並構建正則表達式
        if col == '其他應敘明事項':
            regex = re.sub(r'[:：]', r'\\s*[:：]*\\s*(.*)(?=\n|$)', substring_beg)
            fields[col] = regex
        else:
            regex = re.sub(r'[:：]', r'\\s*[:：]*\\s*(.*?)(?=\n|$)', substring_beg)
            fields[col] = regex

    fields['財務起日'] = r"起訖日期.*?(\d{2,4}[年./-]*\d{1,2}[月./-]*\d{1,2}[日./]?)\s*[~～-]"
    fields['財務迄日'] = r"起訖日期.*?[~～-]\s*(\d{2,4}[年./-]*\d{1,2}[月./-]*\d{1,2}[日./]?)"
    fields['截字fin_type'] = r"(?:季|年|年度|度)(合併|個別)?(?:財務報告|財務報表|財報)"
    
    return fields

# ==================================================================================================================
# 主要截字程式
def extract_original_data(df, fields, extract):
    txt_data = []
    date_col = ['董事會決議日', '審計委員會通過日', '財務起日', '財務迄日']

    for idx in range(len(df)):
        
        subject = df['subject'][idx]
        txt = df['txt'][idx]
        
        row = {}

        # 使用正則表達式提取其他欄位數據
        for field, pattern in fields.items():
            
            if field == '截字fin_type':
                
                match = re.search(pattern, subject, re.S)
            
                if match:
                    value = next((g for g in match.groups() if g), None)
                    row[field] = value if value else ''
                else:
                    row[field] = ''
            
            elif field in date_col:  
                
                match = re.search(pattern, txt, re.S)
            
                if match:
                    value = next((g for g in match.groups() if g), None)
                    row[field] = value if value else None
                else:
                    row[field] = None
                
            else:
                
                match = re.search(pattern, txt, re.S)
                
                if match:
                    value = next((g for g in match.groups() if g), None)
                    
                    if field == '其他應敘明事項' and '其他應敘明事項' in value: 
                        # 針對例外txt格式除錯，可參考:'U11-6886-20230412-2.xml' & 'U11-6984-20230814-4.xml'
                        if '因應措施' in value:
                            value = value.split('\n3.因應措施')[0]
                            row[field] = value if value else ''
                        else:
                            match_ = re.search(pattern, value, re.S)
                            value_ = next((g for g in match_.groups() if g), None)
                            row[field] = value_ if value_ else ''
                    else:
                        row[field] = value if value else ''
                else:
                    row[field] = ''
        
        txt_data.append(row)

    # 將結果轉為 DataFrame
    extract_data = pd.DataFrame(txt_data)

    # 更新原來的 extract DataFrame
    extract.update(extract_data)
    
    return extract_data, extract


# ==================================================================================================================
# 透過以下函數將日期整理成YYYYMMDD格式
def normalize_date(date):
    
    # 判斷 nan
    if pd.isna(date):
        return date
    
    elif any(keyword in date for keyword in ['不適用', 'NA', '無']):
        return None
    
    # U11-5301-20230328-1.xml 起訖日期(XXX/XX/XX~XXX/XX/XX):111/01/01~12/31 -> 迄日：None
    elif len(date) < 6:
        return None     
    
    # 移除不必要的符號
    date_ = re.sub(r"[^\d年月日./-]", "", date)  # 移除非數字、年月日和斜線的符號
    date_ = re.sub(r"/{2,}", "/", date_)        # 將多餘的 `//` 替換成單一 `/`
    
    # 取得系統年 & 系統年 -1 年
    current_AD = datetime.today().strftime('%Y')
    current_ROC = str(int(current_AD) - 1911)
    last_ROC = str(int(current_ROC) - 1)
    
    # 正則表達式匹配不同格式的日期
    match = re.search(r"(\d{6,8})", date_)
    if match:
        if match.group(1):
            year, month, day = match.group(1)[:-4], match.group(1)[-4:-2], match.group(1)[-2:]
        # else:
        #     raise ValueError("無法解析日期格式")
        
        # 將民國年轉為西元年
        if len(year) == 2 and int(year) < 77:
            year = str(int('1' + year) + 1911) # 有人打 12/08/08
        elif len(year) == 2 or len(year) == 3:
            year = str(int(year) + 1911)
        elif len(year) == 4 and current_ROC in year:
            year = str(int(current_ROC) + 1911) # 有人打 1113 1130
        elif len(year) == 4 and last_ROC in year:
            year = str(int(last_ROC) + 1911)  # 有人打 1113 1130
            
        if int(month) > 12:
            print('month超過合理值: %s' % date)
            return None
        elif month in ['01', '03', '05', '07', '08', '10', '12']:
            if int(day) > 31:
                print('day超過合理值: %s' % date)
                return None
        elif month in ['04', '06', '09', '11']:
            if int(day) > 30:
                print('day超過合理值: %s' % date)
                return None
        elif month == '02':
            if int(day) > 29:
                print('day超過合理值: %s' % date)
                return None
        else:
            print('month不符合合理值: %s' % date)
            return None
            
        return f"{year}{month}{day}"
    
    else:
        match = re.search(r"(\d{2,4})[年./-]?(\d{1,2})[月./-]?(\d{1,2})[日./]?", date_)
        if match:
            # 提取年份、月份和日期
            if match.group(1):  # 匹配到分離的年份、月份、日期
                year = match.group(1)
                month = match.group(2).zfill(2)  # 補零
                day = match.group(3).zfill(2)    # 補零
            else: 
                raise ValueError("無法解析日期格式")
            
            # 將民國年轉為西元年
            if len(year) == 2 and int(year) < 77:
                year = str(int('1' + year) + 1911) # 有人打 12/08/08
            elif len(year) == 2 or len(year) == 3:
                year = str(int(year) + 1911)
            elif len(year) == 4 and current_ROC in year:
                year = str(int(current_ROC) + 1911)  # 有人打 1113 1130
            elif len(year) == 4 and last_ROC in year:
                year = str(int(last_ROC) + 1911)  # 有人打 1113 1130
                
            if int(month) > 12:
                print('month超過合理值: %s' % date)
                return None
            elif month in ['01', '03', '05', '07', '08', '10', '12']:
                if int(day) > 31:
                    print('day超過合理值: %s' % date)
                    return None
            elif month in ['04', '06', '09', '11']:
                if int(day) > 30:
                    print('day超過合理值: %s' % date)
                    return None
            elif month == '02':
                if int(day) > 29:
                    print('day超過合理值: %s' % date)
                    return None
            else:
                print('month不符合合理值: %s' % date)
                return None
        
            return f"{year}{month}{day}"

# ==================================================================================================================
# 處理沒有小數點的數值欄位(目前為每股盈餘以外的數值欄位)
def handle_no_decimal_num(num):
    
    # return None: 為了對應匯入 DB 的 type 要求
    if num == '':
        return None
    
    elif any(keyword in num for keyword in ['不適用', 'NA', '無']):
        return None
    
    num_ = re.sub(r"[（(]{2,}", "(", num)  # 將多餘的 `((` 或 `（（` 替換成單一 `(`
    num_ = re.sub(r"[)）]{2,}", ")", num_)  # 將多餘的 `))` 或 `））` 替換成單一 `)`
    
    # 僅取的元素中的數字,.()（）-
    pattern = r"[\d,.()（）-]"
    match = ''.join(re.findall(pattern, num_))  # 4,775(個別財務報告) -> 4,775()；(39,967)(更正) -> (39,967)()
    
    if match:
        match = re.sub(r'[(（][)）]', '', match)  # 4,775()-> 4,775; (39,967)() -> (39,967)
        match_bracket = re.search(r'(?<!\d)([()（）][\d,.-]+[()（）])', match)  # 匹配有括號的數字，且括號前不可為數字，括號內只能有數字,.-
        if match_bracket:
            match_bracket = next((g for g in match_bracket.groups() if g), None)
            match = match_bracket.replace(',', '').replace('.', '').replace('-', '')
            value = '-' + match[1:-1] 
        elif match[-1] in [')', '）', '.', '-'] and match[0].isdigit():   # 4,596,498) -> 4596498; 5,195,927.- -> 5195927
            value = ''.join(re.findall(r"\d+", match))
        else:
            value = match.replace(',', '').replace('.', '')
        
        try:
            return int(value)  # 轉換成整數
        except ValueError:
            print('ValueError: %s' % num)
            return num  # 若轉換失敗，則返回原始值
    
    else:
        print('沒通過以上判斷: %s' % num)
        return num 

# ==================================================================================================================
# 處理有小數點的數值欄位(目前僅有每股盈餘)
def handle_decimal_num(num):
    
    # return None: 為了對應匯入 DB 的 type 要求
    if num == '':
        return None
    
    elif any(keyword in num for keyword in ['不適用', 'NA', '無']):
        return None
    
    num_ = re.sub(r"[（(]{2,}", "(", num)  # 將多餘的 `((` 或 `（（` 替換成單一 `(`
    num_ = re.sub(r"[)）]{2,}", ")", num_)  # 將多餘的 `))` 或 `））` 替換成單一 `)`
    
    # 僅取的元素中的數字,.()（）-
    pattern = r"[\d,.()（）-]"
    match = ''.join(re.findall(pattern, num_))  # 4,775(個別財務報告) -> 4,775()；(39,967)(更正) -> (39,967)()
    
    if match:
        match = re.sub(r'[(（][)）]', '', match)  # 4,775()-> 4,775; (39,967)() -> (39,967)
        match_bracket = re.search(r'(?<!\d)([()（）][\d,.-]+[()（）])', match)  # 匹配有括號的數字，且括號前不可為數字，括號內只能有數字,.-
        if match_bracket:
            match_bracket = next((g for g in match_bracket.groups() if g), None)
            match = match_bracket.replace(',', '.').replace('-', '')
            value = '-' + match[1:-1]
        elif match[-1] in [')', '）', '.', '-'] and match[0].isdigit():  # 0.57.
            value = match[:-1]
        else:
            value = match.replace(',', '.')
        
        try:
            return float(value)  # 轉換成浮點數
        except ValueError:
            print('ValueError: %s' % num)
            return num  # 若轉換失敗，則返回原始值
    
    else:
        print('沒通過以上判斷: %s' % num)
        return num 

# ==================================================================================================================
def handle_data(extract):
    
    '''
    處理fin_type, 其他應敘明事項, 日期欄位
    '''
    # 處理 fin_type: 截到'合併'->回傳'M'；截到'個別'->回傳'A'
    extract['合併M/個別A'] = extract['截字fin_type'].map({'合併': 'M', '個別': 'A'}).fillna('')
    
    # 刪除其他應敘明事項的空格及換行
    extract['其他應敘明事項'] = extract['其他應敘明事項'].apply(lambda x: x.replace('\n', '').replace('\u3000', '').replace(' ', ''))
    
    '''
    當截出的RMK，符合以下任一條件時，匯入空白：
    1.RMK等於「無」、「無。」、「無.」、「不適用」、「不適用。」、「”不適用”」、「NA」、「N/A」
    2.RMK包含「主管機關規定」、「主管機關規定期限內」、「規定期限內」、「規定時間」、「公開資訊觀測站」、
    「保險業財務報告」、「證券商財務報告編製準則」、「期貨商財務報告編製準則」、「證券交易法施行細則第7條第9款」、
    「更正並不影響損益」、「http://www」、「https://www」、「累計財務數據」，且不包含「每股面額」、「營業收入」、「每股盈餘」、「誤植」
    '''
    # 條件 1：等於特定值
    equal_conditions = ["無", "無。", "無.", "不適用", "不適用。", "”不適用”", "NA", "N/A"]

    # 條件 2：包含特定關鍵字但不包含例外關鍵字
    include_keywords = [
        "主管機關規定", "主管機關規定期限內", "規定期限內", "規定時間",
        "公開資訊觀測站", "保險業財務報告", "證券商財務報告編製準則", "期貨商財務報告編製準則",
        "證券交易法施行細則第7條第9款", "更正並不影響損益", "http://www", "https://www", "累計財務數據"
    ]
    exclude_keywords = ["每股面額", "營業收入", "每股盈餘", "誤植"]

    # 自定義條件函數
    def process_text(text):
        # 檢查是否符合條件 1
        if text in equal_conditions:
            return ''
        # 檢查是否符合條件 2
        if any(keyword in text for keyword in include_keywords) and not any(keyword in text for keyword in exclude_keywords):
            return ''
        return text

    # 應用條件函數
    extract['其他應敘明事項'] = extract['其他應敘明事項'].apply(process_text)
    
    
    # 將日期欄位套用 normalize_date 轉為 YYYYMMDD 格式
    date_col = ['董事會決議日', '審計委員會通過日', '財務起日', '財務迄日']

    extract[date_col] = extract[date_col].map(normalize_date)
    
    '''
    截字成果在有'_TXT'的欄位，先複製到沒'_TXT'的欄位再進行數值資料整理
    '''
    # 找出所有含有 `_TXT` 的欄位
    txt_columns = [col for col in extract.columns if col.endswith('_TXT')]

    # 將 `_TXT` 的值複製到對應的沒有 `_TXT` 的欄位
    # 將沒有`_TXT` 的欄位存到 no_txt_columns
    no_txt_columns = []
    for txt_col in txt_columns:
        no_txt_col = txt_col.replace('_TXT', '')  # 找到對應的欄位名稱
        if no_txt_col in extract.columns:  # 確保沒有 `_TXT` 的欄位存在
            extract[no_txt_col] = extract[txt_col]
            no_txt_columns.append(no_txt_col)
            
    '''
    整理出 decimal_col 和 no_decimal_col 方便對於有無小數點的數值資料分別整理
    '''
    decimal_col = []

    for idx in range(len(pm_data)):
        substring_beg = pm_data['substring_beg'][idx]
        col = pm_data['col_cname'][idx]
        if '(元)' in substring_beg:
            decimal_col.append(col.replace('_TXT', ''))
            
    no_decimal_col = [col for col in no_txt_columns if col not in decimal_col]
    
    extract[no_decimal_col] = extract[no_decimal_col].map(handle_no_decimal_num)
    extract[decimal_col] = extract[decimal_col].map(handle_decimal_num) 
    return extract   

# ==================================================================================================================
'''
【衍生規則: 抓取fin_type】
BYM = estm_u11_board.FIN_END_DATE(迄日)的年月
FMM = fin_fininfo_m.QUARTER *3 
FYM = FIN_YAER + FMM  (年季轉年月) 
取estm_u11_board.COMP_ID 和 basic.attr_stdid.COMP_ID 配, 得TEJ_COMP_ID 
再用TEJ_COMP_ID 、BYM和fin_fininfo_m.TEJ_COMP_ID、FYM且fininfo_m = A配, 
得fin_fininfo_m.FIN_YEAR、QUARTER、FIN_TYPE， 限取 BYM-1年 <= FYM  <BYM 最大年月的FIN_TYPE。
'''

def fill_empty_fin_type(extract):
    
    stdid_df = get_stdid_data()
    fininfo_df = get_fininfo_data()
    
    for idx in range(len(extract)):
        fin_type = extract['合併M/個別A'][idx]
        comp_id = extract['公司碼'][idx]
        fin_end_date = extract['財務迄日'][idx]
        if pd.isna(fin_end_date) or fin_type:
            continue
        
        BYM = int(fin_end_date[:6])  # BYM: 財務迄日年季
        tej_comp_id = stdid_df[stdid_df['comp_id'] == str(comp_id)]['tej_comp_id'].iloc[0]
        
        fin_type_df = fininfo_df[(fininfo_df['tej_comp_id'] == tej_comp_id) & (fininfo_df['fin_aq'] == 'A')]
        FY = fin_type_df['fin_year'].to_list()
        FY = [dt.strftime('%Y') for dt in FY]  # '2024'
        FQ = fin_type_df['quarter'].to_list()  # 9
        FM = [str(quarter*3).zfill(2) for quarter in FQ]  # '09'
        fin_type_list = fin_type_df['fin_type'].to_list()
        FYM_list = [int(FY[idx] + FM[idx]) for idx in range(len(FY))]  # 202409

        pass_FYM = []
        for FYM in FYM_list:
            if (BYM-100 <= FYM < BYM):  # 取 BYM-1年 <= FYM < BYM 最大年月的 FIN_TYPE
                pass_FYM.append(FYM)
                
        fin_type_add = fin_type_list[FYM_list.index(max(pass_FYM))]

        extract.loc[idx, '合併M/個別A'] = fin_type_add

    return extract