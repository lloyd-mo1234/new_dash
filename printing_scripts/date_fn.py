import glob
from datetime import datetime, timedelta
import cba.analytics.xcurves as xc
import xbbg.blp as blp
import os

def get_most_recent_date_file(ccy):
    ccy = ccy.lower()
    path = "C:\\BAppGeneral\\chart_app\\" + ccy + "_curves\\??????*"    
    files = glob.glob(path)
    
    if not files:
        return None, datetime.now().strftime("%Y-%m-%d")
    
    # Extract dates and find most recent (excluding 99xxxx dates which are from 1999)
    date_files = []
    for file in files:
        filename = os.path.basename(file)
        if len(filename) >= 6 and filename[:6].isdigit():
            # Skip files starting with "99" (1999 dates)
            if not filename.startswith('9'):
                date_files.append((filename[:6], file))
    
    if not date_files:
        print("caught")
        return None, datetime.now().strftime("%Y-%m-%d")
    
    most_recent = max(date_files, key=lambda x: x[0])
    most_recent_dt = yymmdd_to_yyyy_mm_dd(most_recent[0])
    today = datetime.now().strftime("%Y-%m-%d")
    
    return most_recent_dt, today

def yymmdd_to_yyyy_mm_dd(date_string):
    # Parse 6-digit date (YYMMDD)
    date_obj = datetime.strptime(date_string, '%y%m%d')
    # Format as YYYY-MM-DD
    return date_obj.strftime('%Y-%m-%d')

def excel_serial_to_date_string(serial_number):
    # Excel epoch starts from December 30, 1899 (accounts for Excel's leap year bug)
    excel_epoch = datetime(1899, 12, 30)
    date_obj = excel_epoch + timedelta(days=serial_number)
    return date_obj.strftime('%Y-%m-%d')

def yymmdd_to_excel_serial(date_string):
    # Parse 6-digit date (YYMMDD)
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    
    # Excel epoch (accounting for Excel's leap year bug)
    excel_epoch = datetime(1899, 12, 30)
    
    # Calculate difference in days
    delta = date_obj - excel_epoch
    return delta.days

def get_dates(cal,ccy):
    dts = []

    dates1 = get_most_recent_date_file(ccy)

    last_dt = dates1[0]
    current_dt = dates1[1]

    print(dates1)

    new_date = xc.DateAdd(last_dt, "1b", cal)
    #new_date is currently in excel serial

    current_dt = yymmdd_to_excel_serial(current_dt)

    while new_date < current_dt:
        dts.append(excel_serial_to_date_string(new_date))
        new_date = xc.DateAdd(excel_serial_to_date_string(new_date), "1b", cal)
    
    return dts            
    
        #keep calling xcdateadd and append the new date to dts in the format 

def transpose(list_of_lists):
    if not list_of_lists:
        return []
    return [[row[i] for row in list_of_lists] for i in range(len(list_of_lists[0]))]

def get_fx_rate(ccy, date):
    try:
        df = blp.bdh(f"{ccy} Curncy", 'PX_LAST', date, date)
        return df.iloc[0, 0] if not df.empty else None
    except:
        return None
