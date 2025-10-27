import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import printing_scripts.date_fn as date_fn


def get_fx_rate(ccy, date):
    try:
        df = blp.bdh(f"{ccy} curncy", 'PX_LAST', date, date)
        return df.iloc[0, 0] if not df.empty else None
    except:
        return None

def transpose(list_of_lists):
    if not list_of_lists:
        return []
    return [[row[i] for row in list_of_lists] for i in range(len(list_of_lists[0]))]

def get_dates():
    # Today's date
    today = datetime.now().strftime("%y%m%d")
    
    # Latest curve file date from aud_curve folder
    pattern = os.path.join("..", "..", "aud_curves", "*_aud_curve.json")
    files = glob.glob(pattern)
    
    if files:
        # Extract dates and find latest
        dates = []
        for file in files:
            filename = os.path.basename(file)
            match = re.match(r'(\d{6})_aud_curve\.json', filename)
            if match:
                dates.append(match.group(1))
        
        if dates:
            latest = max(dates)  # Gets latest date
            latest_formatted = f"{latest[:2]} {latest[2:4]} {latest[4:6]}"

    
    return today, latest

def aud_curve_serialiser(date):
    date = str(date)
    settle_date = xc.DateAdd(date, "2b", "syb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')

    fx_rate = str(get_fx_rate("audusd", date))

    aud_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "AUD"],
        ["Funding Currency", "USD"],
        ["Funding Discount Index", "SOFR"],
        ["Calendar", "SYB"],
        ["Base rate", "3m"],
        ["Build Type", "BUILD SINGLE CCY"],
        ["Discount Index", "3m"],
        ["Bump Zeros", "following"],
        ["Bump Swaps", "MODFOL"],
        ["Left Extrap", "Linear"],
        ["Right Extrap", "Linear"],
        ["Fast Rebuild", "TRUE"],
        ["FX spot", fx_rate],
        ["Curve bundle", "AUDUSD_BUNDLE"]
    ]
    securities = [
        # IRS Securities
        "ADSWAP1Q curncy", "ADSWAP2Q curncy", "ADSWAP3Q curncy",
        "ADSWAP4 curncy", "ADSWAP5 curncy", "ADSWAP6 curncy", "ADSWAP7 curncy", 
        "ADSWAP8 curncy", "ADSWAP9 curncy", "ADSWAP10 curncy", "ADSWAP12 curncy", 
        "ADSWAP15 curncy", "ADSWAP20 curncy", "ADSWAP25 curncy", "ADSWAP30 curncy",
        
        # 6x3 Basis Securities
        "ADBBCF1 curncy", "ADBBCF2 curncy", "ADBBCF3 curncy", "ADBBCF4 curncy", 
        "ADBBCF5 curncy", "ADBBCF6 curncy", "ADBBCF7 curncy", "ADBBCF8 curncy", 
        "ADBBCF9 curncy", "ADBBCF10 curncy", "ADBBCF12 curncy", "ADBBCF15 curncy", 
        "ADBBCF20 curncy", "ADBBCF25 curncy", "ADBBCF30 curncy",
        
        # BOB Securities
        "ADBBCO1 curncy", 
        "ADBBCO2 curncy", "ADBBCO3 curncy", "ADBBCO4 curncy", "ADBBCO5 curncy", 
        "ADBBCO6 curncy", "ADBBCO7 curncy", "ADBBCO8 curncy", "ADBBCO9 curncy", 
        "ADBBCO10 curncy", "ADBBCO12 curncy", "ADBBCO15 curncy", "ADBBCO20 curncy", 
        "ADBBCO25 curncy", "ADBBCO30 curncy",
        
        # Cross Currency Securities
        "ADOIQQ1 curncy", "ADOIQQ2 curncy", "ADOIQQ3 curncy", 
        "ADOIQQ4 curncy", "ADOIQQ5 curncy", "ADOIQQ6 curncy", 
        "ADOIQQ7 curncy", "ADOIQQ8 curncy", "ADOIQQ9 curncy", 
        "ADOIQQ10 curncy", "ADOIQQ12 curncy", "ADOIQQ15 curncy", 
        "ADOIQQ20 curncy", "ADOIQQ25 curncy", "ADOIQQ30 curncy"
    ]
    
    aud_bbg_data = blp.bdh(securities, ["MID"], date, date)

    
    def get_aud_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = aud_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"

    # Date to use for pricing 
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()

    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_ibor_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    basis_ois_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]

    # Build outright swaps data with dynamic include flags
    outright_swaps_data = []
    swap_instruments = [
        ("AUDIRS-QQ", "1y", "ADSWAP1Q curncy"),
        ("AUDIRS-QQ", "2y", "ADSWAP2Q curncy"),
        ("AUDIRS-QQ", "3y", "ADSWAP3Q curncy"),
        ("AUDIRS-SS", "4y", "ADSWAP4 curncy"),
        ("AUDIRS-SS", "5y", "ADSWAP5 curncy"),
        ("AUDIRS-SS", "6y", "ADSWAP6 curncy"),
        ("AUDIRS-SS", "7y", "ADSWAP7 curncy"),
        ("AUDIRS-SS", "8y", "ADSWAP8 curncy"),
        ("AUDIRS-SS", "9y", "ADSWAP9 curncy"),
        ("AUDIRS-SS", "10y", "ADSWAP10 curncy"),
        ("AUDIRS-SS", "12y", "ADSWAP12 curncy"),
        ("AUDIRS-SS", "15y", "ADSWAP15 curncy"),
        ("AUDIRS-SS", "20y", "ADSWAP20 curncy"),
        ("AUDIRS-SS", "25y", "ADSWAP25 curncy"),
        ("AUDIRS-SS", "30y", "ADSWAP30 curncy")
    ]
    
    for template, tenor, security in swap_instruments:
        price, include_flag = get_aud_price_and_include(security, pricing_date, 100)
        outright_swaps_data.append([template, settle_date, tenor, price, include_flag, security])
    # Build basis_ibor_data with dynamic include flags
    basis_ibor_data = []
    ibor_instruments = [
        ("1y", "ADBBCF1 curncy"), ("2y", "ADBBCF2 curncy"), ("3y", "ADBBCF3 curncy"),
        ("4y", "ADBBCF4 curncy"), ("5y", "ADBBCF5 curncy"), ("6y", "ADBBCF6 curncy"),
        ("7y", "ADBBCF7 curncy"), ("8y", "ADBBCF8 curncy"), ("9y", "ADBBCF9 curncy"),
        ("10y", "ADBBCF10 curncy"), ("12y", "ADBBCF12 curncy"), ("15y", "ADBBCF15 curncy"),
        ("20y", "ADBBCF20 curncy"), ("25y", "ADBBCF25 curncy"), ("30y", "ADBBCF30 curncy")
    ]
    
    for tenor, security in ibor_instruments:
        price, include_flag = get_aud_price_and_include(security, pricing_date, 10000)
        basis_ibor_data.append(["AUDBASIS-6X3", settle_date, tenor, price, include_flag, security])
    
    # Build basis_ois_data with dynamic include flags
    basis_ois_data = []
    ois_instruments = [
        ("1y", "ADBBCO1 curncy"), ("2y", "ADBBCO2 curncy"), ("3y", "ADBBCO3 curncy"),
        ("4y", "ADBBCO4 curncy"), ("5y", "ADBBCO5 curncy"), ("6y", "ADBBCO6 curncy"),
        ("7y", "ADBBCO7 curncy"), ("8y", "ADBBCO8 curncy"), ("9y", "ADBBCO9 curncy"),
        ("10y", "ADBBCO10 curncy"), ("12y", "ADBBCO12 curncy"), ("15y", "ADBBCO15 curncy"),
        ("20y", "ADBBCO20 curncy"), ("25y", "ADBBCO25 curncy"), ("30y", "ADBBCO30 curncy")
    ]
    
    for tenor, security in ois_instruments:
        price, include_flag = get_aud_price_and_include(security, pricing_date, 10000)
        basis_ois_data.append(["AUDBOB-3M", settle_date, tenor, price, include_flag, security])
    
    # Build basis_xccy_data with dynamic include flags
    basis_xccy_data = []
    xccy_instruments = [
        ("1y", "ADOIQQ1 curncy"), ("2y", "ADOIQQ2 curncy"), ("3y", "ADOIQQ3 curncy"),
        ("4y", "ADOIQQ4 curncy"), ("5y", "ADOIQQ5 curncy"), ("6y", "ADOIQQ6 curncy"),
        ("7y", "ADOIQQ7 curncy"), ("8y", "ADOIQQ8 curncy"), ("9y", "ADOIQQ9 curncy"),
        ("10y", "ADOIQQ10 curncy"), ("12y", "ADOIQQ12 curncy"), ("15y", "ADOIQQ15 curncy"),
        ("20y", "ADOIQQ20 curncy"), ("25y", "ADOIQQ25 curncy"), ("30y", "ADOIQQ30 curncy")
    ]
    
    for tenor, security in xccy_instruments:
        price, include_flag = get_aud_price_and_include(security, pricing_date, 10000)
        basis_xccy_data.append(["aonia-sofr", settle_date, tenor, price, include_flag, security])

    outright_swaps_data = transpose(outright_swaps_data)
    basis_ibor_data = transpose(basis_ibor_data)
    basis_ois_data = transpose(basis_ois_data)
    basis_xccy_data = transpose(basis_xccy_data)

    curve_name = "aud.primary"
    aud_curve = xc.BuildCurves(curve_name, aud_config,"outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-ibor", basis_ibor_headers, basis_ibor_data, "basis-ois", basis_ois_headers,
                               basis_ois_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)

    return aud_curve

def usd_curve_serialiser(date):

    settle_date = xc.DateAdd(date, "2b", "nyc")

    excel_date = settle_date
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    # Convert to YYYY-MM-DD string
    settle_date = pd.to_datetime(excel_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')

    usd_sofr_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "USD"],
        ["Calendar", "NYC"],
        ["Build Type", "build simple"],
        ["Base Rate", "SOFR"],
        ["Discount Index", "SOFR"],
        ["Bump Zeros", "following"],
        ["Bump Swaps", "MODFOL"],
        ["Left Extrap", "Parabolic"],
        ["Right Extrap", "Parabolic"],
        ["Fast Rebuild", "TRUE"],
        ["Spread Interp", "Parabolic"]
    ]
    
    # Extract all the USD SOFR securities we need
    usd_securities = [
        # Monthly tenors
        "USOSFRA curncy", "USOSFRB curncy", "USOSFRC curncy", "USOSFRD curncy", 
        "USOSFRE curncy", "USOSFRF curncy", "USOSFRG curncy", "USOSFRH curncy", 
        "USOSFRI curncy", "USOSFRJ curncy", "USOSFRK curncy", "USOSFR1C curncy", 
        "USOSFR1F curncy", "USOSFR1I curncy",
        
        # Yearly tenors
        "USOSFR1 curncy", "USOSFR2 curncy", "USOSFR3 curncy", "USOSFR4 curncy", 
        "USOSFR5 curncy", "USOSFR6 curncy", "USOSFR7 curncy", "USOSFR8 curncy", 
        "USOSFR9 curncy", "USOSFR10 curncy", "USOSFR12 curncy", "USOSFR15 curncy", 
        "USOSFR20 curncy", "USOSFR25 curncy", "USOSFR30 curncy"
    ]
   
    # Get Bloomberg data for all USD SOFR securities (use original date format for Bloomberg)
    usd_bbg_data = blp.bdh(usd_securities, ["MID"], date, date)

    def get_usd_price(security, pricing_date, scale_factor=100):
        try:
            price = usd_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor)
        except (KeyError, IndexError):
            return "0.0"

    usd_outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    
    ## USD SOFR Outright Swaps Data (divide by 100)
    usd_outright_swaps_data = [
        # Monthly tenors (Include = 0)
        ["USDSOFR", settle_date, "1m", get_usd_price("USOSFRA curncy", pricing_date), "1", "USOSFRA curncy"],
        ["USDSOFR", settle_date, "2m", get_usd_price("USOSFRB curncy", pricing_date), "1", "USOSFRB curncy"],
        ["USDSOFR", settle_date, "3m", get_usd_price("USOSFRC curncy", pricing_date), "1", "USOSFRC curncy"],
        ["USDSOFR", settle_date, "4m", get_usd_price("USOSFRD curncy", pricing_date), "1", "USOSFRD curncy"],
        ["USDSOFR", settle_date, "5m", get_usd_price("USOSFRE curncy", pricing_date), "1", "USOSFRE curncy"],
        ["USDSOFR", settle_date, "6m", get_usd_price("USOSFRF curncy", pricing_date), "1", "USOSFRF curncy"],
        ["USDSOFR", settle_date, "7m", get_usd_price("USOSFRG curncy", pricing_date), "1", "USOSFRG curncy"],
        ["USDSOFR", settle_date, "8m", get_usd_price("USOSFRH curncy", pricing_date), "1", "USOSFRH curncy"],
        ["USDSOFR", settle_date, "9m", get_usd_price("USOSFRI curncy", pricing_date), "1", "USOSFRI curncy"],
        ["USDSOFR", settle_date, "10m", get_usd_price("USOSFRJ curncy", pricing_date), "1", "USOSFRJ curncy"],
        ["USDSOFR", settle_date, "11m", get_usd_price("USOSFRK curncy", pricing_date), "1", "USOSFRK curncy"],
        ["USDSOFR", settle_date, "15m", get_usd_price("USOSFR1C curncy", pricing_date), "1", "USOSFR1C curncy"],
        ["USDSOFR", settle_date, "18m", get_usd_price("USOSFR1F curncy", pricing_date), "1", "USOSFR1F curncy"],
        ["USDSOFR", settle_date, "21m", get_usd_price("USOSFR1I curncy", pricing_date), "1", "USOSFR1I curncy"],
        
        # Yearly tenors (Include = 1)
        ["USDSOFR", settle_date, "1y", get_usd_price("USOSFR1 curncy", pricing_date), "1", "USOSFR1 curncy"],
        ["USDSOFR", settle_date, "2y", get_usd_price("USOSFR2 curncy", pricing_date), "1", "USOSFR2 curncy"],
        ["USDSOFR", settle_date, "3y", get_usd_price("USOSFR3 curncy", pricing_date), "1", "USOSFR3 curncy"],
        ["USDSOFR", settle_date, "4y", get_usd_price("USOSFR4 curncy", pricing_date), "1", "USOSFR4 curncy"],
        ["USDSOFR", settle_date, "5y", get_usd_price("USOSFR5 curncy", pricing_date), "1", "USOSFR5 curncy"],
        ["USDSOFR", settle_date, "6y", get_usd_price("USOSFR6 curncy", pricing_date), "1", "USOSFR6 curncy"],
        ["USDSOFR", settle_date, "7y", get_usd_price("USOSFR7 curncy", pricing_date), "1", "USOSFR7 curncy"],
        ["USDSOFR", settle_date, "8y", get_usd_price("USOSFR8 curncy", pricing_date), "1", "USOSFR8 curncy"],
        ["USDSOFR", settle_date, "9y", get_usd_price("USOSFR9 curncy", pricing_date), "1", "USOSFR9 curncy"],
        ["USDSOFR", settle_date, "10y", get_usd_price("USOSFR10 curncy", pricing_date), "1", "USOSFR10 curncy"],
        ["USDSOFR", settle_date, "12y", get_usd_price("USOSFR12 curncy", pricing_date), "1", "USOSFR12 curncy"],
        ["USDSOFR", settle_date, "15y", get_usd_price("USOSFR15 curncy", pricing_date), "1", "USOSFR15 curncy"],
        ["USDSOFR", settle_date, "20y", get_usd_price("USOSFR20 curncy", pricing_date), "1", "USOSFR20 curncy"],
        ["USDSOFR", settle_date, "25y", get_usd_price("USOSFR25 curncy", pricing_date), "1", "USOSFR25 curncy"],
        ["USDSOFR", settle_date, "30y", get_usd_price("USOSFR30 curncy", pricing_date), "1", "USOSFR30 curncy"]
    ]
    
    curve_name = "usd.sofr.primary"
    usd_outright_swaps_data = np.array(usd_outright_swaps_data).T.tolist()
    
    # Build USD SOFR curve with only outright swaps
    usd_curve = xc.BuildCurves(curve_name, usd_sofr_config, "outright-swaps", usd_outright_swaps_headers, usd_outright_swaps_data)
    
    return usd_curve

def yyyy_mm_dd_to_yymmdd(date_string):
    # Parse YYYY-MM-DD format
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    # Format as YYMMDD
    return date_obj.strftime('%y%m%d')

if __name__ == "__main__":
    dates = date_fn.get_dates("syb", "aud")
    for date in dates:
        usd_curve = usd_curve_serialiser(date)
        fx_rate = str(get_fx_rate("audusd", date))
        aud_usd_curve = xc.BuildBlockBundle("AUDUSD_BUNDLE", [["USD","usd.sofr.primary"]]  , [["AUDUSD", fx_rate]] )
        aud_curve = aud_curve_serialiser(date)
        date = yyyy_mm_dd_to_yymmdd(date)
        name = date + "_aud_curve.json"
        xc.Serialise("aud.primary", os.path.join("..", "..", "aud_curves", name), True)
