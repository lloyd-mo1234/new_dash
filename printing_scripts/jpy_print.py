import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import date_fn

def jpy_curve_serialiser(date):
    settle_date = xc.DateAdd(date, "2b", "tkb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    fx_rate = str(1/date_fn.get_fx_rate("usdjpy", date))  # Inverted for JPY
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    jpy_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "JPY"],
        ["Funding Currency", "USD"],
        ["Curve Bundle", "USDJPY_BUNDLE"],
        ["Calendar", "TKB"],
        ["Build Type", "BUILD SINGLE CCY"],
        ["Base Rate", "OIS"],
        ["Discount Index", "OIS"],
        ["Funding Discount Index", "SOFR"],
        ["Bump Zeros", "following"],
        ["Bump Swaps", "MODFOL"],
        ["Left Extrap", "Linear"],
        ["Right Extrap", "Linear"],
        ["fx spot", fx_rate],
        ["Fast Rebuild", "TRUE"]
    ]
    
    jpy_securities = [
        "JYSOC CURNCY", "JYSOF CURNCY", "JYSOI CURNCY", "JYSO1 CURNCY",
        "JYSO1F CURNCY", "JYSO2 CURNCY", "JYSO3 CURNCY", "JYSO4 CURNCY",
        "JYSO5 CURNCY", "JYSO6 CURNCY", "JYSO7 CURNCY", "JYSO8 CURNCY",
        "JYSO9 CURNCY", "JYSO10 CURNCY", "JYSO12 CURNCY", "JYSO15 CURNCY",
        "JYSO20 CURNCY", "JYSO25 CURNCY", "JYSO30 CURNCY",
        # Cross currency
        "JYBSS12M CURNCY", "JYBSS2Y CURNCY", "JYBSS3Y CURNCY", "JYBSS4Y CURNCY",
        "JYBSS5Y CURNCY", "JYBSS6Y CURNCY", "JYBSS7Y CURNCY", "JYBSS8Y CURNCY",
        "JYBSS9Y CURNCY", "JYBSS10Y CURNCY", "JYBSS12Y CURNCY", "JYBSS15Y CURNCY",
        "JYBSS20Y CURNCY", "JYBSS25Y CURNCY", "JYBSS30Y CURNCY"
    ]
    
    jpy_bbg_data = blp.bdh(jpy_securities, ["MID"], date, date)
    
    def get_jpy_price(security, pricing_date, scale_factor=100):
        try:
            price = jpy_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor)
        except (KeyError, IndexError):
            return "0.0"
    
    def get_jpy_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = jpy_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"
    
    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    
    # Build outright swaps data with dynamic include flags
    outright_swaps_data = []
    swap_instruments = [
        ("3m", "JYSOC CURNCY"), ("6m", "JYSOF CURNCY"), ("9m", "JYSOI CURNCY"),
        ("1y", "JYSO1 CURNCY"), ("18m", "JYSO1F CURNCY"), ("2y", "JYSO2 CURNCY"),
        ("3y", "JYSO3 CURNCY"), ("4y", "JYSO4 CURNCY"), ("5y", "JYSO5 CURNCY"),
        ("6y", "JYSO6 CURNCY"), ("7y", "JYSO7 CURNCY"), ("8y", "JYSO8 CURNCY"),
        ("9y", "JYSO9 CURNCY"), ("10y", "JYSO10 CURNCY"), ("12y", "JYSO12 CURNCY"),
        ("15y", "JYSO15 CURNCY"), ("20y", "JYSO20 CURNCY"), ("25y", "JYSO25 CURNCY"),
        ("30y", "JYSO30 CURNCY")
    ]
    
    for tenor, security in swap_instruments:
        price, include_flag = get_jpy_price_and_include(security, pricing_date, 100)
        outright_swaps_data.append(["JPYOIS", settle_date, tenor, price, include_flag, security])
    
    # Build basis_xccy_data with dynamic include flags
    basis_xccy_data = []
    xccy_instruments = [
        ("1y", "JYBSS12M CURNCY"), ("2y", "JYBSS2Y CURNCY"), ("3y", "JYBSS3Y CURNCY"),
        ("4y", "JYBSS4Y CURNCY"), ("5y", "JYBSS5Y CURNCY"), ("6y", "JYBSS6Y CURNCY"),
        ("7y", "JYBSS7Y CURNCY"), ("8y", "JYBSS8Y CURNCY"), ("9y", "JYBSS9Y CURNCY"),
        ("10y", "JYBSS10Y CURNCY"), ("12y", "JYBSS12Y CURNCY"), ("15y", "JYBSS15Y CURNCY"),
        ("20y", "JYBSS20Y CURNCY"), ("25y", "JYBSS25Y CURNCY"), ("30y", "JYBSS30Y CURNCY")
    ]
    
    for tenor, security in xccy_instruments:
        price, include_flag = get_jpy_price_and_include(security, pricing_date, 10000)
        basis_xccy_data.append(["TONAR-SOFR", settle_date, tenor, price, include_flag, security])
    
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    curve_name = "jpy.tonar.primary"
    jpy_curve = xc.BuildCurves(curve_name, jpy_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return jpy_curve
