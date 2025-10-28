import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))
import date_fn

def cad_curve_serialiser(date):
    settle_date = xc.DateAdd(date, "1b", "trb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    fx_rate = str(1/date_fn.get_fx_rate("usdcad", date))  # Inverted for CAD
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    cad_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "CAD"],
        ["Funding Currency", "USD"],
        ["Curve Bundle", "USDCAD_BUNDLE"],
        ["Calendar", "TRB"],
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
    
    cad_securities = [
        "CDSO1 CURNCY", "CDSO2 CURNCY", "CDSO3 CURNCY", "CDSO4 CURNCY",
        "CDSO5 CURNCY", "CDSO6 CURNCY", "CDSO7 CURNCY", "CDSO8 CURNCY",
        "CDSO9 CURNCY", "CDSO10 CURNCY", "CDSO12 CURNCY", "CDSO15 CURNCY",
        "CDSO20 CURNCY", "CDSO30 CURNCY",
        # Cross currency
        "CDXOQQ1 CURNCY", "CDXOQQ1F CURNCY", "CDXOQQ2 CURNCY", "CDXOQQ3 CURNCY",
        "CDXOQQ4 CURNCY", "CDXOQQ5 CURNCY", "CDXOQQ6 CURNCY", "CDXOQQ7 CURNCY",
        "CDXOQQ8 CURNCY", "CDXOQQ9 CURNCY", "CDXOQQ10 CURNCY", "CDXOQQ12 CURNCY",
        "CDXOQQ15 CURNCY", "CDXOQQ20 CURNCY", "CDXOQQ25 CURNCY", "CDXOQQ30 CURNCY"
    ]
    
    cad_bbg_data = blp.bdh(cad_securities, ["MID"], date, date)
    
    def get_cad_price(security, pricing_date, scale_factor=100):
        try:
            price = cad_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor)
        except (KeyError, IndexError):
            return "0.0"
    
    def get_cad_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = cad_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"
    
    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    
    # Build outright swaps data with dynamic include flags
    outright_swaps_data = []
    swap_instruments = [
        ("CADOIS", "1y", "CDSO1 CURNCY"),
        ("CADOIS-SS", "2y", "CDSO2 CURNCY"), ("CADOIS-SS", "3y", "CDSO3 CURNCY"),
        ("CADOIS-SS", "4y", "CDSO4 CURNCY"), ("CADOIS-SS", "5y", "CDSO5 CURNCY"),
        ("CADOIS-SS", "6y", "CDSO6 CURNCY"), ("CADOIS-SS", "7y", "CDSO7 CURNCY"),
        ("CADOIS-SS", "8y", "CDSO8 CURNCY"), ("CADOIS-SS", "9y", "CDSO9 CURNCY"),
        ("CADOIS-SS", "10y", "CDSO10 CURNCY"), ("CADOIS-SS", "12y", "CDSO12 CURNCY"),
        ("CADOIS-SS", "15y", "CDSO15 CURNCY"), ("CADOIS-SS", "20y", "CDSO20 CURNCY"),
        ("CADOIS-SS", "30y", "CDSO30 CURNCY")
    ]
    
    for template, tenor, security in swap_instruments:
        price, include_flag = get_cad_price_and_include(security, pricing_date, 100)
        outright_swaps_data.append([template, settle_date, tenor, price, include_flag, security])
    
    # Build basis_xccy_data with dynamic include flags
    basis_xccy_data = []
    xccy_instruments = [
        ("1Y", "CDXOQQ1 CURNCY"), ("2Y", "CDXOQQ2 CURNCY"), ("3Y", "CDXOQQ3 CURNCY"),
        ("4Y", "CDXOQQ4 CURNCY"), ("5Y", "CDXOQQ5 CURNCY"), ("6Y", "CDXOQQ6 CURNCY"),
        ("7Y", "CDXOQQ7 CURNCY"), ("8Y", "CDXOQQ8 CURNCY"), ("9Y", "CDXOQQ9 CURNCY"),
        ("10Y", "CDXOQQ10 CURNCY"), ("12Y", "CDXOQQ12 CURNCY"), ("15Y", "CDXOQQ15 CURNCY"),
        ("20Y", "CDXOQQ20 CURNCY"), ("25Y", "CDXOQQ25 CURNCY"), ("30Y", "CDXOQQ30 CURNCY")
    ]
    
    for tenor, security in xccy_instruments:
        price, include_flag = get_cad_price_and_include(security, pricing_date, 10000)
        basis_xccy_data.append(["CORRA-SOFR", settle_date, tenor, price, include_flag, security])
    
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    curve_name = "cad.curve.primary"
    cad_curve = xc.BuildCurves(curve_name, cad_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return cad_curve
