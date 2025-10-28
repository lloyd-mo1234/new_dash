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


def nzd_curve_serialiser(date):
    settle_date = xc.DateAdd(date, "2b", "aub,web")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    fx_rate = str(date_fn.get_fx_rate("nzdusd", date))
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    nzd_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "NZD"],
        ["Funding Currency", "USD"],
        ["Curve Bundle", "NZDUSD_BUNDLE"],
        ["Calendar", "AUB,WEB"],
        ["Build Type", "BUILD SINGLE CCY"],
        ["Base Rate", "3M"],
        ["Discount Index", "OIS"],
        ["Funding Discount Index", "SOFR"],
        ["Bump Zeros", "following"],
        ["Bump Swaps", "MODFOL"],
        ["Left Extrap", "Linear"],
        ["Right Extrap", "Linear"],
        ["fx spot", fx_rate],
        ["Fast Rebuild", "TRUE"]
    ]
    
    nzd_securities = [
        "NDSWAP1 CURNCY", "NDSWAP2 CURNCY", "NDSWAP3 CURNCY", "NDSWAP4 CURNCY",
        "NDSWAP5 CURNCY", "NDSWAP6 CURNCY", "NDSWAP7 CURNCY", "NDSWAP8 CURNCY",
        "NDSWAP9 CURNCY", "NDSWAP10 CURNCY", "NDSWAP12 CURNCY", "NDSWAP15 CURNCY",
        "NDSWAP20 CURNCY", "NDSWAP25 CURNCY", "NDSWAP30 CURNCY",
        # Cross currency
        "NDBSQQ1 CURNCY", "NDBSQQ2 CURNCY", "NDBSQQ3 CURNCY", "NDBSQQ4 CURNCY",
        "NDBSQQ5 CURNCY", "NDBSQQ6 CURNCY", "NDBSQQ7 CURNCY", "NDBSQQ8 CURNCY",
        "NDBSQQ9 CURNCY", "NDBSQQ10 CURNCY", "NDBSQQ12 CURNCY", "NDBSQQ15 CURNCY",
        "NDBSQQ20 CURNCY"
    ]
    
    nzd_bbg_data = blp.bdh(nzd_securities, ["MID"], date, date)
    
    def get_nzd_price(security, pricing_date, scale_factor=100):
        try:
            price = nzd_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor)
        except (KeyError, IndexError):
            return "0.0"
    
    def get_nzd_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = nzd_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"
    
    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    
    # Build outright swaps data with dynamic include flags
    outright_swaps_data = []
    swap_instruments = [
        ("1y", "NDSWAP1 CURNCY"), ("2y", "NDSWAP2 CURNCY"), ("3y", "NDSWAP3 CURNCY"),
        ("4y", "NDSWAP4 CURNCY"), ("5y", "NDSWAP5 CURNCY"), ("6y", "NDSWAP6 CURNCY"),
        ("7y", "NDSWAP7 CURNCY"), ("8y", "NDSWAP8 CURNCY"), ("9y", "NDSWAP9 CURNCY"),
        ("10y", "NDSWAP10 CURNCY"), ("12y", "NDSWAP12 CURNCY"), ("15y", "NDSWAP15 CURNCY"),
        ("20y", "NDSWAP20 CURNCY"), ("25y", "NDSWAP25 CURNCY"), ("30y", "NDSWAP30 CURNCY")
    ]
    
    for tenor, security in swap_instruments:
        price, include_flag = get_nzd_price_and_include(security, pricing_date, 100)
        outright_swaps_data.append(["NZDIRS-SQ", settle_date, tenor, price, include_flag, security])
    
    # Build basis_xccy_data with dynamic include flags
    basis_xccy_data = []
    xccy_instruments = [
        ("1y", "NDBSQQ1 CURNCY"), ("2y", "NDBSQQ2 CURNCY"), ("3y", "NDBSQQ3 CURNCY"),
        ("4y", "NDBSQQ4 CURNCY"), ("5y", "NDBSQQ5 CURNCY"), ("6y", "NDBSQQ6 CURNCY"),
        ("7y", "NDBSQQ7 CURNCY"), ("8y", "NDBSQQ8 CURNCY"), ("9y", "NDBSQQ9 CURNCY"),
        ("10y", "NDBSQQ10 CURNCY"), ("12y", "NDBSQQ12 CURNCY"), ("15y", "NDBSQQ15 CURNCY"),
        ("20y", "NDBSQQ20 CURNCY")
    ]
    
    for tenor, security in xccy_instruments:
        price, include_flag = get_nzd_price_and_include(security, pricing_date, 10000)
        basis_xccy_data.append(["BKBM-SOFR", settle_date, tenor, price, include_flag, security])

    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    curve_name = "nzd.bkbm.primary"
    nzd_curve = xc.BuildCurves(curve_name, nzd_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return nzd_curve
