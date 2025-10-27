import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import date_fn

def eur_curve_serialiser(date):
    settle_date = xc.DateAdd(date, "2b", "tgt")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    fx_rate = str(date_fn.get_fx_rate("eurusd", date))
    # Keep the original date string for DataFrame indexing
    pricing_date_str = date
    # Create date object only if needed for other operations
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    eur_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "EUR"],
        ["Funding Currency", "USD"],
        ["Curve Bundle", "EURUSD_BUNDLE"],
        ["Calendar", "TGT"],
        ["Build Type", "BUILD SINGLE CCY"],
        ["Base Rate", "3M"],
        ["Discount Index", "ESTR"],
        ["Funding Discount Index", "SOFR"],
        ["Bump Zeros", "FOLLOWING"],
        ["Bump Swaps", "MODFOL"],
        ["Left Extrap", "Linear"],
        ["Right Extrap", "Linear"],
        ["fx spot", fx_rate],
        ["Fast Rebuild", "TRUE"]
    ]
    
    eur_securities = [
        "EUSWFVC curncy", "EUSW1VC curncy", "EUSW2V3 curncy", "EUSA3 curncy", 
        "EUSA4 curncy", "EUSA5 curncy", "EUSA6 curncy", "EUSA7 curncy", 
        "EUSA8 curncy", "EUSA9 curncy", "EUSA10 curncy", "EUSA12 curncy", 
        "EUSA15 curncy", "EUSA20 curncy", "EUSA25 curncy", "EUSA30 curncy",
        # Cross currency
        "EUXOQQ1 curncy", "EUXOQQ1F curncy", "EUXOQQ2 curncy", "EUXOQQ3 curncy",
        "EUXOQQ4 curncy", "EUXOQQ5 curncy", "EUXOQQ6 curncy", "EUXOQQ7 curncy",
        "EUXOQQ8 curncy", "EUXOQQ9 curncy", "EUXOQQ10 curncy", "EUXOQQ12 curncy",
        "EUXOQQ15 curncy", "EUXOQQ20 curncy", "EUXOQQ25 curncy", "EUXOQQ30 curncy",
        # Basis OIS
        "EEOBVC1 curncy", "EEOBVC2 curncy", "EEOBVC3 curncy", "EEOBVC4 curncy",
        "EEOBVC5 curncy", "EEOBVC6 curncy", "EEOBVC7 curncy", "EEOBVC8 curncy",
        "EEOBVC9 curncy", "EEOBVC10 curncy", "EEOBVC12 curncy", "EEOBVC15 curncy",
        "EEOBVC20 curncy", "EEOBVC25 curncy", "EEOBVC30 curncy",
        # Basis 6x3
        "EUBSVT1 curncy", "EUBSVT2 curncy", "EUBSVT3 curncy", "EUBSVT4 curncy",
        "EUBSVT5 curncy", "EUBSVT6 curncy", "EUBSVT7 curncy", "EUBSVT8 curncy",
        "EUBSVT9 curncy", "EUBSVT10 curncy", "EUBSVT12 curncy", "EUBSVT15 curncy",
        "EUBSVT20 curncy", "EUBSVT25 curncy", "EUBSVT30 curncy"
    ]
    
    eur_bbg_data = blp.bdh(eur_securities, ["MID"], date, date)
    
    def get_eur_price(security, pricing_date, scale_factor=100):
        price = eur_bbg_data.loc[pricing_date, (security, "MID")]
        return str(price / scale_factor) if pd.notna(price) else "0.0"
    
    def get_eur_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = eur_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"
    
    # Headers
    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    basis_ois_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    basis_ibor_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    
    # Build outright swaps data with dynamic include flags
    outright_swaps_data = []
    swap_instruments = [
        ("EURIRS-AQ", "6m", "EUSWFVC curncy"),
        ("EURIRS-AQ", "12m", "EUSW1VC curncy"),
        ("EURIRS-AQ", "2y", "EUSW2V3 curncy"),
        ("EURIRS-AS", "3y", "EUSA3 curncy"),
        ("EURIRS-AS", "4y", "EUSA4 curncy"),
        ("EURIRS-AS", "5y", "EUSA5 curncy"),
        ("EURIRS-AS", "6y", "EUSA6 curncy"),
        ("EURIRS-AS", "7y", "EUSA7 curncy"),
        ("EURIRS-AS", "8y", "EUSA8 curncy"),
        ("EURIRS-AS", "9y", "EUSA9 curncy"),
        ("EURIRS-AS", "10y", "EUSA10 curncy"),
        ("EURIRS-AS", "12y", "EUSA12 curncy"),
        ("EURIRS-AS", "15y", "EUSA15 curncy"),
        ("EURIRS-AS", "20y", "EUSA20 curncy"),
        ("EURIRS-AS", "25y", "EUSA25 curncy"),
        ("EURIRS-AS", "30y", "EUSA30 curncy")
    ]
    
    for template, tenor, security in swap_instruments:
        price, include_flag = get_eur_price_and_include(security, pricing_date, 100)
        outright_swaps_data.append([template, settle_date, tenor, price, include_flag, security])
    
    # Build basis_xccy_data with dynamic include flags
    basis_xccy_data = []
    xccy_instruments = [
        ("1y", "EUXOQQ1 curncy"), ("18m", "EUXOQQ1F curncy"), ("2y", "EUXOQQ2 curncy"),
        ("3y", "EUXOQQ3 curncy"), ("4y", "EUXOQQ4 curncy"), ("5y", "EUXOQQ5 curncy"),
        ("6y", "EUXOQQ6 curncy"), ("7y", "EUXOQQ7 curncy"), ("8y", "EUXOQQ8 curncy"),
        ("9y", "EUXOQQ9 curncy"), ("10y", "EUXOQQ10 curncy"), ("12y", "EUXOQQ12 curncy"),
        ("15y", "EUXOQQ15 curncy"), ("20y", "EUXOQQ20 curncy"), ("25y", "EUXOQQ25 curncy"),
        ("30y", "EUXOQQ30 curncy")
    ]
    
    for tenor, security in xccy_instruments:
        price, include_flag = get_eur_price_and_include(security, pricing_date, 10000)
        basis_xccy_data.append(["ESTR-SOFR", settle_date, tenor, price, include_flag, security])
    
    # Build basis_ois_data with dynamic include flags
    basis_ois_data = []
    ois_instruments = [
        ("1y", "EEOBVC1 curncy"), ("2y", "EEOBVC2 curncy"), ("3y", "EEOBVC3 curncy"),
        ("4y", "EEOBVC4 curncy"), ("5y", "EEOBVC5 curncy"), ("6y", "EEOBVC6 curncy"),
        ("7y", "EEOBVC7 curncy"), ("8y", "EEOBVC8 curncy"), ("9y", "EEOBVC9 curncy"),
        ("10y", "EEOBVC10 curncy"), ("12y", "EEOBVC12 curncy"), ("15y", "EEOBVC15 curncy"),
        ("20y", "EEOBVC20 curncy"), ("25y", "EEOBVC25 curncy"), ("30y", "EEOBVC30 curncy")
    ]
    
    for tenor, security in ois_instruments:
        price, include_flag = get_eur_price_and_include(security, pricing_date, 10000)
        basis_ois_data.append(["EURESTR-EURIBOR3M", settle_date, tenor, price, include_flag, security])

    # Build basis_ibor_data with dynamic include flags
    basis_ibor_data = []
    ibor_instruments = [
        ("1y", "EUBSVT1 curncy"), ("2y", "EUBSVT2 curncy"), ("3y", "EUBSVT3 curncy"),
        ("4y", "EUBSVT4 curncy"), ("5y", "EUBSVT5 curncy"), ("6y", "EUBSVT6 curncy"),
        ("7y", "EUBSVT7 curncy"), ("8y", "EUBSVT8 curncy"), ("9y", "EUBSVT9 curncy"),
        ("10y", "EUBSVT10 curncy"), ("12y", "EUBSVT12 curncy"), ("15y", "EUBSVT15 curncy"),
        ("20y", "EUBSVT20 curncy"), ("25y", "EUBSVT25 curncy"), ("30y", "EUBSVT30 curncy")
    ]
    
    for tenor, security in ibor_instruments:
        price, include_flag = get_eur_price_and_include(security, pricing_date, 10000)
        basis_ibor_data.append(["EURBASIS-6X3", settle_date, tenor, price, include_flag, security])
    # date_fn.transpose data
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    basis_ois_data = date_fn.transpose(basis_ois_data)
    basis_ibor_data = date_fn.transpose(basis_ibor_data)
    
    curve_name = "eur.primary"
    eur_curve = xc.BuildCurves(curve_name, eur_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data,
                               "basis-ois", basis_ois_headers, basis_ois_data, "basis-ibor", basis_ibor_headers, basis_ibor_data)
    return eur_curve
