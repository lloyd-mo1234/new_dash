import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import printing_scripts.date_fn as date_fn

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
        "USOSFRA CURNCY", "USOSFRB CURNCY", "USOSFRC CURNCY", "USOSFRD CURNCY", 
        "USOSFRE CURNCY", "USOSFRF CURNCY", "USOSFRG CURNCY", "USOSFRH CURNCY", 
        "USOSFRI CURNCY", "USOSFRJ CURNCY", "USOSFRK CURNCY", "USOSFR1C CURNCY", 
        "USOSFR1F CURNCY", "USOSFR1I CURNCY",
        
        # Yearly tenors
        "USOSFR1 CURNCY", "USOSFR2 CURNCY", "USOSFR3 CURNCY", "USOSFR4 CURNCY", 
        "USOSFR5 CURNCY", "USOSFR6 CURNCY", "USOSFR7 CURNCY", "USOSFR8 CURNCY", 
        "USOSFR9 CURNCY", "USOSFR10 CURNCY", "USOSFR12 CURNCY", "USOSFR15 CURNCY", 
        "USOSFR20 CURNCY", "USOSFR25 CURNCY", "USOSFR30 CURNCY"
    ]
   
    # Get Bloomberg data for all USD SOFR securities (use original date format for Bloomberg)
    usd_bbg_data = blp.bdh(usd_securities, ["MID"], date, date)

    def get_usd_price(security, pricing_date, scale_factor=100):
        try:
            price = usd_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor)
        except (KeyError, IndexError):
            return "0.0"
    
    def get_usd_price_and_include(security, pricing_date, scale_factor=100):
        try:
            price = usd_bbg_data.loc[pricing_date, (security, "MID")]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError):
            return "0.0", "0"

    usd_outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    
    # Build USD SOFR Outright Swaps Data with dynamic include flags
    usd_outright_swaps_data = []
    swap_instruments = [
        # Monthly tenors
        ("1m", "USOSFRA CURNCY"), ("2m", "USOSFRB CURNCY"), ("3m", "USOSFRC CURNCY"),
        ("4m", "USOSFRD CURNCY"), ("5m", "USOSFRE CURNCY"), ("6m", "USOSFRF CURNCY"),
        ("7m", "USOSFRG CURNCY"), ("8m", "USOSFRH CURNCY"), ("9m", "USOSFRI CURNCY"),
        ("10m", "USOSFRJ CURNCY"), ("11m", "USOSFRK CURNCY"), ("15m", "USOSFR1C CURNCY"),
        ("18m", "USOSFR1F CURNCY"), ("21m", "USOSFR1I CURNCY"),
        # Yearly tenors
        ("1y", "USOSFR1 CURNCY"), ("2y", "USOSFR2 CURNCY"), ("3y", "USOSFR3 CURNCY"),
        ("4y", "USOSFR4 CURNCY"), ("5y", "USOSFR5 CURNCY"), ("6y", "USOSFR6 CURNCY"),
        ("7y", "USOSFR7 CURNCY"), ("8y", "USOSFR8 CURNCY"), ("9y", "USOSFR9 CURNCY"),
        ("10y", "USOSFR10 CURNCY"), ("12y", "USOSFR12 CURNCY"), ("15y", "USOSFR15 CURNCY"),
        ("20y", "USOSFR20 CURNCY"), ("25y", "USOSFR25 CURNCY"), ("30y", "USOSFR30 CURNCY")
    ]
    
    for tenor, security in swap_instruments:
        price, include_flag = get_usd_price_and_include(security, pricing_date, 100)
        usd_outright_swaps_data.append(["USDSOFR", settle_date, tenor, price, include_flag, security])
    
    curve_name = "usd.sofr.primary"
    usd_outright_swaps_data = np.array(usd_outright_swaps_data).T.tolist()
    
    # Build USD SOFR curve with only outright swaps
    usd_curve = xc.BuildCurves(curve_name, usd_sofr_config, "outright-swaps", usd_outright_swaps_headers, usd_outright_swaps_data)
    
    return usd_curve
