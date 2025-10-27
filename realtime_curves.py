import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import sys

# Add the printing_scripts directory to the path to import date_fn
script_dir = os.path.dirname(os.path.abspath(__file__))
chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
printing_scripts_path = os.path.join(chart_app_dir, 'printing_scripts')
if printing_scripts_path not in sys.path:
    sys.path.append(printing_scripts_path)

import printing_scripts.date_fn as date_fn


def get_all_securities_for_currencies(currencies):
    """Collect all securities needed for the specified currencies"""
    all_securities = []
    
    # FX rates (always needed for non-USD currencies)
    fx_pairs = []
    if any(ccy.lower() != 'usd' for ccy in currencies):
        fx_mapping = {
            'aud': 'audusd curncy',
            'eur': 'eurusd curncy', 
            'jpy': 'usdjpy curncy',
            'cad': 'usdcad curncy',
            'nzd': 'nzdusd curncy'
        }
        for ccy in currencies:
            ccy_lower = ccy.lower()
            if ccy_lower in fx_mapping:
                fx_pairs.append(fx_mapping[ccy_lower])
    
    all_securities.extend(fx_pairs)
    
    # Currency-specific securities
    for ccy in currencies:
        ccy_lower = ccy.lower()
        
        if ccy_lower == 'aud':
            # AUD securities
            aud_securities = [
                # Swap instruments
                "ADSWAP1Q curncy", "ADSWAP2Q curncy", "ADSWAP3Q curncy",
                "ADSWAP4 curncy", "ADSWAP5 curncy", "ADSWAP6 curncy", "ADSWAP7 curncy",
                "ADSWAP8 curncy", "ADSWAP9 curncy", "ADSWAP10 curncy", "ADSWAP12 curncy",
                "ADSWAP15 curncy", "ADSWAP20 curncy", "ADSWAP25 curncy", "ADSWAP30 curncy",
                # IBOR instruments
                "ADBBCF1 curncy", "ADBBCF2 curncy", "ADBBCF3 curncy", "ADBBCF4 curncy",
                "ADBBCF5 curncy", "ADBBCF6 curncy", "ADBBCF7 curncy", "ADBBCF8 curncy",
                "ADBBCF9 curncy", "ADBBCF10 curncy", "ADBBCF12 curncy", "ADBBCF15 curncy",
                "ADBBCF20 curncy", "ADBBCF25 curncy", "ADBBCF30 curncy",
                # OIS instruments
                "ADBBCO1 curncy", "ADBBCO2 curncy", "ADBBCO3 curncy", "ADBBCO4 curncy",
                "ADBBCO5 curncy", "ADBBCO6 curncy", "ADBBCO7 curncy", "ADBBCO8 curncy",
                "ADBBCO9 curncy", "ADBBCO10 curncy", "ADBBCO12 curncy", "ADBBCO15 curncy",
                "ADBBCO20 curncy", "ADBBCO25 curncy", "ADBBCO30 curncy",
                # XCCY instruments
                "ADOIQQ1 curncy", "ADOIQQ2 curncy", "ADOIQQ3 curncy", "ADOIQQ4 curncy",
                "ADOIQQ5 curncy", "ADOIQQ6 curncy", "ADOIQQ7 curncy", "ADOIQQ8 curncy",
                "ADOIQQ9 curncy", "ADOIQQ10 curncy", "ADOIQQ12 curncy", "ADOIQQ15 curncy",
                "ADOIQQ20 curncy", "ADOIQQ25 curncy", "ADOIQQ30 curncy"
            ]
            all_securities.extend(aud_securities)
            
        elif ccy_lower == 'eur':
            # EUR securities
            eur_securities = [
                # Swap instruments
                "EUSWFVC curncy", "EUSW1VC curncy", "EUSW2V3 curncy", "EUSA3 curncy",
                "EUSA4 curncy", "EUSA5 curncy", "EUSA6 curncy", "EUSA7 curncy",
                "EUSA8 curncy", "EUSA9 curncy", "EUSA10 curncy", "EUSA12 curncy",
                "EUSA15 curncy", "EUSA20 curncy", "EUSA25 curncy", "EUSA30 curncy",
                # XCCY instruments
                "EUXOQQ1 curncy", "EUXOQQ1F curncy", "EUXOQQ2 curncy", "EUXOQQ3 curncy",
                "EUXOQQ4 curncy", "EUXOQQ5 curncy", "EUXOQQ6 curncy", "EUXOQQ7 curncy",
                "EUXOQQ8 curncy", "EUXOQQ9 curncy", "EUXOQQ10 curncy", "EUXOQQ12 curncy",
                "EUXOQQ15 curncy", "EUXOQQ20 curncy", "EUXOQQ25 curncy", "EUXOQQ30 curncy",
                # OIS instruments
                "EEOBVC1 curncy", "EEOBVC2 curncy", "EEOBVC3 curncy", "EEOBVC4 curncy",
                "EEOBVC5 curncy", "EEOBVC6 curncy", "EEOBVC7 curncy", "EEOBVC8 curncy",
                "EEOBVC9 curncy", "EEOBVC10 curncy", "EEOBVC12 curncy", "EEOBVC15 curncy",
                "EEOBVC20 curncy", "EEOBVC25 curncy", "EEOBVC30 curncy",
                # IBOR instruments
                "EUBSVT1 curncy", "EUBSVT2 curncy", "EUBSVT3 curncy", "EUBSVT4 curncy",
                "EUBSVT5 curncy", "EUBSVT6 curncy", "EUBSVT7 curncy", "EUBSVT8 curncy",
                "EUBSVT9 curncy", "EUBSVT10 curncy", "EUBSVT12 curncy", "EUBSVT15 curncy",
                "EUBSVT20 curncy", "EUBSVT25 curncy", "EUBSVT30 curncy"
            ]
            all_securities.extend(eur_securities)
            
        elif ccy_lower == 'jpy':
            # JPY securities
            jpy_securities = [
                # Swap instruments
                "JYSOC CURNCY", "JYSOF CURNCY", "JYSOI CURNCY", "JYSO1 CURNCY",
                "JYSO1F CURNCY", "JYSO2 CURNCY", "JYSO3 CURNCY", "JYSO4 CURNCY",
                "JYSO5 CURNCY", "JYSO6 CURNCY", "JYSO7 CURNCY", "JYSO8 CURNCY",
                "JYSO9 CURNCY", "JYSO10 CURNCY", "JYSO12 CURNCY", "JYSO15 CURNCY",
                "JYSO20 CURNCY", "JYSO25 CURNCY", "JYSO30 CURNCY",
                # XCCY instruments
                "JYBSS12M CURNCY", "JYBSS2Y CURNCY", "JYBSS3Y CURNCY", "JYBSS4Y CURNCY",
                "JYBSS5Y CURNCY", "JYBSS6Y CURNCY", "JYBSS7Y CURNCY", "JYBSS8Y CURNCY",
                "JYBSS9Y CURNCY", "JYBSS10Y CURNCY", "JYBSS12Y CURNCY", "JYBSS15Y CURNCY",
                "JYBSS20Y CURNCY", "JYBSS25Y CURNCY", "JYBSS30Y CURNCY"
            ]
            all_securities.extend(jpy_securities)
            
        elif ccy_lower == 'cad':
            # CAD securities
            cad_securities = [
                # Swap instruments
                "CDSO1 CURNCY", "CDSO2 CURNCY", "CDSO3 CURNCY", "CDSO4 CURNCY",
                "CDSO5 CURNCY", "CDSO6 CURNCY", "CDSO7 CURNCY", "CDSO8 CURNCY",
                "CDSO9 CURNCY", "CDSO10 CURNCY", "CDSO12 CURNCY", "CDSO15 CURNCY",
                "CDSO20 CURNCY", "CDSO30 CURNCY",
                # XCCY instruments
                "CDXOQQ1 CURNCY", "CDXOQQ2 CURNCY", "CDXOQQ3 CURNCY", "CDXOQQ4 CURNCY",
                "CDXOQQ5 CURNCY", "CDXOQQ6 CURNCY", "CDXOQQ7 CURNCY", "CDXOQQ8 CURNCY",
                "CDXOQQ9 CURNCY", "CDXOQQ10 CURNCY", "CDXOQQ12 CURNCY", "CDXOQQ15 CURNCY",
                "CDXOQQ20 CURNCY", "CDXOQQ25 CURNCY", "CDXOQQ30 CURNCY"
            ]
            all_securities.extend(cad_securities)
            
        elif ccy_lower == 'nzd':
            # NZD securities
            nzd_securities = [
                # Swap instruments
                "NDSWAP1 CURNCY", "NDSWAP2 CURNCY", "NDSWAP3 CURNCY", "NDSWAP4 CURNCY",
                "NDSWAP5 CURNCY", "NDSWAP6 CURNCY", "NDSWAP7 CURNCY", "NDSWAP8 CURNCY",
                "NDSWAP9 CURNCY", "NDSWAP10 CURNCY", "NDSWAP12 CURNCY", "NDSWAP15 CURNCY",
                "NDSWAP20 CURNCY", "NDSWAP25 CURNCY", "NDSWAP30 CURNCY",
                # XCCY instruments
                "NDBSQQ1 CURNCY", "NDBSQQ2 CURNCY", "NDBSQQ3 CURNCY", "NDBSQQ4 CURNCY",
                "NDBSQQ5 CURNCY", "NDBSQQ6 CURNCY", "NDBSQQ7 CURNCY", "NDBSQQ8 CURNCY",
                "NDBSQQ9 CURNCY", "NDBSQQ10 CURNCY", "NDBSQQ12 CURNCY", "NDBSQQ15 CURNCY",
                "NDBSQQ20 CURNCY"
            ]
            all_securities.extend(nzd_securities)
            
        elif ccy_lower == 'usd':
            # USD securities
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
            all_securities.extend(usd_securities)
    
    return list(set(all_securities))  # Remove duplicates

def get_all_prices_single_call(securities):
    """Make a single BDP call for all securities and return the results"""
    try:

        all_prices = blp.bdp(securities, "LAST_PRICE")

        return all_prices
    except Exception as e:

        return pd.DataFrame()

def get_price_from_results(all_prices, security, scale_factor=100):
    """Extract price for a specific security from the bulk results"""

    try:
        if security in all_prices.index:
            price = all_prices.loc[security, "last_price"]
            if pd.notna(price):
                return str(price / scale_factor), "1"
        return "0.0", "0"
    except (KeyError, IndexError, Exception):
        return "0.0", "0"

def get_fx_rate_from_results(all_prices, ccy):
    """Get FX rate from bulk results"""
    try:
        fx_security = ccy
        if fx_security in all_prices.index:
            rate = all_prices.loc[fx_security, "last_price"]
            if pd.notna(rate):
                return rate
        return None
    except:
        return None

def aud_curve_serialiser_realtime(date, all_prices=None):
    """AUD curve serializer using BDP for real-time prices"""
    date = str(date)
    settle_date = xc.DateAdd(date, "2b", "syb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    
    # Get FX rate from bulk results if available, otherwise fallback error
    if all_prices is not None:
        fx_rate = str(get_fx_rate_from_results(all_prices, "audusd curncy"))
    else:
        fx_rate = "1.0"  # Fallback if bulk results not available
    
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()

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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)

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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
        basis_xccy_data.append(["aonia-sofr", settle_date, tenor, price, include_flag, security])

    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_ibor_data = date_fn.transpose(basis_ibor_data)
    basis_ois_data = date_fn.transpose(basis_ois_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)

    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_aud_curve.json"



    

    aud_curve = xc.BuildCurves(curve_name, aud_config,"outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-ibor", basis_ibor_headers, basis_ibor_data, "basis-ois", basis_ois_headers,
                               basis_ois_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    
    
    return aud_curve

def eur_curve_serialiser_realtime(date, all_prices=None):
    """EUR curve serializer using BDP for real-time prices"""
    settle_date = xc.DateAdd(date, "2b", "tgt")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    
    # Get FX rate from bulk results if available, otherwise fallback
    if all_prices is not None:
        fx_rate = str(get_fx_rate_from_results(all_prices, "eurusd curncy"))
    else:
        fx_rate = "1.0"  # Fallback if bulk results not available
    
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
    
    def get_eur_price_and_include_realtime(security, scale_factor=100):
        try:
            price_data = blp.bdp(security, "LAST_PRICE")
            price = price_data.iloc[0, 0]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError, Exception):
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 100), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
        else:
            # Fallback to individual call if bulk results not available
            try:
                price_data = blp.bdp(security, "LAST_PRICE")
                price = price_data.iloc[0, 0]
                price, include_flag = str(price / 10000), "1"
            except (KeyError, IndexError, Exception):
                price, include_flag = "0.0", "0"
        basis_ibor_data.append(["EURBASIS-6X3", settle_date, tenor, price, include_flag, security])
    
    # Transpose data
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    basis_ois_data = date_fn.transpose(basis_ois_data)
    basis_ibor_data = date_fn.transpose(basis_ibor_data)
    
    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_eur_curve.json"
    eur_curve = xc.BuildCurves(curve_name, eur_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data,
                               "basis-ois", basis_ois_headers, basis_ois_data, "basis-ibor", basis_ibor_headers, basis_ibor_data)
    return eur_curve

def jpy_curve_serialiser_realtime(date, all_prices=None):
    """JPY curve serializer using BDP for real-time prices"""
    settle_date = xc.DateAdd(date, "2b", "tkb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    # Get FX rate from bulk results if available, otherwise fallback
    if all_prices is not None:
        usdjpy_rate = get_fx_rate_from_results(all_prices, "usdjpy curncy")
        fx_rate = str(1/usdjpy_rate) if usdjpy_rate else "1.0"  # Inverted for JPY
    else:
        fx_rate = "1.0"  # Fallback if bulk results not available
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
    
    def get_jpy_price_and_include_realtime(security, scale_factor=100):
        try:
            price_data = blp.bdp(security, "LAST_PRICE")
            price = price_data.iloc[0, 0]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError, Exception):
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
            basis_xccy_data.append(["TONAR-SOFR", settle_date, tenor, price, include_flag, security])
    
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_jpy_curve.json"
    jpy_curve = xc.BuildCurves(curve_name, jpy_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return jpy_curve

def cad_curve_serialiser_realtime(date, all_prices=None):
    """CAD curve serializer using BDP for real-time prices"""
    settle_date = xc.DateAdd(date, "1b", "trb")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    # Get FX rate from bulk results if available, otherwise fallback
    if all_prices is not None:
        usdcad_rate = get_fx_rate_from_results(all_prices, "usdcad curncy")
        fx_rate = str(1/usdcad_rate) if usdcad_rate else "1.0"  # Inverted for CAD
    else:
        fx_rate = "1.0"  # Fallback if bulk results not available
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
    
    def get_cad_price_and_include_realtime(security, scale_factor=100):
        try:
            price_data = blp.bdp(security, "LAST_PRICE")
            price = price_data.iloc[0, 0]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError, Exception):
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
            basis_xccy_data.append(["CORRA-SOFR", settle_date, tenor, price, include_flag, security])
    
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_cad_curve.json"
    cad_curve = xc.BuildCurves(curve_name, cad_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return cad_curve

def nzd_curve_serialiser_realtime(date, all_prices=None):
    """NZD curve serializer using BDP for real-time prices"""
    settle_date = xc.DateAdd(date, "2b", "aub,web")
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    
    # Get FX rate from bulk results if available, otherwise fallback
    if all_prices is not None:
        fx_rate = str(get_fx_rate_from_results(all_prices, "nzdusd curncy"))
    else:
        fx_rate = "1.0"  # Fallback if bulk results not available
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
    
    def get_nzd_price_and_include_realtime(security, scale_factor=100):
        try:
            price_data = blp.bdp(security, "LAST_PRICE")
            price = price_data.iloc[0, 0]
            return str(price / scale_factor), "1"
        except (KeyError, IndexError, Exception):
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 10000)
            basis_xccy_data.append(["BKBM-SOFR", settle_date, tenor, price, include_flag, security])

    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_nzd_curve.json"
    nzd_curve = xc.BuildCurves(curve_name, nzd_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return nzd_curve

def usd_curve_serialiser_realtime(date, all_prices=None):
    """USD curve serializer using BDP for real-time prices"""
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
        if all_prices is not None:
            price, include_flag = get_price_from_results(all_prices, security, 100)
        usd_outright_swaps_data.append(["USDSOFR", settle_date, tenor, price, include_flag, security])
    
    # Use today's date format for curve name so it matches expected filename
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    curve_name = f"{today_yymmdd}_usd_curve.json"
    usd_outright_swaps_data = np.array(usd_outright_swaps_data).T.tolist()
    
    # Build USD SOFR curve with only outright swaps
    usd_curve = xc.BuildCurves(curve_name, usd_sofr_config, "outright-swaps", usd_outright_swaps_headers, usd_outright_swaps_data)
    
    return usd_curve

def build_selected_curves_realtime(date, currencies=["aud", "eur"]):
    """Build selected currency curves using real-time BDP data with SINGLE BDP CALL optimization
    
    Args:
        date: Date string in YYYY-MM-DD format
        currencies: List of currency codes to build (default: ["aud", "eur"])
                   Available: "usd", "aud", "eur", "jpy", "cad", "nzd"
    
    Returns:
        Dictionary with 'curves' and 'bundles' keys
    """
    curves = {}
    bundles = {}
    
    # Convert currencies to lowercase for consistency with input
    currencies.append('usd')
    currencies = [ccy.lower() for ccy in currencies]
    
    # OPTIMIZATION: Get all securities needed and make ONE BDP call
    all_securities = get_all_securities_for_currencies(currencies)
    all_prices = get_all_prices_single_call(all_securities)
    
    if all_prices.empty:
        all_prices = None
    
    # Step 1: Always build USD first (required for FX bundles)
    # Only build USD if we have non-USD currencies
    non_usd_currencies = [ccy for ccy in currencies if ccy != "usd"]
    if non_usd_currencies:
        try:
            curves['USD'] = usd_curve_serialiser_realtime(date, all_prices)
        except Exception as e:
            return {'curves': curves, 'bundles': bundles}
    
    # Get today's date format for USD curve name
    today_yymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d")
    usd_curve_name = f"{today_yymmdd}_usd_curve.json"
    
    # Step 2: Build only the required FX bundles for requested currencies
    bundle_mapping = {
        "aud": ("audusd", "AUDUSD_BUNDLE"),
        "eur": ("eurusd", "EURUSD_BUNDLE"), 
        "jpy": ("usdjpy", "USDJPY_BUNDLE"),
        "cad": ("usdcad", "USDCAD_BUNDLE"),
        "nzd": ("nzdusd", "NZDUSD_BUNDLE")
    }
    
    for ccy in currencies:
        if ccy in bundle_mapping:
            fx_pair, bundle_name = bundle_mapping[ccy]
            try:
                # Use bulk results for FX rate if available
                if all_prices is not None:
                    fx_pair_name = fx_pair + " curncy"
                    fx_rate = str(get_fx_rate_from_results(all_prices, fx_pair_name))
                bundles[bundle_name] = xc.BuildBlockBundle(bundle_name, [["USD", usd_curve_name]], [[fx_pair.upper(), fx_rate]])
            except Exception as e:
                pass
    
    # Step 3: Build only the selected currency curves using bulk pricing data
    curve_functions = {
        "aud": aud_curve_serialiser_realtime,
        "eur": eur_curve_serialiser_realtime,
        "jpy": jpy_curve_serialiser_realtime,
        "cad": cad_curve_serialiser_realtime,
        "nzd": nzd_curve_serialiser_realtime
    }
    
    for ccy in currencies:
        if ccy in curve_functions:
            try:
                curves[ccy.upper()] = curve_functions[ccy](date, all_prices)
            except Exception as e:
                pass
    
    # Step 4: Build today's core block bundle with all successfully built curves
    # Collect all successfully built curves
    currency_list = []
    curve_names = []
    
    # Include all currencies that were successfully built
    for ccy_key, curve_result in curves.items():
        if curve_result:  # If curve was built successfully
            ccy_code = ccy_key[:3].upper()  # Get 3-letter uppercase code
            curve_name = f"{today_yymmdd}_{ccy_key.lower()}_curve.json"
            currency_list.append(ccy_code)
            curve_names.append(curve_name)
    
    # Build today's core bundle if we have curves
    if currency_list:
        bundle_name = f"{today_yymmdd}_core_bundle"
        try:
            # Build currency-curve pairs: [[ccy1, curve1], [ccy2, curve2], ...]
            currency_curve_pairs = []
            for i in range(len(currency_list)):
                currency_curve_pairs.append([currency_list[i], curve_names[i]])
            
            # Build FX pairs: [["AUDUSD", "1"], ["EURUSD", "1"], ...]
            fx_pair_rates = [["AUDUSD", "1"], ["EURUSD", "1"], ["USDJPY", "1"], ["USDCAD", "1"], ["NZDUSD", "1"], ["GBPUSD", "1"]]
            
            xc.BuildBlockBundle(bundle_name, currency_curve_pairs, fx_pair_rates)
            
            # Add bundle info to return data
            bundles['core'] = {
                'name': bundle_name,
                'currencies': currency_list,
                'curve_names': curve_names
            }
            
        except Exception as e:
            pass
    
    return {'curves': curves, 'bundles': bundles}

if __name__ == "__main__":
    # Example usage
    today = datetime.now().strftime("%Y-%m-%d")
    curves = build_selected_curves_realtime(today, currencies=["aud", "eur"])
    print(curves)
    print(f"Built {len(curves)} curves successfully")
