import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime
import pandas as pd
import date_fn

def gbp_curve_serialiser(date):
    settle_date = xc.DateAdd(date, "0b", "lnb")  # Note: settle date is same day for GBP
    settle_date = pd.to_datetime(settle_date, origin='1899-12-30', unit='D').strftime('%Y-%m-%d')
    fx_rate = str(date_fn.get_fx_rate("gbpusd", date))
    pricing_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    gbp_config = [
        ["Valuation Date", date],
        ["Default Settle Date", settle_date],
        ["Interp Method", "Parabolic"],
        ["Interp Variable", "Rate Time"],
        ["Use Monotonic", "FALSE"],
        ["Currency", "GBP"],
        ["Funding Currency", "USD"],
        ["Curve Bundle", "GBPUSD_BUNDLE"],
        ["Calendar", "LNB"],
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
    
    gbp_securities = [
        "BPSWSF CURNCY", "BPSWS1 CURNCY", "BPSWS1C CURNCY", "BPSWS2 CURNCY",
        "BPSWS3 CURNCY", "BPSWS4 CURNCY", "BPSWS5 CURNCY", "BPSWS6 CURNCY",
        "BPSWS7 CURNCY", "BPSWS8 CURNCY", "BPSWS9 CURNCY", "BPSWS10 CURNCY",
        "BPSWS12 CURNCY", "BPSWS15 CURNCY", "BPSWS20 CURNCY", "BPSWS25 CURNCY",
        "BPSWS30 CURNCY",
        # Cross currency
        "BPXOQQ1 CURNCY", "BPXOQQ1F CURNCY", "BPXOQQ2 CURNCY", "BPXOQQ3 CURNCY",
        "BPXOQQ4 CURNCY", "BPXOQQ5 CURNCY", "BPXOQQ6 CURNCY", "BPXOQQ7 CURNCY",
        "BPXOQQ8 CURNCY", "BPXOQQ9 CURNCY", "BPXOQQ10 CURNCY", "BPXOQQ12 CURNCY",
        "BPXOQQ15 CURNCY", "BPXOQQ20 CURNCY", "BPXOQQ25 CURNCY", "BPXOQQ30 CURNCY"
    ]
    
    gbp_bbg_data = blp.bdh(gbp_securities, ["MID"], date, date)
    
    def get_gbp_price(security, pricing_date, scale_factor=100):
        price = gbp_bbg_data.loc[pricing_date, (security, "MID")]
        return str(price / scale_factor) if pd.notna(price) else "0.0"
    
    outright_swaps_headers = ["Template", "Start Date", "End Date", "Rate", "Include", "Risk Node"]
    basis_xccy_headers = ["Template", "Start Date", "End Date", "Spread", "Include", "Risk Node"]
    
    outright_swaps_data = [
        ["GBPOIS", settle_date, "6m", get_gbp_price("BPSWSF CURNCY", pricing_date), "1", "BPSWSF CURNCY"],
        ["GBPOIS", settle_date, "1y", get_gbp_price("BPSWS1 CURNCY", pricing_date), "1", "BPSWS1 CURNCY"],
        ["GBPOIS", settle_date, "18m", get_gbp_price("BPSWS1C CURNCY", pricing_date), "1", "BPSWS1C CURNCY"],
        ["GBPOIS", settle_date, "2y", get_gbp_price("BPSWS2 CURNCY", pricing_date), "1", "BPSWS2 CURNCY"],
        ["GBPOIS", settle_date, "3y", get_gbp_price("BPSWS3 CURNCY", pricing_date), "1", "BPSWS3 CURNCY"],
        ["GBPOIS", settle_date, "4y", get_gbp_price("BPSWS4 CURNCY", pricing_date), "1", "BPSWS4 CURNCY"],
        ["GBPOIS", settle_date, "5y", get_gbp_price("BPSWS5 CURNCY", pricing_date), "1", "BPSWS5 CURNCY"],
        ["GBPOIS", settle_date, "6y", get_gbp_price("BPSWS6 CURNCY", pricing_date), "1", "BPSWS6 CURNCY"],
        ["GBPOIS", settle_date, "7y", get_gbp_price("BPSWS7 CURNCY", pricing_date), "1", "BPSWS7 CURNCY"],
        ["GBPOIS", settle_date, "8y", get_gbp_price("BPSWS8 CURNCY", pricing_date), "1", "BPSWS8 CURNCY"],
        ["GBPOIS", settle_date, "9y", get_gbp_price("BPSWS9 CURNCY", pricing_date), "1", "BPSWS9 CURNCY"],
        ["GBPOIS", settle_date, "10y", get_gbp_price("BPSWS10 CURNCY", pricing_date), "1", "BPSWS10 CURNCY"],
        ["GBPOIS", settle_date, "12y", get_gbp_price("BPSWS12 CURNCY", pricing_date), "1", "BPSWS12 CURNCY"],
        ["GBPOIS", settle_date, "15y", get_gbp_price("BPSWS15 CURNCY", pricing_date), "1", "BPSWS15 CURNCY"],
        ["GBPOIS", settle_date, "20y", get_gbp_price("BPSWS20 CURNCY", pricing_date), "1", "BPSWS20 CURNCY"],
        ["GBPOIS", settle_date, "25y", get_gbp_price("BPSWS25 CURNCY", pricing_date), "1", "BPSWS25 CURNCY"],
        ["GBPOIS", settle_date, "30y", get_gbp_price("BPSWS30 CURNCY", pricing_date), "1", "BPSWS30 CURNCY"]
    ]
    
    basis_xccy_data = [
        ["SONIA-SOFR", settle_date, "1y", get_gbp_price("BPXOQQ1 CURNCY", pricing_date, 10000), "1", "BPXOQQ1 CURNCY"],
        ["SONIA-SOFR", settle_date, "18m", get_gbp_price("BPXOQQ1F CURNCY", pricing_date, 10000), "1", "BPXOQQ1F CURNCY"],
        ["SONIA-SOFR", settle_date, "2y", get_gbp_price("BPXOQQ2 CURNCY", pricing_date, 10000), "1", "BPXOQQ2 CURNCY"],
        ["SONIA-SOFR", settle_date, "3y", get_gbp_price("BPXOQQ3 CURNCY", pricing_date, 10000), "1", "BPXOQQ3 CURNCY"],
        ["SONIA-SOFR", settle_date, "4y", get_gbp_price("BPXOQQ4 CURNCY", pricing_date, 10000), "1", "BPXOQQ4 CURNCY"],
        ["SONIA-SOFR", settle_date, "5y", get_gbp_price("BPXOQQ5 CURNCY", pricing_date, 10000), "1", "BPXOQQ5 CURNCY"],
        ["SONIA-SOFR", settle_date, "6y", get_gbp_price("BPXOQQ6 CURNCY", pricing_date, 10000), "1", "BPXOQQ6 CURNCY"],
        ["SONIA-SOFR", settle_date, "7y", get_gbp_price("BPXOQQ7 CURNCY", pricing_date, 10000), "1", "BPXOQQ7 CURNCY"],
        ["SONIA-SOFR", settle_date, "8y", get_gbp_price("BPXOQQ8 CURNCY", pricing_date, 10000), "1", "BPXOQQ8 CURNCY"],
        ["SONIA-SOFR", settle_date, "9y", get_gbp_price("BPXOQQ9 CURNCY", pricing_date, 10000), "1", "BPXOQQ9 CURNCY"],
        ["SONIA-SOFR", settle_date, "10y", get_gbp_price("BPXOQQ10 CURNCY", pricing_date, 10000), "1", "BPXOQQ10 CURNCY"],
        ["SONIA-SOFR", settle_date, "12y", get_gbp_price("BPXOQQ12 CURNCY", pricing_date, 10000), "1", "BPXOQQ12 CURNCY"],
        ["SONIA-SOFR", settle_date, "15y", get_gbp_price("BPXOQQ15 CURNCY", pricing_date, 10000), "1", "BPXOQQ15 CURNCY"],
        ["SONIA-SOFR", settle_date, "20y", get_gbp_price("BPXOQQ20 CURNCY", pricing_date, 10000), "1", "BPXOQQ20 CURNCY"],
        ["SONIA-SOFR", settle_date, "25y", get_gbp_price("BPXOQQ25 CURNCY", pricing_date, 10000), "1", "BPXOQQ25 CURNCY"],
        ["SONIA-SOFR", settle_date, "30y", get_gbp_price("BPXOQQ30 CURNCY", pricing_date, 10000), "1", "BPXOQQ30 CURNCY"]
    ]
    
    outright_swaps_data = date_fn.transpose(outright_swaps_data)
    basis_xccy_data = date_fn.transpose(basis_xccy_data)
    
    curve_name = "gbp.sonia.primary"
    gbp_curve = xc.BuildCurves(curve_name, gbp_config, "outright-swaps", outright_swaps_headers,
                               outright_swaps_data, "basis-xccy", basis_xccy_headers, basis_xccy_data)
    return gbp_curve