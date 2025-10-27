import os
import re
from datetime import datetime
import pandas as pd
from cba.analytics import xcurves as xc
from simplified_curve_loader import (
    initialize_curves_simplified, 
    is_simplified_curves_loaded,
    get_simplified_bundle_name,
    get_available_dates,
    get_simplified_cache_stats,
    clear_simplified_curves
)

# Currency configuration with template-embedded currency codes
CURRENCY_CONFIG = {
    # Standard AUD templates
    'aud': {
        'template': 'AUDIRS-SS',
        'index': 'AUD 6M'
    },
    'audbs': {
        'template': 'BBSW-SOFR',
        'index': 'USD SOFR'
    },
    'audxc': {
        'template': 'AONIA-SOFR',
        'index': 'USD SOFR'
    },
    'audbob': {
        'template': 'AUDBOB-3M',
        'index': 'AUD 6M'
    },
    'aud6s3s': {
        'template': 'AUDBASIS-6X3',
        'index': 'AUD 6M'
    },
    
    # Standard GBP templates
    'gbp': {
        'template': 'GBPOIS',
        'index': 'GBP OIS'
    },
    'gbpxc': {
        'template': 'SONIA-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard USD templates
    'usd': {
        'template': 'USDSOFR',
        'index': 'USD SOFR'
    },
    
    # Standard EUR templates
    'eur': {
        'template': 'EURIRS-AS',
        'index': 'EUR 6M'
    },
    'eurxc': {
        'template': 'ESTR-SOFR',
        'index': 'USD SOFR'
    },
    'eurbob': {
        'template': 'EURESTR-EURIBOR3M',
        'index': 'EUR 6M'
    },
    'eur6s3s': {
        'template': 'EURBASIS-6X3',
        'index': 'EUR 6M'
    },
    
    # Standard JPY templates
    'jpy': {
        'template': 'JPYOIS',
        'index': 'JPY OIS'
    },
    'jpyxc': {
        'template': 'TONAR-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard CAD templates
    'cad': {
        'template': 'CADOIS',
        'index': 'CAD OIS'
    },
    'cadxc': {
        'template': 'CORRA-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard NZD templates
    'nzd': {
        'template': 'NZDIRS-SQ',
        'index': 'NZD 3M'
    },
    'nzdbs': {
        'template': 'BKBM-SOFR',
        'index': 'USD SOFR'
    },
    'nzdxc': {
        'template': 'NZOCR-SOFR',
        'index': 'USD SOFR'
    }
}

def yymmdd_to_datetime(date_str: str) -> datetime:
    """Convert YYMMDD to datetime object"""
    yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
    # Handle dates starting with 9 as 1990s (90-99 -> 1990-1999)
    year = 1900 + yy if yy >= 90 else 2000 + yy
    return datetime(year, mm, dd)

def yymmdd_to_excel_date(date_str: str) -> int:
    """Convert YYMMDD to Excel date number"""
    date_obj = yymmdd_to_datetime(date_str)
    excel_epoch = datetime(1900, 1, 1)
    return (date_obj - excel_epoch).days + 2

def simplified_swap_rate(start, end, currency: str = 'aud'):
    """
    Calculate swap rates using pre-loaded core bundles (simplified approach)
    
    Args:
        start: Start tenor (e.g., '1y')
        end: End tenor (e.g., '1y') 
        currency: Currency code (including template, e.g., 'aud', 'audxc', 'audbs')
    
    Returns:
        dict: Dictionary of {date: rate} pairs
    """
    if currency not in CURRENCY_CONFIG:
        raise ValueError(f"Unsupported currency: {currency}")
    
    # Get configuration directly from CURRENCY_CONFIG
    config = CURRENCY_CONFIG[currency]
    swap_rates = {}
    
    # Get all available dates from the simplified cache
    available_dates = get_available_dates()
    
    if not available_dates:
        print("❌ No core bundles loaded. Run initialize_curves_simplified() first.")
        return {}
    
    print(f"🔍 Calculating {currency}.{start}{end} rates for {len(available_dates)} dates")
    
    for i, date_str in enumerate(available_dates, 1):
        try:
            # Get the bundle name for this date
            bundle_name = get_simplified_bundle_name(date_str)
            if not bundle_name:
                continue
            
            date = yymmdd_to_datetime(date_str)
            date_excel = str(yymmdd_to_excel_date(date_str))
            
            settlement_date = str(xc.DateAdd(date_excel, "2b", "syb"))
            swap_start = str(xc.DateAdd(settlement_date, start, "syb"))
            swap_end = str(xc.DateAdd(swap_start, end, "syb"))

            # Use the pre-loaded bundle directly
            rate = xc.StandardSwapParRate(
                bundle_name,
                config['template'],
                settlement_date,
                swap_start,
                swap_end,
                config['index']
            )
            
            swap_rates[date] = float(rate) * 100
                
        except Exception as e:
            # Filter out redundant xcurves error messages about curve indices
            error_str = str(e)
            if "does not exist, has the curve been built?" not in error_str:
                print(f"  [{i:3d}/{len(available_dates)}] ❌ {date_str}: {e}")
    
    print(f"✅ Calculated {len(swap_rates)} rates for {currency}.{start}{end}")
    return swap_rates

def simplified_swap_rate_fixed_date(fixed_start_date, tenor, currency: str = 'aud'):
    """
    Calculate swap rates with a fixed start date using pre-loaded core bundles
    Only includes dates that are <= fixed_start_date
    
    Args:
        fixed_start_date: Fixed start date for the swap (datetime object)
        tenor: Tenor from fixed start date (e.g., '1y')
        currency: Currency code (including template, e.g., 'aud', 'audxc', 'audbs')
    
    Returns:
        dict: Dictionary of {date: rate} pairs
    """
    if currency not in CURRENCY_CONFIG:
        raise ValueError(f"Unsupported currency: {currency}")
    
    # Get configuration directly from CURRENCY_CONFIG
    config = CURRENCY_CONFIG[currency]
    swap_rates = {}
    
    # Get all available dates from the simplified cache
    available_dates = get_available_dates()
    
    if not available_dates:
        print("❌ No core bundles loaded. Run initialize_curves_simplified() first.")
        return {}
    
    # Convert fixed start date to Excel date for xcurves
    excel_epoch = datetime(1900, 1, 1)
    fixed_start_excel = str((fixed_start_date - excel_epoch).days + 2)
    
    # Calculate fixed end date
    fixed_end_excel = str(xc.DateAdd(fixed_start_excel, tenor, "syb"))
    
    print(f"🔍 Calculating {currency} fixed-date swap ({fixed_start_date.strftime('%Y-%m-%d')}.{tenor}) for {len(available_dates)} dates")
    
    for i, date_str in enumerate(available_dates, 1):
        try:
            curve_date = yymmdd_to_datetime(date_str)
            
            # Skip curves where curve date > fixed start date
            if curve_date > fixed_start_date:
                continue
            
            # Get the bundle name for this date
            bundle_name = get_simplified_bundle_name(date_str)
            if not bundle_name:
                continue
            
            # Calculate settlement date for this curve
            curve_date_excel = str(yymmdd_to_excel_date(date_str))
            settlement_date = str(xc.DateAdd(curve_date_excel, "2b", "syb"))
            
            # Use the pre-loaded bundle directly with fixed start and end dates
            rate = xc.StandardSwapParRate(
                bundle_name,
                config['template'],
                settlement_date,
                fixed_start_excel,
                fixed_end_excel,
                config['index']
            )
            
            swap_rates[curve_date] = float(rate) * 100
                
        except Exception as e:
            # Filter out redundant xcurves error messages about curve indices
            error_str = str(e)
            if "does not exist, has the curve been built?" not in error_str:
                print(f"  [{i:3d}/{len(available_dates)}] ❌ {date_str}: {e}")
    
    print(f"✅ Calculated {len(swap_rates)} rates for {currency} fixed-date swap")
    return swap_rates

def get_simplified_swap_data(tenor_syntax: str):
    """
    Parse tenor syntax and return swap rate data using simplified core bundles
    Supports multiple formats:
    1. Standard: 'aud.1y1y' (AUD IRS using AUDIRS-SS template)
    2. Template-specific: 'audxc.1y1y' (AUD cross-currency using AONIA-SOFR template)
    3. Basis swaps: 'aud6s3s.1y1y' (AUD 6s3s basis using AUDBASIS-6X3 template)
    4. Fixed-date swap: 'aud.130526.1y' (fixed start date 13 May 2026, 1y tenor)
    5. Spot spread: 'aud.5y.10y' (10y less 5y, interpreted as aud.0y5y.0y10y)
    6. Forward spread: 'aud.5y5y.10y10y' (10y10y - 5y5y)
    7. Butterfly: 'aud.5y5y.10y10y.20y10y' (2*10y10y - 5y5y - 20y10y)
    """
    try:
        # Check if simplified curves are loaded
        if not is_simplified_curves_loaded():
            return None, "Simplified curves not loaded. Run initialize_curves_simplified() first."
        
        parts = tenor_syntax.lower().split('.')
        
        if len(parts) == 2:
            currency, tenor = parts
            
            if currency not in CURRENCY_CONFIG:
                raise ValueError(f"Unsupported currency: {currency}. Supported: {list(CURRENCY_CONFIG.keys())}")
            
            # Check if it's a simple outright (e.g., "10y") or forward (e.g., "5y5y")
            if re.match(r'^\d+[ymd]$', tenor):
                # Simple outright: aud.10y -> start="0y", end="10y" (spot-10y rate)
                start = "0y"
                end = tenor
                print(f"🔍 Parsed simple outright: {currency}.{tenor} -> start='{start}', end='{end}'")
            elif re.match(r'^\d+[ymd]\d+[ymd]$', tenor):
                # Forward rate: aud.5y5y -> start="5y", end="5y"
                match = re.match(r'^(\d+[ymd])(\d+[ymd])$', tenor)
                if not match:
                    raise ValueError(f"Could not parse tenor: {tenor}")
                start, end = match.groups()
                print(f"🔍 Parsed forward rate: {currency}.{tenor} -> start='{start}', end='{end}'")
            else:
                raise ValueError(f"Invalid tenor format: {tenor}. Use format like 10y, 1y1y, 5y5y, etc.")
            
            # Calculate swap rates using the simplified approach
            rates = simplified_swap_rate(start, end, currency)
            if not rates:
                return None, "No swap rates calculated"
            
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(list(rates.items()), columns=['Date', 'Rate'])
            df = df.sort_values('Date')
            
            return df, None
            
        elif len(parts) == 3:
            currency, part1, part2 = parts
            
            if currency not in CURRENCY_CONFIG:
                raise ValueError(f"Unsupported currency: {currency}. Supported: {list(CURRENCY_CONFIG.keys())}")
            
            # Check if it's a fixed-date swap: aud.130526.1y (DDMMYY.tenor format)
            if re.match(r'^\d{6}$', part1) and re.match(r'^\d+[ymd]$', part2):
                # Fixed-date swap: aud.130526.1y
                fixed_date_str = part1  # DDMMYY format
                tenor = part2  # e.g., "1y"
                
                # Parse the fixed date (DDMMYY -> datetime)
                try:
                    dd, mm, yy = int(fixed_date_str[:2]), int(fixed_date_str[2:4]), int(fixed_date_str[4:6])
                    # Handle dates starting with 9 as 1990s (90-99 -> 1990-1999)
                    year = 1900 + yy if yy >= 90 else 2000 + yy
                    fixed_start_date = datetime(year, mm, dd)
                except ValueError:
                    raise ValueError(f"Invalid date format: {fixed_date_str}. Use DDMMYY format (e.g., 130526 for 13 May 2026)")
                
                # Calculate fixed-date swap rates using simplified approach
                rates = simplified_swap_rate_fixed_date(fixed_start_date, tenor, currency)
                if not rates:
                    return None, "No swap rates calculated"
                
                # Convert to DataFrame for easier handling
                df = pd.DataFrame(list(rates.items()), columns=['Date', 'Rate'])
                df = df.sort_values('Date')
                
                return df, None
            
            else:
                # Spread format: aud.5y5y.10y10y (10y10y - 5y5y) or aud.5y.10y (10y - 5y, spot starting)
                tenor1, tenor2 = part1, part2
                
                # Convert simple tenors to outright format if needed
                # aud.5y.10y -> aud.0y5y.0y10y
                if re.match(r'^\d+[ymd]$', tenor1):
                    tenor1_formatted = f"0y{tenor1}"
                else:
                    tenor1_formatted = tenor1
                    
                if re.match(r'^\d+[ymd]$', tenor2):
                    tenor2_formatted = f"0y{tenor2}"
                else:
                    tenor2_formatted = tenor2
                
                # Get data for both tenors
                df1, error1 = get_simplified_swap_data(f"{currency}.{tenor1_formatted}")
                if error1:
                    return None, error1
                
                df2, error2 = get_simplified_swap_data(f"{currency}.{tenor2_formatted}")
                if error2:
                    return None, error2
                
                # Calculate spread (tenor2 - tenor1)
                merged = pd.merge(df1, df2, on='Date', suffixes=('_1', '_2'))
                merged['Rate'] = merged['Rate_2'] - merged['Rate_1']
                
                result_df = merged[['Date', 'Rate']].sort_values('Date')
                return result_df, None
            
        elif len(parts) == 4:
            # Butterfly format: aud.5y5y.10y10y.20y10y (2*10y10y - 5y5y - 20y10y)
            currency, tenor1, tenor2, tenor3 = parts
            
            if currency not in CURRENCY_CONFIG:
                raise ValueError(f"Unsupported currency: {currency}. Supported: {list(CURRENCY_CONFIG.keys())}")
            
            # Get data for all three tenors
            df1, error1 = get_simplified_swap_data(f"{currency}.{tenor1}")
            if error1:
                return None, error1
            
            df2, error2 = get_simplified_swap_data(f"{currency}.{tenor2}")
            if error2:
                return None, error2
                
            df3, error3 = get_simplified_swap_data(f"{currency}.{tenor3}")
            if error3:
                return None, error3
            
            # Calculate butterfly (2*tenor2 - tenor1 - tenor3)
            merged = pd.merge(df1, df2, on='Date', suffixes=('_1', '_2'))
            merged = pd.merge(merged, df3, on='Date')
            merged = merged.rename(columns={'Rate': 'Rate_3'})
            
            merged['Rate'] = 2 * merged['Rate_2'] - merged['Rate_1'] - merged['Rate_3']
            
            result_df = merged[['Date', 'Rate']].sort_values('Date')
            return result_df, None
            
        else:
            raise ValueError("Invalid syntax. Use format: currency.tenor (e.g., aud.1y1y, audxc.1y1y), currency.tenor1.tenor2 (spread), or currency.tenor1.tenor2.tenor3 (butterfly)")
        
    except Exception as e:
        return None, str(e)

# Convenience functions to maintain compatibility with existing code
def initialize_curves_simplified_default(max_days: int = 200):
    """Initialize simplified curves with default settings"""
    return initialize_curves_simplified(max_days)

def get_simplified_status():
    """Get status of simplified curves"""
    stats = get_simplified_cache_stats()
    return {
        'loaded': stats['loaded'],
        'bundle_count': stats['bundle_count'],
        'date_range': stats['date_range'],
        'available_dates_sample': stats.get('sample_dates', [])
    }
