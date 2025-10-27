import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime, timedelta
import pandas as pd
import printing_scripts.date_fn as date_fn

# Import all curve serializers
from aud_print import aud_curve_serialiser
from usd_print import usd_curve_serialiser
from eur_print import eur_curve_serialiser
from gbp_print import gbp_curve_serialiser
from cad_print import cad_curve_serialiser
from jpy_print import jpy_curve_serialiser
from nzd_print import nzd_curve_serialiser

def get_fx_rate(ccy, date):
    """Get FX rate for a given currency pair and date"""
    try:
        df = blp.bdh(f"{ccy} Curncy", 'PX_LAST', date, date)
        return df.iloc[0, 0] if not df.empty else None
    except:
        return None

def yyyy_mm_dd_to_yymmdd(date_string):
    """Convert YYYY-MM-DD format to YYMMDD format"""
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    return date_obj.strftime('%y%m%d')

def get_business_dates_range(start_date, end_date, calendar):
    """Get all business dates between start_date and end_date (inclusive) for given calendar"""
    dates = []
    
    # Convert to Excel serial numbers (same approach as date_fn.py)
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    excel_epoch = datetime(1899, 12, 30)
    start_serial = (start_dt - excel_epoch).days
    end_serial = (end_dt - excel_epoch).days
    
    # Use the same logic as date_fn.get_dates() but for a range
    current_serial = start_serial
    
    while current_serial <= end_serial:
        # Convert serial to date string
        current_date = excel_epoch + timedelta(days=current_serial)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Use xc.DateAdd to get next business day from current date
        try:
            next_business_day = xc.DateAdd(date_str, "0b", calendar)
            # Convert back to date string to compare
            next_date = excel_epoch + timedelta(days=next_business_day)
            next_date_str = next_date.strftime('%Y-%m-%d')
            
            # If adding 0 business days returns the same date, it's a business day
            if next_date_str == date_str:
                dates.append(date_str)
        except:
            pass
        
        current_serial += 1
    
    return dates

def process_currency(currency, start_date, end_date):
    """
    Process curves for a specific currency between start_date and end_date (inclusive)
    
    Parameters:
    currency (str): Currency code ('usd', 'aud', 'eur', 'gbp', 'cad', 'jpy', 'nzd')
    start_date (str): Start date in YYYY-MM-DD format
    end_date (str): End date in YYYY-MM-DD format
    """
    
    # INPUT VARIABLES - MODIFY THESE AS NEEDED
    # ========================================
    
    # Currency configuration
    currencies_config = {
        'usd': {
            'pair': None,
            'bundle': None,
            'serializer': usd_curve_serialiser,
            'curve_name': 'usd.sofr.primary',
            'calendar': 'nyc',
            'folder_path': os.path.join('..', '..', 'usd_curves')
        },
        'aud': {
            'pair': 'audusd',
            'bundle': 'AUDUSD_BUNDLE',
            'serializer': aud_curve_serialiser,
            'curve_name': 'aud.primary',
            'calendar': 'syb',
            'folder_path': os.path.join('..', '..', 'aud_curves'),
            'invert_fx': False
        },
        'eur': {
            'pair': 'eurusd', 
            'bundle': 'EURUSD_BUNDLE',
            'serializer': eur_curve_serialiser,
            'curve_name': 'eur.primary',
            'calendar': 'tgt',
            'folder_path': os.path.join('..', '..', 'eur_curves'),
            'invert_fx': False
        },
        'gbp': {
            'pair': 'gbpusd',
            'bundle': 'GBPUSD_BUNDLE', 
            'serializer': gbp_curve_serialiser,
            'curve_name': 'gbp.sonia.primary',
            'calendar': 'lnb',
            'folder_path': os.path.join('..', '..', 'gbp_curves'),
            'invert_fx': False
        },
        'cad': {
            'pair': 'usdcad',
            'bundle': 'USDCAD_BUNDLE',
            'serializer': cad_curve_serialiser,
            'curve_name': 'cad.curve.primary',
            'calendar': 'trb',
            'folder_path': os.path.join('..', '..', 'cad_curves'),
            'invert_fx': True
        },
        'jpy': {
            'pair': 'usdjpy',
            'bundle': 'USDJPY_BUNDLE',
            'serializer': jpy_curve_serialiser,
            'curve_name': 'jpy.tonar.primary',
            'calendar': 'tkb',
            'folder_path': os.path.join('..', '..', 'jpy_curves'),
            'invert_fx': True
        },
        'nzd': {
            'pair': 'nzdusd',
            'bundle': 'NZDUSD_BUNDLE',
            'serializer': nzd_curve_serialiser,
            'curve_name': 'nzd.bkbm.primary',
            'calendar': 'aub,web',
            'folder_path': os.path.join('..', '..', 'nzd_curves'),
            'invert_fx': False
        }
    }
    
    # Validation settings
    REQUIRE_USER_CONFIRMATION = True  # Set to False to skip confirmation prompts
    VERBOSE_OUTPUT = True             # Set to False for less detailed output
    STOP_ON_ERROR = False            # Set to True to stop processing on first error
    
    # File naming settings
    FILE_EXTENSION = '.json'
    OVERWRITE_EXISTING = True        # Set to False to skip existing files
    
    # ========================================
    # END OF INPUT VARIABLES
    # ========================================
    
    # Validate inputs
    currency = currency.lower()
    if currency not in currencies_config:
        print(f"Error: Unsupported currency '{currency}'. Supported currencies: {list(currencies_config.keys())}")
        return False
    
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format")
        return False
    
    if start_date > end_date:
        print("Error: Start date must be before or equal to end date")
        return False
    
    # Get currency configuration
    config = currencies_config[currency]
    
    # Create output folder if it doesn't exist
    os.makedirs(config['folder_path'], exist_ok=True)
    
    # Get business dates for the specified range
    if VERBOSE_OUTPUT:
        print(f"\n{'='*60}")
        print(f"PROCESSING {currency.upper()} CURRENCY")
        print(f"{'='*60}")
        print(f"Calendar: {config['calendar']}")
        print(f"Start date: {start_date}")
        print(f"End date: {end_date}")
        print(f"Output folder: {config['folder_path']}")
    
    try:
        dates = get_business_dates_range(start_date, end_date, config['calendar'])
    except Exception as e:
        print(f"Error getting business dates: {e}")
        return False
    
    if not dates:
        print(f"No business dates found between {start_date} and {end_date} for calendar {config['calendar']}")
        return False
    
    if VERBOSE_OUTPUT:
        print(f"Total business dates to process: {len(dates)}")
        print(f"Date range: {dates[0]} to {dates[-1]}")
    
    # Ask for confirmation if required
    if REQUIRE_USER_CONFIRMATION:
        response = input(f"\nProceed with {currency.upper()} curve processing for {len(dates)} dates? (y/n): ").strip().lower()
        if response != 'y':
            print(f"Skipping {currency.upper()} processing...")
            return False
    
    # Process each date
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    for i, date in enumerate(dates):
        try:
            if VERBOSE_OUTPUT:
                print(f"\n--- {currency.upper()} {date} ({i+1}/{len(dates)}) ---")
            
            # Check if file already exists
            formatted_date = yyyy_mm_dd_to_yymmdd(date)
            filename = f"{formatted_date}_{currency}_curve{FILE_EXTENSION}"
            filepath = os.path.join(config['folder_path'], filename)
            
            if os.path.exists(filepath) and not OVERWRITE_EXISTING:
                if VERBOSE_OUTPUT:
                    print(f"⚠ Skipping existing file: {filename}")
                skipped_count += 1
                continue
            
            # Special handling for USD (base currency)
            if currency == 'usd':
                if VERBOSE_OUTPUT:
                    print("Building USD curve...")
                usd_curve = config['serializer'](date)
                
            else:
                # For other currencies, we need USD curve available
                if VERBOSE_OUTPUT:
                    print(f"Building {currency.upper()} curve...")
                
                # Check if USD curve exists for this date
                usd_filename = f"{formatted_date}_usd_curve.json"
                usd_filepath = os.path.join("..", "..", "usd_curves", usd_filename)
                
                if not os.path.exists(usd_filepath):
                    raise ValueError(f"USD curve not found for {date}: {usd_filepath}")
                
                # Get FX rate
                if config.get('invert_fx', False):
                    raw_fx = get_fx_rate(config['pair'], date)
                    if raw_fx is None or raw_fx == 0:
                        raise ValueError(f"Invalid FX rate for {config['pair']}: {raw_fx}")
                    fx_rate = str(1/raw_fx)
                else:
                    fx_rate = get_fx_rate(config['pair'], date)
                    if fx_rate is None:
                        raise ValueError(f"Could not get FX rate for {config['pair']}")
                    fx_rate = str(fx_rate)
                
                if VERBOSE_OUTPUT:
                    print(f"FX Rate ({config['pair']}): {fx_rate}")
                
                # Load USD curve
                usd_curve = xc.Deserialise(usd_filepath, "usd.sofr.primary", True)
                
                # Build currency bundle
                currency_bundle = xc.BuildBlockBundle(
                    config['bundle'], 
                    [["USD", "usd.sofr.primary"]], 
                    [[config['pair'].upper(), fx_rate]]
                )
                
                # Build currency curve
                currency_curve = config['serializer'](date)
            
            # Serialize curve
            xc.Serialise(config['curve_name'], filepath, True)
            
            success_count += 1
            if VERBOSE_OUTPUT:
                print(f"✓ {currency.upper()} curve saved: {filename}")
            
        except Exception as e:
            error_count += 1
            error_msg = f"✗ Error creating {currency.upper()} curve for {date}: {e}"
            print(error_msg)
            
            if STOP_ON_ERROR:
                print("Stopping processing due to error (STOP_ON_ERROR = True)")
                break
            
            continue
    
    # Print summary
    total_processed = success_count + error_count
    success_rate = (success_count / total_processed * 100) if total_processed > 0 else 0
    
    print(f"\n{currency.upper()} PROCESSING SUMMARY:")
    print(f"  Success:  {success_count}/{len(dates)} ({success_rate:.1f}%)")
    print(f"  Errors:   {error_count}/{len(dates)}")
    print(f"  Skipped:  {skipped_count}/{len(dates)}")
    print(f"  Total:    {len(dates)} business dates")
    
    return success_count > 0

if __name__ == "__main__":
    # MAIN EXECUTION VARIABLES - MODIFY THESE TO RUN THE SCRIPT
    # =========================================================
    
    # Currency to process (change this to the currency you want)
    CURRENCY = "eur"  # Options: 'usd', 'aud', 'eur', 'gbp', 'cad', 'jpy', 'nzd'
    
    # Date range (change these to your desired date range)
    START_DATE = "2024-10-11"  # Format: YYYY-MM-DD (use historical date with data)
    END_DATE = "2024-10-11"    # Format: YYYY-MM-DD
    
    # =========================================================
    # END OF MAIN EXECUTION VARIABLES
    # =========================================================
    
    print("=== Currency Curve Patch Processing ===")
    print(f"Currency: {CURRENCY.upper()}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    
    success = process_currency(CURRENCY, START_DATE, END_DATE)
    
    if success:
        print(f"\n✓ {CURRENCY.upper()} curve processing completed successfully!")
    else:
        print(f"\n✗ {CURRENCY.upper()} curve processing failed or was cancelled.")
