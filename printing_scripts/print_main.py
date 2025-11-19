import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))
import date_fn
import traceback

# Import all curve serializers
from aud_print import aud_curve_serialiser
from usd_print import usd_curve_serialiser
from eur_print import eur_curve_serialiser
from gbp_print import gbp_curve_serialiser
from cad_print import cad_curve_serialiser
from jpy_print import jpy_curve_serialiser
from nzd_print import nzd_curve_serialiser

def get_fx_rate(ccy, date):
    try:
        df = blp.bdh(f"{ccy} Curncy", 'PX_LAST', date, date)
        return df.iloc[0, 0] if not df.empty else None
    except:
        return None

def yyyy_mm_dd_to_yymmdd(date_string):
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    return date_obj.strftime('%y%m%d')

def create_curve_folders():
    """Create folders for all currency curves"""
    currencies = ['usd', 'aud', 'eur', 'gbp', 'cad', 'jpy', 'nzd']
    base_path = os.path.join("..")  # Parent directory (the_dash)
    
    for currency in currencies:
        folder_path = os.path.join(base_path, f"{currency}_curves")
        os.makedirs(folder_path, exist_ok=True)
        print(f"Created/verified folder: {folder_path}")

def process_currency(ccy, config, dates, latest_date, current_date):
    """Process a single currency for all its business dates"""
    print(f"\n{'='*60}")
    print(f"PROCESSING {ccy.upper()} CURRENCY")
    print(f"{'='*60}")
    print(f"Calendar: {config['calendar']}")
    print(f"Latest existing date: {latest_date}")
    print(f"Current date: {current_date}")
    print(f"Total dates to process: {len(dates)}")
    print(f"Date range: {dates[0] if dates else 'N/A'} to {dates[-1] if dates else 'N/A'}")
    print(f"Auto-proceeding with {ccy.upper()} curve serialization...")
    
    success_count = 0
    error_count = 0
    
    for i, date in enumerate(dates):
        try:
            print(f"\n--- {ccy.upper()} {date} ({i+1}/{len(dates)}) ---")
            print(f"DEBUG: Processing date: {date} (type: {type(date)})")
            
            # Special handling for USD (base currency)
            if ccy == 'usd':
                print("Building USD curve...")
                print(f"DEBUG: About to call usd_curve_serialiser with date: {date} (type: {type(date)})")
                try:
                    usd_curve = config['serializer'](date)
                    print(f"DEBUG: Successfully called usd_curve_serialiser")
                except Exception as serializer_error:
                    print(f"DEBUG: Error in usd_curve_serialiser: {serializer_error}")
                    print("DEBUG: Full traceback:")
                    traceback.print_exc()
                    raise serializer_error
                
            else:
                # For other currencies, we need USD curve available
                # But we build each currency independently on its own business days
                print(f"Building {ccy.upper()} curve...")
                
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
                
                print(f"FX Rate ({config['pair']}): {fx_rate}")
                
                formatted_date = yyyy_mm_dd_to_yymmdd(date)
              
                usd_curve = xc.Deserialise(os.path.join("..", "usd_curves", formatted_date + "_usd_curve.json"), "usd.sofr.primary", True)

                # Build currency bundle (this will reference USD curve)
                currency_bundle = xc.BuildBlockBundle(
                    config['bundle'], 
                    [["USD", "usd.sofr.primary"]], 
                    [[config['pair'].upper(), fx_rate]]
                )
                
                # Build currency curve
                print(f"DEBUG: About to call {ccy}_curve_serialiser with date: {date} (type: {type(date)})")
                try:
                    currency_curve = config['serializer'](date)
                    print(f"DEBUG: Successfully called {ccy}_curve_serialiser")
                except Exception as serializer_error:
                    print(f"DEBUG: Error in {ccy}_curve_serialiser: {serializer_error}")
                    print("DEBUG: Full traceback:")
                    traceback.print_exc()
                    raise serializer_error
            
            # Format date and create filename
            formatted_date = yyyy_mm_dd_to_yymmdd(date)
            filename = f"{formatted_date}_{ccy}_curve.json"
            filepath = os.path.join("..", f"{ccy}_curves", filename)
            
            # Serialize curve
            xc.Serialise(config['curve_name'], filepath, True)
            
            success_count += 1
            print(f"✓ {ccy.upper()} curve saved: {filename}")
            
        except Exception as e:
            print(f"✗ Error creating {ccy.upper()} curve for {date}: {e}")
            print("DEBUG: Full traceback for main exception:")
            traceback.print_exc()
            error_count += 1
            continue
    
    # Currency summary
    total = success_count + error_count
    success_rate = (success_count / total * 100) if total > 0 else 0
    print(f"\n{ccy.upper()} SUMMARY:")
    print(f"  Success: {success_count}/{total} ({success_rate:.1f}%)")
    print(f"  Errors:  {error_count}/{total}")
    
    return success_count, error_count

def main():
    """Main function for comprehensive multi-currency curve update"""
    print("=== Starting Comprehensive Multi-Currency Curve Update ===")
    
    # Create folders if they don't exist
    create_curve_folders()
    
    # Currency configuration with their specific calendars
    currencies = {
        'usd': {
            'pair': None,
            'bundle': None,
            'serializer': usd_curve_serialiser,
            'curve_name': 'usd.sofr.primary',
            'calendar': 'nyc'
        },
        'aud': {
            'pair': 'audusd',
            'bundle': 'AUDUSD_BUNDLE',
            'serializer': aud_curve_serialiser,
            'curve_name': 'aud.primary',
            'calendar': 'syb'
        },
        'eur': {
            'pair': 'eurusd', 
            'bundle': 'EURUSD_BUNDLE',
            'serializer': eur_curve_serialiser,
            'curve_name': 'eur.primary',
            'calendar': 'tgt'
        },
        'gbp': {
            'pair': 'gbpusd',
            'bundle': 'GBPUSD_BUNDLE', 
            'serializer': gbp_curve_serialiser,
            'curve_name': 'gbp.sonia.primary',
            'calendar': 'lnb'
        },
        'cad': {
            'pair': 'usdcad',
            'bundle': 'USDCAD_BUNDLE',
            'serializer': cad_curve_serialiser,
            'curve_name': 'cad.curve.primary',
            'calendar': 'trb',
            'invert_fx': True
        },
        'jpy': {
            'pair': 'usdjpy',
            'bundle': 'USDJPY_BUNDLE',
            'serializer': jpy_curve_serialiser,
            'curve_name': 'jpy.tonar.primary',
            'calendar': 'tkb',
            'invert_fx': True
        },
        'nzd': {
            'pair': 'nzdusd',
            'bundle': 'NZDUSD_BUNDLE',
            'serializer': nzd_curve_serialiser,
            'curve_name': 'nzd.bkbm.primary',
            'calendar': 'aub,web'
        }
    }
    
    # Process USD first (since other currencies depend on it)
    print("STEP 1: Processing USD (Base Funding Currency)")
    usd_config = currencies['usd']
    
    # Get latest date info for USD
    usd_latest_date, usd_current_date = date_fn.get_most_recent_date_file('usd')
    usd_dates = date_fn.get_dates(usd_config['calendar'], 'usd')
    usd_success, usd_errors = process_currency('usd', usd_config, usd_dates, usd_latest_date, usd_current_date)
    
    # Then process each other currency independently
    print("\nSTEP 2: Processing Other Currencies")
    
    total_stats = {'usd': (usd_success, usd_errors)}
    
    for ccy, config in currencies.items():
        if ccy == 'usd':  # Already processed
            continue
            
        try:
            # Get latest date info for this currency
            latest_date, current_date = date_fn.get_most_recent_date_file(ccy)
            
            # Get business dates specific to this currency
            currency_dates = date_fn.get_dates(config['calendar'], ccy)
            
            if not currency_dates:
                print(f"\nWarning: No dates found for {ccy.upper()}")
                continue
            
            # Process this currency
            success, errors = process_currency(ccy, config, currency_dates, latest_date, current_date)
            total_stats[ccy] = (success, errors)
            
        except Exception as e:
            print(f"\nError processing {ccy.upper()}: {e}")
            total_stats[ccy] = (0, 1)
    
    # Print comprehensive final statistics
    print("\n" + "="*70)
    print("COMPREHENSIVE FINAL STATISTICS")
    print("="*70)
    
    total_success = 0
    total_errors = 0
    
    for ccy, (success, errors) in total_stats.items():
        total = success + errors
        success_rate = (success / total * 100) if total > 0 else 0
        calendar = currencies[ccy]['calendar']
        
        print(f"{ccy.upper():4s} ({calendar:8s}): {success:4d} success, {errors:4d} errors ({success_rate:5.1f}% success)")
        
        total_success += success
        total_errors += errors
    
    overall_total = total_success + total_errors
    overall_rate = (total_success / overall_total * 100) if overall_total > 0 else 0
    
    print("-" * 70)
    print(f"OVERALL TOTALS:        {total_success:4d} success, {total_errors:4d} errors ({overall_rate:5.1f}% success)")
    print(f"Total curve files:     {total_success}")
    print(f"Currencies processed:  {len([ccy for ccy, (s, e) in total_stats.items() if s + e > 0])}")
    print("="*70)
    print("=== Multi-Currency Curve Update Complete ===")

if __name__ == "__main__":
    main()
