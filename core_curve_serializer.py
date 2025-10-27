import os
import glob
import re
from datetime import datetime, timedelta
import pandas as pd
from cba.analytics import xcurves as xc
import threading
import time
import sys

# Add the printing_scripts directory to the path to import serializers
script_dir = os.path.dirname(os.path.abspath(__file__))
printing_scripts_path = os.path.join(script_dir, '..', 'printing_scripts')
if printing_scripts_path not in sys.path:
    sys.path.append(printing_scripts_path)

# Currency configuration matching the initialize_curves function
CURRENCY_CONFIG = {
    'aud': {
        'folder': 'aud_curves',
        'curve_name': 'aud.primary'
    },
    'usd': {
        'folder': 'usd_curves', 
        'curve_name': 'usd.sofr.primary'
    },
    'eur': {
        'folder': 'eur_curves',
        'curve_name': 'eur.primary'
    },
    'gbp': {
        'folder': 'gbp_curves',
        'curve_name': 'gbp.sonia.primary'
    },
    'cad': {
        'folder': 'cad_curves',
        'curve_name': 'cad.curve.primary'
    },
    'jpy': {
        'folder': 'jpy_curves',
        'curve_name': 'jpy.tonar.primary'
    },
    'nzd': {
        'folder': 'nzd_curves',
        'curve_name': 'nzd.bkbm.primary'
    }
}

def yymmdd_to_datetime(date_str: str) -> datetime:
    """Convert YYMMDD to datetime object"""
    yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
    # Handle dates starting with 9 as 1990s (90-99 -> 1990-1999)
    year = 1900 + yy if yy >= 90 else 2000 + yy
    return datetime(year, mm, dd)

def datetime_to_yymmdd(date_obj: datetime) -> str:
    """Convert datetime object to YYMMDD string"""
    return date_obj.strftime('%y%m%d')

def get_all_dates(currencies: list, max_days: int = 200):
    """
    Get all dates across all specified currencies (union of all date sets)
    
    Args:
        currencies: List of currency codes
        max_days: Maximum number of days to process
    
    Returns:
        list: List of date strings (YYMMDD) sorted by date (most recent first)
    """
    print(f"🔍 Finding all dates across currencies: {', '.join(currencies).upper()}")
    
    # Get all available dates for each currency
    currency_dates = {}
    all_dates = set()
    
    for currency in currencies:
        if currency not in CURRENCY_CONFIG:
            print(f"⚠️ Skipping unsupported currency: {currency}")
            continue
            
        config = CURRENCY_CONFIG[currency]
        # When script is in the_dash/, currency folders are ../{currency}_curves
        folder_path = os.path.join(script_dir, '..', config['folder'])
        
        if not os.path.exists(folder_path):
            print(f"❌ Folder not found: {folder_path}")
            continue
        
        curve_files = glob.glob(os.path.join(folder_path, "*.json"))
        
        # Extract dates for this currency
        dates = set()
        for filepath in curve_files:
            filename = os.path.basename(filepath)
            try:
                date_str = filename[:6]
                dates.add(date_str)
            except:
                continue
        
        currency_dates[currency] = dates
        all_dates.update(dates)  # Add to union of all dates
        print(f"  {currency.upper()}: {len(dates)} dates available")
    
    # Use union of all dates
    if not all_dates:
        return []
    
    print(f"📅 Found {len(all_dates)} total unique dates across all currencies")
    
    # Convert to datetime objects, sort by date (most recent first), and take max_days
    date_objects = []
    for date_str in all_dates:
        try:
            date_obj = yymmdd_to_datetime(date_str)
            date_objects.append((date_obj, date_str))
        except:
            continue
    
    # Sort by date (most recent first) and take max_days
    date_objects.sort(key=lambda x: x[0], reverse=True)
    selected_dates = [date_str for _, date_str in date_objects[:max_days]]
    
    print(f"🎯 Selected {len(selected_dates)} most recent dates")
    if selected_dates:
        oldest_date = yymmdd_to_datetime(selected_dates[-1])
        newest_date = yymmdd_to_datetime(selected_dates[0])
        print(f"📊 Date range: {oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')}")
    
    return selected_dates

def load_all_curves(currencies: list, dates: list):
    """
    Load all curves for all currencies and dates into memory
    Following the pattern: for date in dates: for currency in currencies: curve = deserialise
    
    Args:
        currencies: List of currency codes
        dates: List of date strings (YYMMDD)
    
    Returns:
        dict: curves[currency][date] = curve_name
    """  
    curves = {}
    total_operations = len(dates) * len(currencies)
    current_operation = 0
    
    # Initialize curves dictionary
    for currency in currencies:
        curves[currency] = {}
    
    # First loop: for date in dates:
    for date_str in dates:
        print(f"\n📅 Processing date: {date_str}")
        
        # Second loop: for currency in currencies:
        for currency in currencies:
            current_operation += 1
            
            if currency not in CURRENCY_CONFIG:
                print(f"  ⚠️ [{current_operation:3d}/{total_operations}] Skipping unsupported currency: {currency}")
                continue
            
            config = CURRENCY_CONFIG[currency]
            
            try:
                # Build filename
                filename = f"{date_str}_{currency}_curve.json"
                # When script is in the_dash/, currency folders are ../{currency}_curves
                filepath = os.path.join(script_dir, '..', config['folder'], filename)
                
                if not os.path.exists(filepath):
                    print(f"  ❌ [{current_operation:3d}/{total_operations}] File not found: {filename}")
                    continue
                
                # Deserialize the curve (this loads it into xcurves memory)
                curve_name = f"{date_str}_{currency}_curve"  # Use unique curve name
                xc.Deserialise(filepath, curve_name, True, True)
                
                # Store in curves dictionary
                curves[currency][date_str] = curve_name
                
                print(f"  ✅ [{current_operation:3d}/{total_operations}] Loaded {currency.upper()}: {filename}")
                
            except Exception as e:
                print(f"  ❌ [{current_operation:3d}/{total_operations}] Error loading {currency.upper()} {date_str}: {e}")
                continue
    
    # Summary
    total_loaded = sum(len(curves[currency]) for currency in curves)
    print(f"\n📊 Curve Loading Summary:")
    for currency in currencies:
        loaded_count = len(curves[currency])
        print(f"  {currency.upper()}: {loaded_count}/{len(dates)} curves loaded")
    print(f"  TOTAL: {total_loaded}/{total_operations} curves loaded")
    
    return curves

def build_core_bundles(curves: dict, dates: list, currencies: list):
    """
    Build block bundles for each date using all available currencies
    Following the pattern from initialize_curves
    
    Args:
        curves: Dictionary of loaded curves
        dates: List of date strings (YYMMDD)
        currencies: List of currency codes
    
    Returns:
        dict: Processing results
    """
    print(f"\n🔗 Building core bundles for each date...")
    print("=" * 60)
    
    # Create core_curves directory relative to app.py location
    # When script is in the_dash/, core_curves should be ../core_curves from there
    core_curves_dir = os.path.join(script_dir, '..', 'core_curves')
    os.makedirs(core_curves_dir, exist_ok=True)
    
    success_count = 0
    error_count = 0
    processed_bundles = []
    
    # Process each date
    for i, date_str in enumerate(dates, 1):
        try:           
            # Get curve set for this date: curves[:, date]
            currency_list = []
            curve_names = []
            
            # Include all currencies that have a curve for this date
            for currency in currencies:
                if currency in curves and date_str in curves[currency]:
                    currency_list.append(currency[:3].upper())  # 3 letters uppercase
                    curve_names.append(curves[currency][date_str])
            
            if not currency_list:
                print(f"  ⚠️ No curves available for date {date_str}")
                error_count += 1
                continue
            
           
            currency_curve_pairs = []
            for j in range(len(currency_list)):
                currency_curve_pairs.append([currency_list[j], curve_names[j]])
            
            fx_pair_rates = [["AUDUSD", "1"], ["EURUSD", "1"], ["USDJPY", "1"], ["USDCAD", "1"], ["NZDUSD", "1"], ["GBPUSD", "1"]]
            
            # Build the block bundle
            bundle_name = f"{date_str}_core"
            xc.BuildBlockBundle(bundle_name, currency_curve_pairs, fx_pair_rates)
            
            # Serialize the core bundle
            core_filename = f"{date_str}_core_bundle.json"
            core_filepath = os.path.join(core_curves_dir, core_filename)
            xc.Serialise(bundle_name, core_filepath, True)
            
            print(f"  💾 Saved core bundle: {core_filename}")
            
            processed_bundles.append({
                'date': date_str,
                'bundle_name': bundle_name,
                'currencies': currency_list,
                'filename': core_filename,
                'status': 'success'
            })
            success_count += 1
            
        except Exception as e:
            print(f"  ❌ Error building bundle for {date_str}: {e}")
            processed_bundles.append({
                'date': date_str,
                'bundle_name': f"{date_str}_core",
                'currencies': [],
                'filename': None,
                'status': 'error',
                'error': str(e)
            })
            error_count += 1
    
    # Summary
    total = success_count + error_count
    success_rate = (success_count / total * 100) if total > 0 else 0
    
    print(f"\n📊 Bundle Building Summary:")
    print(f"  ✅ Success: {success_count}/{total} ({success_rate:.1f}%)")
    print(f"  ❌ Errors:  {error_count}/{total}")
    print(f"  💾 Core bundles saved to: {core_curves_dir}")
    
    return {
        'success_count': success_count,
        'error_count': error_count,
        'success_rate': success_rate,
        'bundles': processed_bundles
    }

def process_core_curves(currencies: list = None, max_days: int = 200):
    """
    Main function to process core curves following the correct pattern:
    1. Load all curves for all currencies and dates
    2. Build block bundles for each date
    
    Args:
        currencies: List of currency codes (default: all available)
        max_days: Maximum number of days to process
    
    Returns:
        dict: Processing results
    """
    if currencies is None:
        currencies = list(CURRENCY_CONFIG.keys())
    
    print("=" * 70)
    print(f"📋 Currencies: {', '.join(currencies).upper()}")
    print(f"📅 Max days: {max_days}")
    print("=" * 70)
    
    # Step 1: Find all dates across all currencies
    dates = get_all_dates(currencies, max_days)
    
    # Step 2: Load all curves into memory
    curves = load_all_curves(currencies, dates)
    
    # Step 3: Build core bundles for each date
    bundle_results = build_core_bundles(curves, dates, currencies)
    
    # Overall summary
    print("\n" + "=" * 70)
    print("📊 OVERALL PROCESSING SUMMARY")
    print("=" * 70)
    print(f"Currencies processed: {', '.join(currencies).upper()}")
    print(f"Dates processed: {len(dates)}")
    print(f"Core bundles created: {bundle_results['success_count']}")
    print(f"Errors: {bundle_results['error_count']}")
    print(f"Success rate: {bundle_results['success_rate']:.1f}%")
    print(f"Output directory: {os.path.join(script_dir, '..', 'core_curves')}")
    print("=" * 70)
    
    return {
        'status': 'completed',
        'currencies': currencies,
        'dates_processed': len(dates),
        'bundle_results': bundle_results
    }

def list_available_curves():
    """List available curves for each currency"""
    print("📋 Available Curves by Currency:")
    print("=" * 50)
    
    for currency in CURRENCY_CONFIG.keys():
        config = CURRENCY_CONFIG[currency]
        # When script is in the_dash/, currency folders are ../{currency}_curves
        folder_path = os.path.join(script_dir, '..', config['folder'])
        
        if not os.path.exists(folder_path):
            print(f"{currency.upper():4s}: Folder not found")
            continue
            
        curve_files = glob.glob(os.path.join(folder_path, "*.json"))
        
        if curve_files:
            # Extract dates and find range
            dates = []
            for filepath in curve_files:
                filename = os.path.basename(filepath)
                try:
                    date_str = filename[:6]
                    date_obj = yymmdd_to_datetime(date_str)
                    dates.append(date_obj)
                except:
                    continue
            
            if dates:
                dates.sort()
                oldest_date = dates[0]
                newest_date = dates[-1]
                print(f"{currency.upper():4s}: {len(dates):4d} files ({oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')})")
            else:
                print(f"{currency.upper():4s}: No valid date files found")
        else:
            print(f"{currency.upper():4s}: No files found")

def get_most_recent_core_bundle():
    """
    Find the most recent core curve bundle in the core_curves directory
    
    Returns:
        tuple: (date_str, filepath) of the most recent bundle, or (None, None) if none found
    """
    core_curves_dir = os.path.join(script_dir, '..', 'core_curves')
    
    if not os.path.exists(core_curves_dir):
        print(f"❌ Core curves directory not found: {core_curves_dir}")
        return None, None
    
    # Find all core bundle files
    bundle_files = glob.glob(os.path.join(core_curves_dir, "*_core_bundle.json"))
    
    if not bundle_files:
        print(f"❌ No core bundle files found in: {core_curves_dir}")
        return None, None
    
    # Extract dates and find the most recent
    bundle_dates = []
    for filepath in bundle_files:
        filename = os.path.basename(filepath)
        try:
            date_str = filename[:6]  # Extract YYMMDD
            date_obj = yymmdd_to_datetime(date_str)
            bundle_dates.append((date_obj, date_str, filepath))
        except:
            continue
    
    if not bundle_dates:
        print(f"❌ No valid core bundle dates found")
        return None, None
    
    # Sort by date (most recent first)
    bundle_dates.sort(key=lambda x: x[0], reverse=True)
    most_recent_date_obj, most_recent_date_str, most_recent_filepath = bundle_dates[0]
    
    print(f"📅 Most recent core bundle: {most_recent_date_str} ({most_recent_date_obj.strftime('%Y-%m-%d')})")
    print(f"📁 File: {os.path.basename(most_recent_filepath)}")
    
    return most_recent_date_str, most_recent_filepath

def get_missing_core_bundle_dates():
    """
    Find dates that exist in individual currency curves but not in core_curves
    
    Returns:
        set: Set of date strings (YYMMDD) that need core bundles created
    """
    print("🔍 Finding dates that need core bundles...")
    
    # Get all dates from individual currency folders
    all_currency_dates = set()
    currencies = list(CURRENCY_CONFIG.keys())
    
    for currency in currencies:
        config = CURRENCY_CONFIG[currency]
        folder_path = os.path.join(script_dir, '..', config['folder'])
        
        if not os.path.exists(folder_path):
            print(f"  ⚠️ Folder not found: {config['folder']}")
            continue
        
        curve_files = glob.glob(os.path.join(folder_path, "*.json"))
        currency_dates = set()
        
        for filepath in curve_files:
            filename = os.path.basename(filepath)
            try:
                date_str = filename[:6]  # Extract YYMMDD
                currency_dates.add(date_str)
                all_currency_dates.add(date_str)
            except:
                continue
        
        print(f"  {currency.upper()}: {len(currency_dates)} dates found")
    
    print(f"📊 Total unique dates across all currencies: {len(all_currency_dates)}")
    
    # Get existing core bundle dates
    core_curves_dir = os.path.join(script_dir, '..', 'core_curves')
    existing_core_dates = set()
    
    if os.path.exists(core_curves_dir):
        core_files = glob.glob(os.path.join(core_curves_dir, "*_core_bundle.json"))
        for filepath in core_files:
            filename = os.path.basename(filepath)
            try:
                date_str = filename[:6]  # Extract YYMMDD
                existing_core_dates.add(date_str)
            except:
                continue
    
    print(f"📊 Existing core bundle dates: {len(existing_core_dates)}")
    
    # Find missing dates
    missing_dates = all_currency_dates - existing_core_dates
    
    if missing_dates:
        # Sort missing dates (most recent first)
        missing_date_objects = []
        for date_str in missing_dates:
            try:
                date_obj = yymmdd_to_datetime(date_str)
                missing_date_objects.append((date_obj, date_str))
            except:
                continue
        
        missing_date_objects.sort(key=lambda x: x[0], reverse=True)
        sorted_missing_dates = [date_str for _, date_str in missing_date_objects]
        
        print(f"🎯 Missing core bundle dates: {len(missing_dates)}")
        if sorted_missing_dates:
            newest_missing = yymmdd_to_datetime(sorted_missing_dates[0])
            oldest_missing = yymmdd_to_datetime(sorted_missing_dates[-1])
            print(f"📅 Date range: {oldest_missing.strftime('%Y-%m-%d')} to {newest_missing.strftime('%Y-%m-%d')}")
        
        return set(sorted_missing_dates)
    else:
        print("✅ No missing core bundle dates found - all dates are up to date!")
        return set()

def serialize_missing_core_curves():
    """
    Find missing core bundle dates and serialize core curves for those dates
    """
    print("=" * 70)
    print("🎯 SERIALIZING MISSING CORE CURVES")
    print("=" * 70)
    
    # Step 1: Find dates that need core bundles
    missing_dates = get_missing_core_bundle_dates()
    
    if not missing_dates:
        print("✅ All core bundles are up to date!")
        return True
    
    # Convert to list and limit to reasonable number
    missing_dates_list = list(missing_dates)[:50]  # Limit to 50 most recent
    
    print(f"\n🎯 Will create core bundles for {len(missing_dates_list)} dates")
    
    # Ask for confirmation
    response = input(f"\nProceed with creating {len(missing_dates_list)} core bundles? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Operation cancelled by user")
        return False
    
    # Step 2: Load all curves for missing dates
    currencies = list(CURRENCY_CONFIG.keys())
    print(f"\n📂 Loading curves for currencies: {', '.join(currencies).upper()}")
    
    curves = load_all_curves(currencies, missing_dates_list)
    
    # Step 3: Build core bundles for missing dates
    bundle_results = build_core_bundles(curves, missing_dates_list, currencies)
    
    # Summary
    print(f"\n✅ Core curve serialization completed!")
    print(f"📊 {bundle_results['success_count']} core bundles created")
    print(f"❌ {bundle_results['error_count']} errors")
    
    return bundle_results['success_count'] > 0

if __name__ == "__main__":
    # Ask user what they want to do
    print("🎯 Core Curve Serializer Options:")
    print("1. Process all core curves (original functionality)")
    print("2. Serialize missing core curves (find and create missing core bundles)")
    
    choice = input("\nEnter your choice (1 or 2): ").strip()
    
    if choice == "2":
        # Serialize missing core curves
        success = serialize_missing_core_curves()
        if success:
            print(f"\n✅ Missing core curve serialization completed successfully!")
        else:
            print(f"\n❌ Missing core curve serialization failed or was cancelled")
    else:
        # Original functionality
        MAX_DAYS = 10000
        
        print(f"📅 Processing {MAX_DAYS} most recent days\n")
        
        # Show available curves
        list_available_curves()
        
        # Process all currencies with the specified number of days
        results = process_core_curves(max_days=MAX_DAYS)
        
        if results.get('status') == 'completed':
            print(f"\n✅ Processing completed successfully!")
            print(f"📊 {results['bundle_results']['success_count']} core bundles created")
        else:
            print(f"\n❌ Processing failed or was cancelled")
