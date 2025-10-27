import os
import glob
from datetime import datetime, timedelta
from core_curve_serializer import build_core_bundles, yymmdd_to_datetime, datetime_to_yymmdd, CURRENCY_CONFIG

def get_most_recent_core_bundle_date():
    """Find the most recent core bundle date"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    core_curves_dir = os.path.join(script_dir, '..', 'core_curves')
    
    if not os.path.exists(core_curves_dir):
        print(f"❌ Core curves directory not found: {core_curves_dir}")
        return None
    
    # Find all core bundle files
    bundle_files = glob.glob(os.path.join(core_curves_dir, "*_core_bundle.json"))
    
    if not bundle_files:
        print(f"❌ No core bundle files found")
        return None
    
    # Extract dates and find the most recent
    bundle_dates = []
    for filepath in bundle_files:
        filename = os.path.basename(filepath)
        try:
            date_str = filename[:6]  # Extract YYMMDD
            date_obj = yymmdd_to_datetime(date_str)
            bundle_dates.append((date_obj, date_str))
        except:
            continue
    
    if not bundle_dates:
        return None
    
    # Sort by date (most recent first)
    bundle_dates.sort(key=lambda x: x[0], reverse=True)
    most_recent_date_obj, most_recent_date_str = bundle_dates[0]
    
    print(f"📅 Most recent core bundle: {most_recent_date_str} ({most_recent_date_obj.strftime('%Y-%m-%d')})")
    return most_recent_date_str

def get_dates_to_build(start_date_str):
    """Get list of dates from start_date until yesterday"""
    start_date = yymmdd_to_datetime(start_date_str)
    yesterday = datetime.now() - timedelta(days=1)
    
    dates_to_build = []
    current_date = start_date + timedelta(days=1)  # Start from day after most recent
    
    while current_date <= yesterday:
        date_str = datetime_to_yymmdd(current_date)
        dates_to_build.append(date_str)
        current_date += timedelta(days=1)
    
    print(f"📅 Dates to build core bundles for: {len(dates_to_build)} dates")
    if dates_to_build:
        print(f"📊 Date range: {yymmdd_to_datetime(dates_to_build[0]).strftime('%Y-%m-%d')} to {yymmdd_to_datetime(dates_to_build[-1]).strftime('%Y-%m-%d')}")
    
    return dates_to_build

def check_available_curves(dates_to_build):
    """
    Check which single currency curves exist for the dates we want to build
    Returns: curves dict, filtered_dates list, currencies list
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    curves = {}
    currencies = list(CURRENCY_CONFIG.keys())
    
    # Initialize curves dictionary
    for currency in currencies:
        curves[currency] = {}
    
    print(f"🔍 Checking available curves for {len(dates_to_build)} dates...")
    
    # Check each date
    valid_dates = []
    for date_str in dates_to_build:
        has_any_curve = False
        
        for currency in currencies:
            config = CURRENCY_CONFIG[currency]
            filename = f"{date_str}_{currency}_curve.json"
            filepath = os.path.join(script_dir, '..', config['folder'], filename)
            
            if os.path.exists(filepath):
                # Store as key=filename, value=filename (as requested)
                curves[currency][date_str] = filename
                has_any_curve = True
        
        # Only include dates that have at least one currency curve
        if has_any_curve:
            valid_dates.append(date_str)
    
    print(f"📊 Valid dates with at least one curve: {len(valid_dates)}")
    
    # Print summary of available curves per currency
    for currency in currencies:
        available_count = len(curves[currency])
        print(f"  {currency.upper()}: {available_count}/{len(valid_dates)} curves available")
    
    return curves, valid_dates, currencies

def load_curves_for_build_core_bundles(curves, dates, currencies):
    """
    Load curves into xcurves memory and return the format expected by build_core_bundles
    """
    from cba.analytics import xcurves as xc
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    loaded_curves = {}
    
    # Initialize loaded_curves dictionary
    for currency in currencies:
        loaded_curves[currency] = {}
    
    print(f"📂 Loading curves into memory...")
    
    total_loaded = 0
    for date_str in dates:
        print(f"📅 Loading curves for date: {date_str}")
        
        for currency in currencies:
            if currency in curves and date_str in curves[currency]:
                try:
                    config = CURRENCY_CONFIG[currency]
                    filename = curves[currency][date_str]
                    filepath = os.path.join(script_dir, '..', config['folder'], filename)
                    
                    # Create unique curve name for xcurves memory
                    curve_name = f"{date_str}_{currency}_curve"
                    
                    # Load into xcurves memory
                    xc.Deserialise(filepath, curve_name, True, True)
                    
                    # Store the curve name (not filename) for build_core_bundles
                    loaded_curves[currency][date_str] = curve_name
                    
                    print(f"  ✅ Loaded {currency.upper()}: {filename}")
                    total_loaded += 1
                    
                except Exception as e:
                    print(f"  ❌ Error loading {currency.upper()} {date_str}: {e}")
    
    print(f"📊 Total curves loaded: {total_loaded}")
    return loaded_curves

def main():
    print("=" * 70)
    print("🎯 SIMPLE CORE CURVE SERIALIZER")
    print("=" * 70)
    
    # Step 1: Find most recent core bundle date
    most_recent_date = get_most_recent_core_bundle_date()
    if not most_recent_date:
        print("❌ Cannot find most recent core bundle date")
        return
    
    # Step 2: Get dates from most recent until yesterday
    dates_to_build = get_dates_to_build(most_recent_date)
    if not dates_to_build:
        print("✅ No dates to build - already up to date!")
        return
    
    # Step 3: Check which single currency curves exist for those dates
    curves, valid_dates, currencies = check_available_curves(dates_to_build)
    if not valid_dates:
        print("❌ No valid dates with available curves")
        return
    
    print(f"\n🎯 Will build core bundles for {len(valid_dates)} dates")
    print(f"📋 Currencies: {', '.join(currencies).upper()}")
    
    # Ask for confirmation
    response = input(f"\nProceed with building {len(valid_dates)} core bundles? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Operation cancelled by user")
        return
    
    # Step 4: Load curves into xcurves memory
    loaded_curves = load_curves_for_build_core_bundles(curves, valid_dates, currencies)
    
    # Step 5: Call build_core_bundles function
    print(f"\n🔗 Building core bundles...")
    bundle_results = build_core_bundles(loaded_curves, valid_dates, currencies)
    
    # Summary
    print(f"\n✅ Simple core curve serialization completed!")
    print(f"📊 {bundle_results['success_count']} core bundles created")
    print(f"❌ {bundle_results['error_count']} errors")
    print(f"🎯 Success rate: {bundle_results['success_rate']:.1f}%")

if __name__ == "__main__":
    main()
