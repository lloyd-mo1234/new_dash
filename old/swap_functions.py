import os
import glob
import re
from datetime import datetime, timedelta
import pandas as pd
from cba.analytics import xcurves as xc
import threading
import time
import sys

# Add the printing_scripts directory to the path to import realtime_curves
script_dir = os.path.dirname(os.path.abspath(__file__))
# From the_dash/dashboard/, go up two levels to get to chart_app, then into printing_scripts
chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
printing_scripts_path = os.path.join(chart_app_dir, 'printing_scripts')
if printing_scripts_path not in sys.path:
    sys.path.append(printing_scripts_path)

try:
    import realtime_curves  # type: ignore
    REALTIME_AVAILABLE = True
    print("✅ Real-time curves module imported successfully")
except ImportError as e:
    REALTIME_AVAILABLE = False
    print(f"⚠️ Real-time curves module not available: {e}")

# Global progress tracking for multiple currencies
progress_data = {
    'currencies': {},  # Track progress per currency
    'overall': {
        'current': 0,
        'total': 0,
        'status': 'idle',
        'message': ''
    }
}
progress_lock = threading.Lock()

# Global curves cache - now supports multiple currencies
curves_cache = {}
curves_loaded = {}  # Track which currencies are loaded
curves_lock = threading.Lock()

# Currency configuration with template-embedded currency codes
CURRENCY_CONFIG = {
    # Standard AUD templates
    'aud': {
        'folder': 'aud_curves',
        'template': 'AUDIRS-SS',
        'index': 'AUD 6M'
    },
    'audbs': {
        'folder': 'aud_curves',
        'template': 'BBSW-SOFR',
        'index': 'USD SOFR'
    },
    'audxc': {
        'folder': 'aud_curves',
        'template': 'AONIA-SOFR',
        'index': 'USD SOFR'
    },
    'audbob': {
        'folder': 'aud_curves',
        'template': 'AUDBOB-3M',
        'index': 'AUD 6M'
    },
    'aud6s3s': {
        'folder': 'aud_curves',
        'template': 'AUDBASIS-6X3',
        'index': 'AUD 6M'
    },
    
    # Standard GBP templates
    'gbp': {
        'folder': 'gbp_curves',
        'template': 'GBPOIS',
        'index': 'GBP OIS'
    },
    'gbpxc': {
        'folder': 'gbp_curves',
        'template': 'SONIA-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard USD templates
    'usd': {
        'folder': 'usd_curves',
        'template': 'USDSOFR',
        'index': 'USD SOFR'
    },
    
    # Standard EUR templates
    'eur': {
        'folder': 'eur_curves',
        'template': 'EURIRS-AS',
        'index': 'EUR 6M'
    },
    'eurxc': {
        'folder': 'eur_curves',
        'template': 'ESTR-SOFR',
        'index': 'USD SOFR'
    },
    'eurbob': {
        'folder': 'eur_curves',
        'template': 'EURESTR-EURIBOR3M',
        'index': 'EUR 6M'
    },
    'eur6s3s': {
        'folder': 'eur_curves',
        'template': 'EURBASIS-6X3',
        'index': 'EUR 6M'
    },
    
    # Standard JPY templates
    'jpy': {
        'folder': 'jpy_curves',
        'template': 'JPYOIS',
        'index': 'JPY OIS'
    },
    'jpyxc': {
        'folder': 'jpy_curves',
        'template': 'TONAR-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard CAD templates
    'cad': {
        'folder': 'cad_curves',
        'template': 'CADOIS',
        'index': 'CAD OIS'
    },
    'cadxc': {
        'folder': 'cad_curves',
        'template': 'CORRA-SOFR',
        'index': 'USD SOFR'
    },
    
    # Standard NZD templates
    'nzd': {
        'folder': 'nzd_curves',
        'template': 'NZDIRS-SQ',
        'index': 'NZD 3M'
    },
    'nzdbs': {
        'folder': 'nzd_curves',
        'template': 'BKBM-SOFR',
        'index': 'USD SOFR'
    },
    'nzdxc': {
        'folder': 'nzd_curves',
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

def load_currency_curves(currency: str, max_files: int = 1000):
    """
    Load curves for any currency using 12-way concurrent processing (no verification for maximum speed)
    
    Args:
        currency: Currency code (e.g., 'aud', 'gbp', 'usd')
        max_files: Maximum number of files to load
    
    Returns:
        dict: Dictionary of loaded curves
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    if currency not in CURRENCY_CONFIG:
        raise ValueError(f"Unsupported currency: {currency}. Supported: {list(CURRENCY_CONFIG.keys())}")
    
    config = CURRENCY_CONFIG[currency]
    # Get the directory where this script is located (dashboard folder)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels to get to the main chart_app directory
    chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
    folder_path = os.path.join(chart_app_dir, config['folder'])
    
    # Use the proper date-sorted method to get the most recent files
    recent_filenames = get_last_n_days_files(currency, max_files)
    
    # Convert filenames to full paths
    curve_files = [os.path.join(folder_path, filename) for filename in recent_filenames]
    
    print(f"📁 Found {len(glob.glob(os.path.join(folder_path, '*.json')))} total {currency.upper()} curve files")
    print(f"📅 Selected {len(curve_files)} most recent {currency.upper()} curve files")
    
    # Initialize progress for this currency
    with progress_lock:
        progress_data['currencies'][currency] = {
            'current': 0,
            'total': len(curve_files),
            'status': 'loading',
            'message': f'Loading {currency.upper()} curves with 12 threads...'
        }
    
    # Split files into 12 batches for concurrent processing
    batch_size = len(curve_files) // 12
    if batch_size == 0:
        batch_size = 1
    
    batches = []
    for i in range(0, len(curve_files), batch_size):
        batch = curve_files[i:i + batch_size]
        batches.append(batch)
    
    print(f"🧵 Processing {len(curve_files)} files in {len(batches)} concurrent batches")
    
    all_curves = {}
    completed_files = 0
    
    # Process batches concurrently with 12 threads
    with ThreadPoolExecutor(max_workers=12) as executor:
        # Submit all batches
        future_to_batch = {}
        for batch_num, batch in enumerate(batches):
            future = executor.submit(process_curve_batch, batch, currency, batch_num + 1)
            future_to_batch[future] = batch_num + 1
        
        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                batch_curves, batch_completed = future.result()
                all_curves.update(batch_curves)
                completed_files += batch_completed
                
                # Update progress
                with progress_lock:
                    progress_data['currencies'][currency]['current'] = completed_files
                    progress_data['currencies'][currency]['message'] = f'Completed batch {batch_num}...'
                
                print(f"🎯 Batch {batch_num} completed: {len(batch_curves)} curves loaded")
                
            except Exception as e:
                print(f"❌ Batch {batch_num} failed: {e}")
    
    # Mark as complete
    with progress_lock:
        progress_data['currencies'][currency]['status'] = 'complete'
        progress_data['currencies'][currency]['message'] = f'Successfully loaded {len(all_curves)} {currency.upper()} curves'
    
    print(f"✅ Successfully loaded {len(all_curves)} {currency.upper()} curves using concurrent processing")
    return all_curves

def process_curve_batch(file_batch, currency, batch_num):
    """
    Process a batch of curve files in a single thread
    
    Args:
        file_batch: List of file paths to process
        currency: Currency code
        batch_num: Batch number for logging
    
    Returns:
        tuple: (curves_dict, completed_count)
    """
    curves = {}
    completed = 0
    
    print(f"🧵 Thread {batch_num}: Starting batch with {len(file_batch)} files")
    
    for filepath in file_batch:
        filename = os.path.basename(filepath)
        try:
            # Simply deserialize the curve (no verification for maximum speed)
            xc.Deserialise(filepath, filename, True, True)
            curves[filename] = filename
            completed += 1
                
        except Exception as e:
            # Only print errors, not successes
            print(f"🧵 Thread {batch_num}: ❌ {filename} failed: {e}")
            completed += 1
    
    print(f"🧵 Thread {batch_num}: Completed batch - {len(curves)}/{len(file_batch)} curves loaded successfully")
    return curves, completed

def load_multiple_currencies(currencies: list, max_files: int = 1000):
    """
    Load curves for multiple currencies with overall progress tracking
    
    Args:
        currencies: List of currency codes to load
        max_files: Maximum files per currency
    
    Returns:
        dict: Dictionary with currency as key and curves dict as value
    """
    all_curves = {}
    
    # Calculate total files across all currencies using the proper date-sorted method
    total_files = 0
    for currency in currencies:
        if currency in CURRENCY_CONFIG:
            # Use get_last_n_days_files to get the actual files that will be loaded
            recent_files = get_last_n_days_files(currency, max_files)
            total_files += len(recent_files)
    
    # Initialize overall progress
    with progress_lock:
        progress_data['overall'] = {
            'current': 0,
            'total': total_files,
            'status': 'loading',
            'message': f'Loading {len(currencies)} currencies...'
        }
    
    current_overall = 0
    
    for currency in currencies:
        print(f"\n🔄 Loading {currency.upper()} curves...")
        
        # Update overall message
        with progress_lock:
            progress_data['overall']['message'] = f'Loading {currency.upper()} curves...'
        
        # Load this currency
        curves = load_currency_curves(currency, max_files)
        all_curves[currency] = curves
        
        # Update overall progress with the actual number of files processed for this currency
        with progress_lock:
            # Get the actual files processed from the currency-specific progress
            if currency in progress_data['currencies']:
                currency_files = progress_data['currencies'][currency]['current']
            else:
                currency_files = len(curves)
            
            current_overall += currency_files
            progress_data['overall']['current'] = current_overall
            progress_data['overall']['message'] = f'Completed {currency.upper()} curves ({currency_files} files)'
    
    # Mark overall as complete
    with progress_lock:
        progress_data['overall']['status'] = 'complete'
        progress_data['overall']['message'] = f'Successfully loaded all {len(currencies)} currencies'
    
    print(f"\n🎉 All currencies loaded successfully!")
    return all_curves


def get_progress():
    """Get current progress data"""
    with progress_lock:
        return progress_data.copy()

def reset_progress():
    """Reset progress data"""
    global progress_data
    with progress_lock:
        progress_data = {
            'currencies': {},
            'overall': {
                'current': 0,
                'total': 0,
                'status': 'idle',
                'message': ''
            }
        }

def swap_rate(curves, start, end, currency: str = 'aud'): 
    """
    Calculate swap rates for given curves and currency
    
    Args:
        curves: Dictionary of curve filenames
        start: Start tenor (e.g., '1y')
        end: End tenor (e.g., '1y') 
        currency: Currency code (including template, e.g., 'aud', 'audxc', 'audbs')
    """
    if currency not in CURRENCY_CONFIG:
        raise ValueError(f"Unsupported currency: {currency}")
    
    # Get configuration directly from CURRENCY_CONFIG
    config = CURRENCY_CONFIG[currency]
    swap_rates = {}
    curve_list = list(curves.keys())
    
    for i, curve_filename in enumerate(curve_list, 1):
        try:
            # Extract just the filename if it's a full path
            filename_only = os.path.basename(curve_filename)
            date_str = filename_only[:6]
            date = yymmdd_to_datetime(date_str)
            date_excel = str(yymmdd_to_excel_date(date_str))
            
            settlement_date = str(xc.DateAdd(date_excel, "2b", "syb"))
            swap_start = str(xc.DateAdd(settlement_date, start, "syb"))
            swap_end = str(xc.DateAdd(swap_start, end, "syb"))

            # Use block bundle instead of individual curve
            bundle_name = f"{date_str}_core"
            
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
                print(f"  [{i:3d}/{len(curve_list)}] ❌ {curve_filename}: {e}")
    
    return swap_rates

def swap_rate_fixed_date(curves, fixed_start_date, tenor, currency: str = 'aud'):
    """
    Calculate swap rates with a fixed start date for given curves and currency
    Only includes curve dates that are <= fixed_start_date
    
    Args:
        curves: Dictionary of curve filenames
        fixed_start_date: Fixed start date for the swap (datetime object)
        tenor: Tenor from fixed start date (e.g., '1y')
        currency: Currency code (including template, e.g., 'aud', 'audxc', 'audbs')
    """
    if currency not in CURRENCY_CONFIG:
        raise ValueError(f"Unsupported currency: {currency}")
    
    # Get configuration directly from CURRENCY_CONFIG
    config = CURRENCY_CONFIG[currency]
    swap_rates = {}
    curve_list = list(curves.keys())
    
    # Convert fixed start date to Excel date for xcurves
    excel_epoch = datetime(1900, 1, 1)
    fixed_start_excel = str((fixed_start_date - excel_epoch).days + 2)
    
    # Calculate fixed end date
    fixed_end_excel = str(xc.DateAdd(fixed_start_excel, tenor, "syb"))
    
    for i, curve_filename in enumerate(curve_list, 1):
        try:
            # Extract just the filename if it's a full path
            filename_only = os.path.basename(curve_filename)
            date_str = filename_only[:6]
            curve_date = yymmdd_to_datetime(date_str)
            
            # Skip curves where curve date > fixed start date
            if curve_date > fixed_start_date:
                continue
            
            # Calculate settlement date for this curve
            curve_date_excel = str(yymmdd_to_excel_date(date_str))
            settlement_date = str(xc.DateAdd(curve_date_excel, "2b", "syb"))
            
            # Use block bundle instead of individual curve
            bundle_name = f"{date_str}_core"
            
            # Use fixed start and end dates for the swap calculation
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
                print(f"  [{i:3d}/{len(curve_list)}] ❌ {curve_filename}: {e}")
    
    return swap_rates

def initialize_curves(currencies: list = ['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad', 'usd']):
    """Initialize curves cache for specified currencies at startup"""
    global curves_cache, curves_loaded
    
    print(f"🚀 Starting curve initialization for: {', '.join(currencies).upper()}")
    
    with curves_lock:
        # Load multiple currencies with 200 files each
        all_curves = load_multiple_currencies(currencies, max_files=200)
        
        # Update cache and loaded status
        for currency in currencies:
            if currency in all_curves:
                curves_cache[currency] = all_curves[currency]
                curves_loaded[currency] = True
                print(f"✅ {currency.upper()} curves loaded and cached: {len(all_curves[currency])} curves")
        
        # Build block bundles for each date combining all available currencies
        print(f"🔗 Building block bundles for all dates...")
        
        # Get all unique dates across all loaded currencies
        all_dates = set()
        for currency in curves_cache:
            for filename in curves_cache[currency].keys():
                all_dates.add(filename[:6])  # Extract YYMMDD
        
        # Build block bundle for each date
        for date_str in sorted(all_dates):
            currency_list = []
            curve_names = []
            
            # Include all currencies that have a curve for this date
            for currency in curves_cache:
                date_filename = f"{date_str}_{currency}_curve.json"
                if date_filename in curves_cache[currency]:
                    currency_list.append(currency[:3].upper())  # 3 letters uppercase
                    curve_names.append(date_filename)
            
            # Build bundle if we have curves for this date
            if currency_list:
                bundle_name = f"{date_str}_core"
                try:
                    # Build currency-curve pairs: [[ccy1, curve1], [ccy2, curve2], ...]
                    currency_curve_pairs = []
                    for i in range(len(currency_list)):
                        currency_curve_pairs.append([currency_list[i], curve_names[i]])
                    
                    # Build FX pairs: [["AUDUSD", "1"], ["EURUSD", "1"], ...]
                    fx_pair_rates = [["AUDUSD", "1"], ["EURUSD", "1"], ["USDJPY", "1"], ["USDCAD", "1"], ["NZDUSD", "1"], ["GBPUSD", "1"]]
                    
                    xc.BuildBlockBundle(bundle_name, currency_curve_pairs, fx_pair_rates)
                    print(f"✅ Built bundle {bundle_name} with {len(currency_list)} currencies")
                except Exception as e:
                    print(f"❌ Failed to build bundle {bundle_name}: {e}")

    print(f"🎯 Curve initialization complete!")
    return curves_cache

def get_curves(currency: str = 'aud'):
    """Get cached curves for specific currency, initialize if not loaded"""
    global curves_cache, curves_loaded
    
    with curves_lock:
        if currency not in curves_loaded or not curves_loaded.get(currency, False):
            # Load this specific currency
            curves = load_currency_curves(currency)
            curves_cache[currency] = curves
            curves_loaded[currency] = True
        
        return curves_cache.get(currency, {})

def get_swap_data(tenor_syntax: str):
    """
    Parse tenor syntax and return swap rate data using cached curves
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
            
            # Get the base currency for curve loading (e.g., 'audxc' -> 'aud', 'aud6s3s' -> 'aud')
            base_currency = currency
            for base in ['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad', 'usd']:
                if currency.startswith(base):
                    base_currency = base
                    break
            
            # Use cached curves for the base currency
            curves = get_curves(base_currency)
            if not curves:
                return None, f"No {base_currency.upper()} curves loaded"
            
            # Calculate swap rates using the specific currency template
            rates = swap_rate(curves, start, end, currency)
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
                
                # Get the base currency for curve loading
                base_currency = currency
                for base in ['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad', 'usd']:
                    if currency.startswith(base):
                        base_currency = base
                        break
                
                # Use cached curves for the base currency
                curves = get_curves(base_currency)
                if not curves:
                    return None, f"No {base_currency.upper()} curves loaded"
                
                # Calculate fixed-date swap rates
                rates = swap_rate_fixed_date(curves, fixed_start_date, tenor, currency)
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
                df1, error1 = get_swap_data(f"{currency}.{tenor1_formatted}")
                if error1:
                    return None, error1
                
                df2, error2 = get_swap_data(f"{currency}.{tenor2_formatted}")
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
            df1, error1 = get_swap_data(f"{currency}.{tenor1}")
            if error1:
                return None, error1
            
            df2, error2 = get_swap_data(f"{currency}.{tenor2}")
            if error2:
                return None, error2
                
            df3, error3 = get_swap_data(f"{currency}.{tenor3}")
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

def is_curves_loaded(currency: str = None):
    """Check if curves are loaded for specific currency or any currency"""
    with curves_lock:
        if currency:
            return curves_loaded.get(currency, False)
        else:
            # Return True if any currency is loaded
            return any(curves_loaded.values()) if curves_loaded else False

def get_loaded_currencies():
    """Get list of currently loaded currencies"""
    with curves_lock:
        return [currency for currency, loaded in curves_loaded.items() if loaded]

def clear_all_curves():
    """Clear all cached curves and reset loaded status"""
    global curves_cache, curves_loaded
    with curves_lock:
        curves_cache.clear()
        curves_loaded.clear()
    print("🧹 All curves cache cleared")

def update_today_with_realtime(currencies: list = None):
    """
    Update today's curve data with real-time rates and save as JSON files
    
    Args:
        currencies: List of currencies to update (default: all loaded currencies)
    
    Returns:
        dict: Status of updates per currency
    """
    if not REALTIME_AVAILABLE:
        return {'error': 'Real-time curves module not available'}
    
    if currencies is None:
        currencies = get_loaded_currencies()
    
    if not currencies:
        return {'error': 'No currencies loaded'}
    
    # Filter to only include currencies that are actually loaded
    loaded_currencies = get_loaded_currencies()
    currencies_to_update = [ccy for ccy in currencies if ccy in loaded_currencies]
    
    if not currencies_to_update:
        return {'error': f'None of the requested currencies {currencies} are loaded. Loaded currencies: {loaded_currencies}'}
    
    print(f"🎯 Optimized real-time update: Only building curves for loaded currencies: {', '.join(currencies_to_update).upper()}")
    
    today = datetime.now().strftime("%Y-%m-%d")
    today_yymmdd = datetime.now().strftime("%y%m%d")
    
    results = {}
    
    try:
        # Build real-time curves for only the loaded currencies (optimized!)
        print(f"🔄 Building real-time curves for {', '.join(currencies_to_update).upper()}...")
        realtime_result = realtime_curves.build_selected_curves_realtime(today, currencies_to_update)
        
        if 'error' in realtime_result:
            return {'error': realtime_result['error']}
        
        realtime_curves_data = realtime_result.get('curves', {})
        
        # Get path to main chart_app directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
        
        # Update cache with the new curve names (no need to save files since curves are already built in xcurves)
        with curves_lock:
            for currency in currencies_to_update:
                try:
                    # Create today's filename format
                    today_filename = f"{today_yymmdd}_{currency}_curve.json"
                    
                    # Add to cache - the curve is already built in xcurves with this name
                    if currency not in curves_cache:
                        curves_cache[currency] = {}
                    
                    curves_cache[currency][today_filename] = today_filename
                    results[currency] = {
                        'status': 'success', 
                        'filename': today_filename,
                        'curve_name': today_filename
                    }
                    print(f"✅ {currency.upper()} real-time curve added to cache as {today_filename}")
                    
                except Exception as e:
                    results[currency] = {'status': 'failed', 'error': f'Failed to add to cache: {str(e)}'}
                    print(f"❌ Failed to add {currency.upper()} real-time curve to cache: {e}")
            
            # Build block bundle for today's date with all updated currencies
            print(f"🔗 Building today's block bundle: {today_yymmdd}_core...")
            currency_list = []
            curve_names = []
            
            # Include all currencies that were successfully updated
            for currency in currencies_to_update:
                if results.get(currency, {}).get('status') == 'success':
                    currency_list.append(currency[:3].upper())  # 3 letters uppercase
                    curve_names.append(f"{today_yymmdd}_{currency}_curve.json")
            
            # Build today's bundle if we have curves
            if currency_list:
                bundle_name = f"{today_yymmdd}_core"
                try:
                    # Build currency-curve pairs: [[ccy1, curve1], [ccy2, curve2], ...]
                    currency_curve_pairs = []
                    for i in range(len(currency_list)):
                        currency_curve_pairs.append([currency_list[i], curve_names[i]])
                    
                    # Build FX pairs: [["AUDUSD", "1"], ["EURUSD", "1"], ...]
                    fx_pair_rates = [["AUDUSD", "1"], ["EURUSD", "1"], ["USDJPY", "1"], ["USDCAD", "1"], ["NZDUSD", "1"], ["GBPUSD", "1"]]
                    
                    xc.BuildBlockBundle(bundle_name, currency_curve_pairs, fx_pair_rates)
                    print(f"✅ Built today's bundle {bundle_name} with {len(currency_list)} currencies")
                    
                    # Add bundle info to results
                    results['bundle'] = {
                        'name': bundle_name,
                        'currencies': currency_list,
                        'status': 'success'
                    }
                    
                except Exception as e:
                    print(f"❌ Failed to build today's bundle {bundle_name}: {e}")
                    results['bundle'] = {
                        'name': bundle_name,
                        'status': 'failed',
                        'error': str(e)
                    }
        
        return {'success': True, 'results': results, 'date': today, 'currencies': currencies_to_update}
        
    except Exception as e:
        return {'error': f'Failed to update real-time curves: {str(e)}'}

def get_realtime_status():
    """Check if real-time functionality is available"""
    return {
        'available': REALTIME_AVAILABLE,
        'loaded_currencies': get_loaded_currencies(),
        'today_date': datetime.now().strftime("%Y-%m-%d")
    }

def get_last_n_days_files(currency: str, n_days: int = 200):
    """
    Get the most recent n days of curve files for a currency
    
    Args:
        currency: Currency code
        n_days: Number of days to retrieve
    
    Returns:
        list: List of filenames for the last n days
    """
    if currency not in CURRENCY_CONFIG:
        return []
    
    config = CURRENCY_CONFIG[currency]
    # Get the directory where this script is located (dashboard folder)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels to get to the main chart_app directory
    chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
    folder_path = os.path.join(chart_app_dir, config['folder'])
    curve_files = glob.glob(os.path.join(folder_path, "*.json"))
    
    # Extract dates and sort by date (most recent first)
    file_dates = []
    for filepath in curve_files:
        filename = os.path.basename(filepath)
        try:
            date_str = filename[:6]
            date_obj = yymmdd_to_datetime(date_str)
            file_dates.append((date_obj, filename))
        except:
            continue
    
    # Sort by date (most recent first) and take last n_days
    file_dates.sort(key=lambda x: x[0], reverse=True)
    recent_files = [filename for _, filename in file_dates[:n_days]]
    
    return recent_files
