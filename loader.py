import os
import glob
import re
from datetime import datetime
import threading
import sys
from cba.analytics import xcurves as xc

# Import realtime_curves from the same directory
try:
    import realtime_curves
    REALTIME_AVAILABLE = True
    print("✅ Real-time curves module imported successfully")
except ImportError as e:
    REALTIME_AVAILABLE = False
    print(f"⚠️ Real-time curves module not available: {e}")

# Global curves cache - stores both historical and real-time bundle names
curves_cache = {}
curves_loaded = {}
curves_lock = threading.Lock()

# Global progress tracking
progress_data = {
    'current': 0,
    'total': 0,
    'status': 'idle',
    'message': ''
}
progress_lock = threading.Lock()

def yymmdd_to_datetime(date_str: str) -> datetime:
    """Convert YYMMDD to datetime object"""
    yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
    # Handle dates starting with 9 as 1990s (90-99 -> 1990-1999)
    year = 1900 + yy if yy >= 90 else 2000 + yy
    return datetime(year, mm, dd)

def get_most_recent_core_bundles(max_days: int = 200):
    """
    Get the most recent x core bundle files from core_curves directory
    
    Args:
        max_days: Maximum number of days to load
    
    Returns:
        list: List of (date_str, bundle_filename) tuples sorted by date (most recent first)
    """
    # Get path to core_curves directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chart_app_dir = os.path.dirname(script_dir)
    core_curves_dir = os.path.join(chart_app_dir, 'core_curves')
    
    if not os.path.exists(core_curves_dir):
        print(f"❌ Core curves directory not found: {core_curves_dir}")
        return []
    
    # Find all core bundle files
    bundle_files = glob.glob(os.path.join(core_curves_dir, "*_core_bundle.json"))
    
    if not bundle_files:
        print(f"❌ No core bundle files found in: {core_curves_dir}")
        return []
    
    # Extract dates and sort by date (most recent first)
    file_dates = []
    for filepath in bundle_files:
        filename = os.path.basename(filepath)
        try:
            # Extract date from filename like "001006_core_bundle.json"
            date_str = filename[:6]
            date_obj = yymmdd_to_datetime(date_str)
            file_dates.append((date_obj, date_str, filename))
        except:
            continue
    
    # Sort by date (most recent first) and take max_days
    file_dates.sort(key=lambda x: x[0], reverse=True)
    recent_bundles = [(date_str, filename) for _, date_str, filename in file_dates[:max_days]]
    
    print(f"📅 Found {len(bundle_files)} total core bundles, selected {len(recent_bundles)} most recent")
    if recent_bundles:
        oldest_date = yymmdd_to_datetime(recent_bundles[-1][0])
        newest_date = yymmdd_to_datetime(recent_bundles[0][0])
        print(f"📊 Date range: {oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')}")
    
    return recent_bundles

def load_historical_bundles(max_days: int = 200, num_threads: int = 12):
    """
    Load historical core bundles from core_curves directory using concurrent threads
    
    Args:
        max_days: Maximum number of days to load
        num_threads: Number of concurrent threads to use for loading
    
    Returns:
        dict: Dictionary of loaded bundle names by date
    """
    # Get the most recent core bundles
    recent_bundles = get_most_recent_core_bundles(max_days)
    
    if not recent_bundles:
        return {}
    
    # Initialize progress
    with progress_lock:
        progress_data.update({
            'current': 0,
            'total': len(recent_bundles),
            'status': 'loading',
            'message': f'Loading {len(recent_bundles)} historical bundles...'
        })
    
    # Get path to core_curves directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chart_app_dir = os.path.dirname(script_dir)
    core_curves_dir = os.path.join(chart_app_dir, 'core_curves')
    
    loaded_bundles = {}
    success_count = 0
    bundle_lock = threading.Lock()
    
    def load_bundle_worker(bundle_info):
        """Worker function to load a single bundle"""
        nonlocal success_count
        date_str, filename = bundle_info
        
        try:
            bundle_filepath = os.path.join(core_curves_dir, filename)
            bundle_name = f"{date_str}_core"
            
            # Deserialize the bundle directly into xcurves
            xc.Deserialise(bundle_filepath, bundle_name, True, True)
            
            # Thread-safe update of results
            with bundle_lock:
                loaded_bundles[date_str] = bundle_name
                success_count += 1
                current_count = success_count
            
            # Update progress (thread-safe)
            with progress_lock:
                progress_data.update({
                    'current': current_count,
                    'message': f'Loading historical bundles: {current_count}/{len(recent_bundles)}'
                })
                
        except Exception as e:
            pass  # Silent failure for individual bundles
    
    # Use ThreadPoolExecutor for concurrent loading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all bundle loading tasks
        future_to_bundle = {
            executor.submit(load_bundle_worker, bundle_info): bundle_info 
            for bundle_info in recent_bundles
        }
        
        # Wait for all tasks to complete
        for future in as_completed(future_to_bundle):
            try:
                future.result()
            except Exception:
                pass  # Silent failure
    
    return loaded_bundles

def add_realtime_bundle(currencies: list = None):
    """
    Add today's real-time bundle to the cache
    
    Args:
        currencies: List of currencies to build real-time curves for
    
    Returns:
        dict: Status of real-time bundle creation
    """
    if not REALTIME_AVAILABLE:
        return {'error': 'Real-time curves module not available'}
    
    if currencies is None:
        currencies = ['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad']  # Default currencies
    
    today = datetime.now().strftime("%Y-%m-%d")
    today_yymmdd = datetime.now().strftime("%y%m%d")
    
    # Update progress to show real-time building
    with progress_lock:
        progress_data.update({
            'status': 'loading',
            'message': f'Building real-time bundle...'
        })
    
    try:
        # Build real-time curves using the existing realtime_curves module
        realtime_result = realtime_curves.build_selected_curves_realtime(today, currencies)
        
        if 'error' in realtime_result:
            with progress_lock:
                progress_data.update({
                    'status': 'error',
                    'message': f'Real-time bundle failed'
                })
            return {'error': realtime_result['error']}
        
        # The real-time system automatically creates a bundle named {today_yymmdd}_core_bundle
        bundle_name = f"{today_yymmdd}_core_bundle"
        
        # Add to our cache
        with curves_lock:
            curves_cache[today_yymmdd] = bundle_name
        
        # Update progress to show real-time bundle completed
        with progress_lock:
            progress_data.update({
                'status': 'complete',
                'message': f'Real-time bundle loaded'
            })
        
        # Show only the currencies included in today's bundle
        currencies_upper = [ccy.upper() for ccy in currencies]
        print(f"📈 Real-time bundle includes: {', '.join(currencies_upper)}")
        
        return {
            'success': True,
            'bundle_name': bundle_name,
            'date': today_yymmdd,
            'currencies': currencies
        }
        
    except Exception as e:
        with progress_lock:
            progress_data.update({
                'status': 'error',
                'message': f'Failed to build real-time bundle'
            })
        return {'error': f'Failed to build real-time bundle: {str(e)}'}

def initialize_curves(max_days: int = 200, include_realtime: bool = True, realtime_currencies: list = None):
    """
    Curve initialization that loads both historical and real-time bundles
    
    Args:
        max_days: Maximum number of historical days to load
        include_realtime: Whether to include today's real-time data
        realtime_currencies: List of currencies for real-time (default: all major currencies)
    
    Returns:
        dict: Dictionary of loaded bundle names with status
    """
    global curves_cache, curves_loaded
    
    results = {
        'historical': {},
        'realtime': {},
        'status': 'success',
        'errors': []
    }
    
    with curves_lock:
        # Step 1: Load historical bundles
        historical_bundles = load_historical_bundles(max_days, num_threads=12)
        curves_cache.update(historical_bundles)
        results['historical'] = historical_bundles
        
        # Mark as loaded and complete after historical bundles are done
        curves_loaded['bundles'] = True
        
        # Mark as complete immediately after historical loading
        with progress_lock:
            progress_data.update({
                'status': 'complete',
                'message': f'Historical curves loaded'
            })
        
        # Step 2: Add real-time bundle if requested
        if include_realtime:
            realtime_result = add_realtime_bundle(realtime_currencies)
            
            if 'error' in realtime_result:
                results['errors'].append(f"Real-time: {realtime_result['error']}")
            else:
                results['realtime'] = realtime_result
    
    return results

def initialize_historical_curves_only(max_days: int = 200):
    """
    Initialize only historical curves (no real-time data)
    
    Args:
        max_days: Maximum number of historical days to load
    
    Returns:
        dict: Dictionary of loaded bundle names with status
    """
    global curves_cache, curves_loaded
    
    results = {
        'historical': {},
        'status': 'success',
        'errors': []
    }
    
    with curves_lock:
        # Load historical bundles only
        historical_bundles = load_historical_bundles(max_days, num_threads=12)
        curves_cache.update(historical_bundles)
        results['historical'] = historical_bundles
        
        # Mark as loaded and complete after historical bundles are done
        curves_loaded['bundles'] = True
        
        # Mark as complete immediately after historical loading
        with progress_lock:
            progress_data.update({
                'status': 'complete',
                'message': f'Historical curves loaded'
            })
    
    return results

def get_bundle_name(date_str: str):
    """
    Get the bundle name for a specific date (works for both historical and real-time)
    
    Args:
        date_str: Date string in YYMMDD format
    
    Returns:
        str: Bundle name or None if not found
    """
    with curves_lock:
        return curves_cache.get(date_str)

def get_available_dates():
    """Get list of available dates in the cache (both historical and real-time)"""
    with curves_lock:
        return sorted(curves_cache.keys())

def is_curves_loaded():
    """Check if curves are loaded"""
    with curves_lock:
        return curves_loaded.get('bundles', False)

def clear_curves():
    """Clear curves cache"""
    global curves_cache, curves_loaded
    with curves_lock:
        curves_cache.clear()
        curves_loaded.clear()
    print("🧹 Curves cache cleared")

def get_cache_stats():
    """Get statistics about the cache"""
    with curves_lock:
        if not curves_cache:
            return {
                'loaded': False,
                'bundle_count': 0,
                'date_range': None,
                'has_today': False
            }
        
        dates = sorted(curves_cache.keys())
        oldest_date = yymmdd_to_datetime(dates[-1]) if dates else None
        newest_date = yymmdd_to_datetime(dates[0]) if dates else None
        
        # Check if today's data is included
        today_yymmdd = datetime.now().strftime("%y%m%d")
        has_today = today_yymmdd in curves_cache
        
        return {
            'loaded': True,
            'bundle_count': len(curves_cache),
            'date_range': {
                'oldest': oldest_date.strftime('%Y-%m-%d') if oldest_date else None,
                'newest': newest_date.strftime('%Y-%m-%d') if newest_date else None
            },
            'has_today': has_today,
            'today_date': datetime.now().strftime('%Y-%m-%d'),
            'sample_dates': dates[:10]  # First 10 dates as sample
        }

def update_realtime_bundle(currencies: list = None):
    """
    Update or add today's real-time bundle
    This can be called periodically to refresh real-time data
    
    Args:
        currencies: List of currencies to update
    
    Returns:
        dict: Status of the update
    """
    print(f"🔄 Updating real-time bundle...")
    
    # Add/update the real-time bundle
    result = add_realtime_bundle(currencies)
    
    if 'error' not in result:
        print(f"✅ Real-time bundle updated successfully")
    else:
        print(f"❌ Failed to update real-time bundle: {result['error']}")
    
    return result

def get_progress():
    """Get current progress of loading"""
    with progress_lock:
        return progress_data.copy()

def reset_progress():
    """Reset progress tracking"""
    global progress_data
    with progress_lock:
        progress_data = {
            'current': 0,
            'total': 0,
            'status': 'idle',
            'message': ''
        }
