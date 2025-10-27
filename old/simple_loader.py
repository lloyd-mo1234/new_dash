import os
import glob
import sys
from datetime import datetime
import threading

# Add the printing_scripts directory to the path to import realtime_curves
script_dir = os.path.dirname(os.path.abspath(__file__))
chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
printing_scripts_path = os.path.join(chart_app_dir, 'printing_scripts')
if printing_scripts_path not in sys.path:
    sys.path.append(printing_scripts_path)

try:
    import realtime_curves
    REALTIME_AVAILABLE = True
    print("✅ Real-time curves module imported successfully")
except ImportError as e:
    REALTIME_AVAILABLE = False
    print(f"⚠️ Real-time curves module not available: {e}")

from cba.analytics import xcurves as xc

# Simple global state
curves_loaded = False
curves_lock = threading.Lock()

def yymmdd_to_datetime(date_str: str) -> datetime:
    """Convert YYMMDD to datetime object"""
    yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
    # Handle dates starting with 9 as 1990s (90-99 -> 1990-1999)
    year = 1900 + yy if yy >= 90 else 2000 + yy
    return datetime(year, mm, dd)

def load_core_curves(max_days: int = 200):
    """
    Load core curves from core_curves directory
    
    Args:
        max_days: Maximum number of days to load
    
    Returns:
        int: Number of bundles loaded
    """
    print(f"🚀 Loading core curves (max {max_days} days)")
    
    # Get path to core_curves directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
    core_curves_dir = os.path.join(chart_app_dir, 'core_curves')
    
    if not os.path.exists(core_curves_dir):
        print(f"❌ Core curves directory not found: {core_curves_dir}")
        return 0
    
    # Find all core bundle files
    bundle_files = glob.glob(os.path.join(core_curves_dir, "*_core_bundle.json"))
    
    if not bundle_files:
        print(f"❌ No core bundle files found in: {core_curves_dir}")
        return 0
    
    # Extract dates and sort by date (most recent first)
    file_dates = []
    for filepath in bundle_files:
        filename = os.path.basename(filepath)
        try:
            # Extract date from filename like "001006_core_bundle.json"
            date_str = filename[:6]
            date_obj = yymmdd_to_datetime(date_str)
            file_dates.append((date_obj, date_str, filename, filepath))
        except:
            continue
    
    # Sort by date (most recent first) and take max_days
    file_dates.sort(key=lambda x: x[0], reverse=True)
    recent_bundles = file_dates[:max_days]
    
    print(f"📅 Found {len(bundle_files)} total core bundles, loading {len(recent_bundles)} most recent")
    if recent_bundles:
        oldest_date = recent_bundles[-1][0]
        newest_date = recent_bundles[0][0]
        print(f"📊 Date range: {oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')}")
    
    # Load each bundle directly
    success_count = 0
    for i, (date_obj, date_str, filename, filepath) in enumerate(recent_bundles, 1):
        try:
            bundle_name = f"{date_str}_core"
            
            # Deserialize the bundle directly into xcurves
            xc.Deserialise(filepath, bundle_name, True, True)
            success_count += 1
            
            if i % 50 == 0 or i == len(recent_bundles):
                print(f"✅ Loaded {i}/{len(recent_bundles)} core bundles")
                
        except Exception as e:
            print(f"❌ Failed to load core bundle {filename}: {e}")
    
    print(f"🎯 Core curves loading complete: {success_count}/{len(recent_bundles)} bundles loaded")
    return success_count

def load_realtime_curves(currencies: list = None):
    """
    Load today's real-time curves
    
    Args:
        currencies: List of currencies to build real-time curves for
    
    Returns:
        dict: Status of real-time curve creation
    """
    if not REALTIME_AVAILABLE:
        return {'error': 'Real-time curves module not available'}
    
    if currencies is None:
        currencies = ['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad']  # Default currencies
    
    today = datetime.now().strftime("%Y-%m-%d")
    today_yymmdd = datetime.now().strftime("%y%m%d")
    
    print(f"🔄 Building real-time curves for today ({today})...")
    
    try:
        # Build real-time curves using the existing realtime_curves module
        realtime_result = realtime_curves.build_selected_curves_realtime(today, currencies)
        
        if 'error' in realtime_result:
            return {'error': realtime_result['error']}
        
        # The real-time system automatically creates a bundle named {today_yymmdd}_core
        bundle_name = f"{today_yymmdd}_core"
        
        print(f"✅ Real-time curves built successfully: {bundle_name}")
        
        return {
            'success': True,
            'bundle_name': bundle_name,
            'date': today_yymmdd,
            'currencies': currencies
        }
        
    except Exception as e:
        return {'error': f'Failed to build real-time curves: {str(e)}'}

def load_curves():
    """
    Simple function to load both core curves and real-time curves
    
    Returns:
        dict: Status of loading operation
    """
    global curves_loaded
    
    with curves_lock:
        print("🚀 Starting curve loading...")
        
        # Step 1: Load core curves
        core_count = load_core_curves(max_days=200)
        
        # Step 2: Load real-time curves
        realtime_result = load_realtime_curves()
        
        # Mark as loaded regardless of real-time success
        curves_loaded = True
        
        result = {
            'success': True,
            'core_bundles': core_count,
            'realtime': realtime_result
        }
        
        if 'error' in realtime_result:
            print(f"⚠️ Real-time loading failed: {realtime_result['error']}")
            result['warning'] = f"Real-time loading failed: {realtime_result['error']}"
        else:
            print("✅ Real-time curves loaded successfully")
        
        print(f"🎯 Curve loading complete! Core: {core_count} bundles, Real-time: {'✅' if 'success' in realtime_result else '❌'}")
        
        return result

def is_curves_loaded():
    """Check if curves are loaded"""
    with curves_lock:
        return curves_loaded

def clear_curves():
    """Clear curves cache"""
    global curves_loaded
    with curves_lock:
        curves_loaded = False
    print("🧹 Curves cache cleared")

def get_curves_status():
    """Get simple status of curves"""
    with curves_lock:
        return {
            'loaded': curves_loaded,
            'timestamp': datetime.now().isoformat()
        }
