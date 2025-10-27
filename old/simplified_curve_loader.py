import os
import glob
import re
from datetime import datetime
import threading
from cba.analytics import xcurves as xc

# Global simplified curves cache - just stores bundle names
simplified_curves_cache = {}
simplified_curves_loaded = {}
simplified_curves_lock = threading.Lock()

# Global progress tracking
simplified_progress_data = {
    'current': 0,
    'total': 0,
    'status': 'idle',
    'message': ''
}
simplified_progress_lock = threading.Lock()

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
    chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
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

def load_core_bundles_directly(max_days: int = 200):
    """
    Directly load pre-existing core bundles from core_curves directory
    This is much simpler than the old approach of loading individual curves and building bundles
    
    Args:
        max_days: Maximum number of days to load
    
    Returns:
        dict: Dictionary of loaded bundle names by date
    """
    print(f"🚀 Starting simplified core bundle loading (max {max_days} days)")
    
    # Get the most recent core bundles
    recent_bundles = get_most_recent_core_bundles(max_days)
    
    if not recent_bundles:
        return {}
    
    # Initialize progress
    with simplified_progress_lock:
        simplified_progress_data.update({
            'current': 0,
            'total': len(recent_bundles),
            'status': 'loading',
            'message': f'Loading {len(recent_bundles)} core bundles...'
        })
    
    # Get path to core_curves directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chart_app_dir = os.path.dirname(os.path.dirname(script_dir))
    core_curves_dir = os.path.join(chart_app_dir, 'core_curves')
    
    loaded_bundles = {}
    success_count = 0
    
    # Load each bundle directly
    for i, (date_str, filename) in enumerate(recent_bundles, 1):
        try:
            bundle_filepath = os.path.join(core_curves_dir, filename)
            bundle_name = f"{date_str}_core"
            
            # Deserialize the bundle directly into xcurves
            xc.Deserialise(bundle_filepath, bundle_name, True, True)
            
            # Store just the bundle name in our cache
            loaded_bundles[date_str] = bundle_name
            success_count += 1
            
            # Update progress
            with simplified_progress_lock:
                simplified_progress_data.update({
                    'current': i,
                    'message': f'Loaded bundle {i}/{len(recent_bundles)}: {bundle_name}'
                })
            
            if i % 50 == 0 or i == len(recent_bundles):
                print(f"✅ Loaded {i}/{len(recent_bundles)} bundles")
                
        except Exception as e:
            print(f"❌ Failed to load bundle {filename}: {e}")
    
    # Mark as complete
    with simplified_progress_lock:
        simplified_progress_data.update({
            'status': 'complete',
            'message': f'Successfully loaded {success_count}/{len(recent_bundles)} core bundles'
        })
    
    print(f"🎯 Simplified loading complete: {success_count}/{len(recent_bundles)} bundles loaded")
    return loaded_bundles

def initialize_curves_simplified(max_days: int = 200):
    """
    Simplified curve initialization that directly loads pre-existing core bundles
    This replaces the complex process of loading individual curves and building bundles
    
    Args:
        max_days: Maximum number of days to load
    
    Returns:
        dict: Dictionary of loaded bundle names
    """
    global simplified_curves_cache, simplified_curves_loaded
    
    print(f"🚀 Starting simplified curve initialization (max {max_days} days)")
    print("📋 This approach directly loads pre-existing core bundles instead of building them")
    
    with simplified_curves_lock:
        # Load the core bundles directly
        loaded_bundles = load_core_bundles_directly(max_days)
        
        if loaded_bundles:
            # Update our simplified cache
            simplified_curves_cache = loaded_bundles
            simplified_curves_loaded['core_bundles'] = True
            
            print(f"✅ Simplified initialization complete!")
            print(f"📊 Loaded {len(loaded_bundles)} core bundles")
            print(f"💾 Cache now contains bundle names for dates: {sorted(loaded_bundles.keys())[:5]}...{sorted(loaded_bundles.keys())[-5:]}")
        else:
            print(f"❌ No bundles were loaded")
            simplified_curves_loaded['core_bundles'] = False
    
    return simplified_curves_cache

def get_simplified_progress():
    """Get current progress of simplified loading"""
    with simplified_progress_lock:
        return simplified_progress_data.copy()

def reset_simplified_progress():
    """Reset simplified progress tracking"""
    global simplified_progress_data
    with simplified_progress_lock:
        simplified_progress_data = {
            'current': 0,
            'total': 0,
            'status': 'idle',
            'message': ''
        }

def is_simplified_curves_loaded():
    """Check if simplified curves are loaded"""
    with simplified_curves_lock:
        return simplified_curves_loaded.get('core_bundles', False)

def get_simplified_bundle_name(date_str: str):
    """
    Get the bundle name for a specific date
    
    Args:
        date_str: Date string in YYMMDD format
    
    Returns:
        str: Bundle name or None if not found
    """
    with simplified_curves_lock:
        return simplified_curves_cache.get(date_str)

def get_available_dates():
    """Get list of available dates in the simplified cache"""
    with simplified_curves_lock:
        return sorted(simplified_curves_cache.keys())

def clear_simplified_curves():
    """Clear simplified curves cache"""
    global simplified_curves_cache, simplified_curves_loaded
    with simplified_curves_lock:
        simplified_curves_cache.clear()
        simplified_curves_loaded.clear()
    print("🧹 Simplified curves cache cleared")

def get_simplified_cache_stats():
    """Get statistics about the simplified cache"""
    with simplified_curves_lock:
        if not simplified_curves_cache:
            return {
                'loaded': False,
                'bundle_count': 0,
                'date_range': None
            }
        
        dates = sorted(simplified_curves_cache.keys())
        oldest_date = yymmdd_to_datetime(dates[-1]) if dates else None
        newest_date = yymmdd_to_datetime(dates[0]) if dates else None
        
        return {
            'loaded': True,
            'bundle_count': len(simplified_curves_cache),
            'date_range': {
                'oldest': oldest_date.strftime('%Y-%m-%d') if oldest_date else None,
                'newest': newest_date.strftime('%Y-%m-%d') if newest_date else None
            },
            'sample_dates': dates[:10]  # First 10 dates as sample
        }
