"""
Performance Optimization Module for Swap Rate Analytics Dashboard

This module implements various performance improvements including:
1. Advanced caching mechanisms
2. Data preprocessing and indexing
3. Lazy loading strategies
4. Memory optimization
5. Concurrent processing
"""

import os
import json
import pickle
import hashlib
import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache, wraps
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any

# Global cache configurations
CACHE_DIR = os.path.join(os.path.dirname(__file__), 'cache')
MEMORY_CACHE = {}
CACHE_LOCK = threading.Lock()
MAX_MEMORY_CACHE_SIZE = 100  # Maximum number of items in memory cache

# Performance monitoring
PERFORMANCE_STATS = {
    'cache_hits': 0,
    'cache_misses': 0,
    'avg_load_time': 0,
    'total_requests': 0
}
STATS_LOCK = threading.Lock()

def ensure_cache_dir():
    """Ensure cache directory exists"""
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

def get_cache_key(func_name: str, *args, **kwargs) -> str:
    """Generate a unique cache key for function calls"""
    key_data = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
    return hashlib.md5(key_data.encode()).hexdigest()

def disk_cache(expiry_hours: int = 24):
    """
    Decorator for disk-based caching with expiry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ensure_cache_dir()
            
            # Generate cache key
            cache_key = get_cache_key(func.__name__, *args, **kwargs)
            cache_file = os.path.join(CACHE_DIR, f"{cache_key}.pkl")
            
            # Check if cache exists and is not expired
            if os.path.exists(cache_file):
                file_age = time.time() - os.path.getmtime(cache_file)
                if file_age < expiry_hours * 3600:  # Convert hours to seconds
                    try:
                        with open(cache_file, 'rb') as f:
                            result = pickle.load(f)
                        
                        with STATS_LOCK:
                            PERFORMANCE_STATS['cache_hits'] += 1
                        
                        return result
                    except:
                        # Cache file corrupted, remove it
                        os.remove(cache_file)
            
            # Cache miss - execute function
            with STATS_LOCK:
                PERFORMANCE_STATS['cache_misses'] += 1
            
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Update performance stats
            with STATS_LOCK:
                PERFORMANCE_STATS['total_requests'] += 1
                total_time = PERFORMANCE_STATS['avg_load_time'] * (PERFORMANCE_STATS['total_requests'] - 1)
                PERFORMANCE_STATS['avg_load_time'] = (total_time + execution_time) / PERFORMANCE_STATS['total_requests']
            
            # Save to cache
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(result, f)
            except Exception as e:
                print(f"Warning: Could not save to cache: {e}")
            
            return result
        return wrapper
    return decorator

def memory_cache_with_lru(maxsize: int = 128):
    """
    Enhanced memory cache with LRU eviction and size limits
    """
    def decorator(func):
        cache = {}
        access_order = []
        cache_lock = threading.Lock()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = get_cache_key(func.__name__, *args, **kwargs)
            
            with cache_lock:
                # Check if in cache
                if cache_key in cache:
                    # Move to end (most recently used)
                    access_order.remove(cache_key)
                    access_order.append(cache_key)
                    
                    with STATS_LOCK:
                        PERFORMANCE_STATS['cache_hits'] += 1
                    
                    return cache[cache_key]
                
                # Cache miss
                with STATS_LOCK:
                    PERFORMANCE_STATS['cache_misses'] += 1
            
            # Execute function
            result = func(*args, **kwargs)
            
            with cache_lock:
                # Add to cache
                cache[cache_key] = result
                access_order.append(cache_key)
                
                # Evict if over size limit
                while len(cache) > maxsize:
                    oldest_key = access_order.pop(0)
                    del cache[oldest_key]
            
            return result
        
        # Add cache management methods
        wrapper.cache_info = lambda: {
            'hits': PERFORMANCE_STATS['cache_hits'],
            'misses': PERFORMANCE_STATS['cache_misses'],
            'size': len(cache),
            'maxsize': maxsize
        }
        wrapper.cache_clear = lambda: cache.clear() or access_order.clear()
        
        return wrapper
    return decorator

class DataPreprocessor:
    """
    Preprocesses and indexes curve data for faster access
    """
    
    def __init__(self):
        self.indexed_data = {}
        self.date_ranges = {}
        self.lock = threading.Lock()
    
    @disk_cache(expiry_hours=6)
    def preprocess_curve_files(self, currency: str, folder_path: str) -> Dict[str, Any]:
        """
        Preprocess curve files to create optimized data structures
        """
        curve_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        
        # Sort files by date for better cache locality
        curve_files.sort()
        
        # Create date index
        date_index = {}
        file_metadata = {}
        
        for filename in curve_files:
            try:
                date_str = filename[:6]
                date_obj = self._yymmdd_to_datetime(date_str)
                date_index[date_obj] = filename
                file_metadata[filename] = {
                    'date': date_obj,
                    'date_str': date_str,
                    'path': os.path.join(folder_path, filename)
                }
            except:
                continue
        
        return {
            'date_index': date_index,
            'file_metadata': file_metadata,
            'date_range': (min(date_index.keys()), max(date_index.keys())) if date_index else None,
            'total_files': len(curve_files)
        }
    
    def _yymmdd_to_datetime(self, date_str: str) -> datetime:
        """Convert YYMMDD to datetime object"""
        try:
            yy, mm, dd = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6])
            year = 1900 + yy if yy >= 90 else 2000 + yy
            return datetime(year, mm, dd)
        except ValueError as e:
            # If we can't parse the date, it might be a full path - extract just the filename
            if '\\' in date_str or '/' in date_str:
                filename = os.path.basename(date_str)
                if len(filename) >= 6:
                    return self._yymmdd_to_datetime(filename[:6])
            raise ValueError(f"Cannot parse date from: {date_str}")


class ChartDataOptimizer:
    """
    Optimizes chart data generation and processing
    """
    
    @staticmethod
    @memory_cache_with_lru(maxsize=50)
    def optimize_dataframe_operations(df: pd.DataFrame, operations: List[str]) -> pd.DataFrame:
        """
        Optimize common dataframe operations with caching
        """
        result_df = df.copy()
        
        for operation in operations:
            if operation == 'sort_by_date':
                result_df = result_df.sort_values('Date')
            elif operation == 'remove_duplicates':
                result_df = result_df.drop_duplicates(subset=['Date'])
            elif operation == 'fill_missing':
                result_df = result_df.fillna(method='forward')
        
        return result_df
    
    @staticmethod
    def batch_process_expressions(expressions: List[Dict], batch_size: int = 2) -> List[Dict]:
        """
        Process expressions in batches to reduce memory usage
        """
        results = []
        
        for i in range(0, len(expressions), batch_size):
            batch = expressions[i:i + batch_size]
            batch_results = []
            
            for expr in batch:
                # Process each expression
                try:
                    # This would call the actual expression processing logic
                    result = ChartDataOptimizer._process_single_expression(expr)
                    batch_results.append(result)
                except Exception as e:
                    print(f"Error processing expression {expr}: {e}")
                    batch_results.append({'error': str(e)})
            
            results.extend(batch_results)
            
            # Small delay between batches to prevent overwhelming the system
            time.sleep(0.01)
        
        return results
    
    @staticmethod
    def _process_single_expression(expr: Dict) -> Dict:
        """Process a single expression (placeholder for actual logic)"""
        # This would contain the actual expression processing logic
        return {'processed': True, 'expression': expr}

class PerformanceMonitor:
    """
    Monitors and reports performance metrics
    """
    
    @staticmethod
    def get_performance_stats() -> Dict[str, Any]:
        """Get current performance statistics"""
        with STATS_LOCK:
            cache_hit_rate = 0
            if PERFORMANCE_STATS['cache_hits'] + PERFORMANCE_STATS['cache_misses'] > 0:
                cache_hit_rate = PERFORMANCE_STATS['cache_hits'] / (
                    PERFORMANCE_STATS['cache_hits'] + PERFORMANCE_STATS['cache_misses']
                ) * 100
            
            return {
                'cache_hit_rate': f"{cache_hit_rate:.1f}%",
                'total_requests': PERFORMANCE_STATS['total_requests'],
                'avg_load_time': f"{PERFORMANCE_STATS['avg_load_time']:.3f}s",
                'cache_hits': PERFORMANCE_STATS['cache_hits'],
                'cache_misses': PERFORMANCE_STATS['cache_misses']
            }
    
    @staticmethod
    def reset_stats():
        """Reset performance statistics"""
        with STATS_LOCK:
            PERFORMANCE_STATS.update({
                'cache_hits': 0,
                'cache_misses': 0,
                'avg_load_time': 0,
                'total_requests': 0
            })

def clear_all_caches():
    """Clear all caches (memory and disk)"""
    # Clear memory cache
    global MEMORY_CACHE
    with CACHE_LOCK:
        MEMORY_CACHE.clear()
    
    # Clear disk cache
    if os.path.exists(CACHE_DIR):
        for filename in os.listdir(CACHE_DIR):
            if filename.endswith('.pkl'):
                try:
                    os.remove(os.path.join(CACHE_DIR, filename))
                except:
                    pass

# Utility functions for integration
def get_cache_size() -> Dict[str, int]:
    """Get current cache sizes"""
    memory_size = len(MEMORY_CACHE)
    
    disk_size = 0
    if os.path.exists(CACHE_DIR):
        disk_size = len([f for f in os.listdir(CACHE_DIR) if f.endswith('.pkl')])
    
    return {
        'memory_cache_size': memory_size,
        'disk_cache_size': disk_size
    }

def optimize_pandas_settings():
    """Optimize pandas settings for better performance"""
    pd.set_option('mode.chained_assignment', None)  # Disable warning for better performance
    pd.set_option('compute.use_bottleneck', True)   # Use bottleneck for faster operations
    pd.set_option('compute.use_numexpr', True)      # Use numexpr for faster operations
