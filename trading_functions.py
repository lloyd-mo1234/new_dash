"""
Trading functions for Bloomberg integration and portfolio management
"""
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from swap_functions import get_swap_data
import json
import os
import re
import xbbg.blp as blp
from cba.analytics import xcurves as xc


class XCSwapPosition:
    """Represents a complex XC swap position that can contain multiple StandardSwap objects"""
    
    def __init__(self, handle: str, price: float, size: float, 
                 instrument: str, component_rates: Dict[str, float] = None, 
                 insertion_date: str = None):
        self.handle = handle
        self.price = price  # As decimal (e.g., 0.0314) - this is the spread price
        self.size = size    # In millions
        self.instrument = instrument  # The complex expression (e.g., "aud.10y10y-aud.5y5y")
        self.notional = size * 1000000  # Convert millions to actual notional
        self.component_rates = component_rates or {}  # Rates for each component
        self.components = []  # List of component dictionaries from parse_complex_expression
        self.xc_swaps = []  # List of individual XC StandardSwap handles
        self.xc_created = False
        self.last_pnl = 0.0
        self.insertion_date = insertion_date or datetime.now().strftime('%Y-%m-%d')  # Default to today
        
    def create_xc_swaps(self):
        """Create multiple XC StandardSwap objects for complex expressions"""
        try:
            # Parse the complex expression into components
            self.components = parse_complex_expression(self.instrument)
            
            if not self.components:
                return False             
            
            # ALWAYS recalculate component rates based on current price
            # This ensures that when price is updated, the rates are recalculated correctly
            self.component_rates = solve_component_rates(self.components, self.price)
            
            # Create XC StandardSwap for each component using DV01-based sizing
            self.xc_swaps = []
            
            for i, comp in enumerate(self.components):
                component_handle = f"{self.handle}_comp_{i}_{comp['instrument'].replace('.', '_')}"
                
                # Get the rate for this component
                component_rate = self.component_rates.get(comp['instrument'], 3.0)  # Default 3.0%
                
                
                
                
                
                
                
                
                # Step 1: Create temporary swap with 1 million notional to calculate DV01
                temp_handle = f"{component_handle}_temp_dv01"
                
                try:
                    
                    xc.StandardSwap(
                        product_handle=temp_handle,
                        template_name=comp['template'],
                        settlement_date=datetime.now().strftime('%Y-%m-%d'),
                        start_date=comp['start_date'],
                        end_date=comp['end_date'],
                        notional=1_000_000.0,  # Always 1 million for DV01 calculation
                        rate=float(component_rate / 100.0),  # Convert percentage to decimal
                        term_spread=0.0,
                        discount_curve="",
                        fx_rate=1.0,
                        roll_date=""
                    )
                    
                    # Step 2: Calculate DV01 per million using xc.DV01
                    today_yymmdd = datetime.now().strftime("%y%m%d")
                    curve_handle = f"{today_yymmdd}_core_bundle"
                    
                    
                    dv01_per_million = xc.DV01(curve_handle, temp_handle)
                    dv01_per_million = float(dv01_per_million)
                    
                    
                    
                    # Step 3: Calculate target DV01 based on size input
                    # Size input (e.g., 500) means we want DV01 = 500 * 1000 = 500,000
                    target_dv01 = self.size * 1000  # size is in units, multiply by 1000
                    
                    # Apply coefficient (for spreads, one leg is positive, one is negative)
                    target_dv01_with_coeff = target_dv01 * comp['coefficient']
                    
                    # Note: No longer distinguishing between entry/exit - all positions use same sign
                    
                    
                    
                    # Step 4: Calculate required notional to achieve target DV01
                    if abs(dv01_per_million) > 0.01:  # Avoid division by very small numbers
                        required_notional = (target_dv01_with_coeff / dv01_per_million) * 1_000_000
                    else:
                        
                        required_notional = target_dv01_with_coeff * 1000  # Fallback
                    
                    
                    
                    # Create the actual swap with calculated notional
                    
                    xc.StandardSwap(
                        product_handle=component_handle,
                        template_name=comp['template'],
                        settlement_date=datetime.now().strftime('%Y-%m-%d'),
                        start_date=comp['start_date'],
                        end_date=comp['end_date'],
                        notional=float(required_notional),
                        rate=float(component_rate / 100.0),  # Convert percentage to decimal
                        term_spread=0.0,
                        discount_curve="",
                        fx_rate=1.0,
                        roll_date=""
                    )
                    
                    self.xc_swaps.append({
                        'handle': component_handle,
                        'instrument': comp['instrument'],
                        'coefficient': comp['coefficient'],
                        'notional': required_notional,
                        'rate': component_rate,
                        'dv01_per_million': dv01_per_million,
                        'target_dv01': target_dv01_with_coeff
                    })
                    
                    
                    
                    
                    
                    
                except Exception as e:
                    
                    return False
            
            self.xc_created = True
            
            
            
            
            return True
            
        except Exception as e:
            
            
            self.xc_created = False
            return False
    
    def calculate_pnl(self, curve_handle: str = None):
        """
        Calculate single-date P&L using xc.PresentValue for all component swaps
        
        Args:
            curve_handle: Specific curve bundle handle to use (optional, defaults to today's curve)
        
        Returns:
            dict with single PnL value
        """
        
        
        
        
        if not self.xc_created or not self.xc_swaps:
            error_msg = f"XC swaps for {self.handle} not created, cannot calculate P&L"
            
            
            return {'pnl': 0.0, 'error': error_msg}
        
        # Generate today's curve bundle name if not provided
        if curve_handle is None:
            today_yymmdd = datetime.now().strftime("%y%m%d")
            curve_handle = f"{today_yymmdd}_core_bundle"
            
        try:
            
            
            total_pnl = 0.0
            component_pnls = []
            
            for swap_info in self.xc_swaps:
                swap_handle = swap_info['handle']
                
                try:
                    pnl = xc.PresentValue(curve_handle, swap_handle)
                    pnl_value = float(pnl)
                    total_pnl += pnl_value
                    
                    component_pnls.append({
                        'handle': swap_handle,
                        'instrument': swap_info['instrument'],
                        'pnl': pnl_value,
                        'coefficient': swap_info['coefficient']
                    })
                    
                    
                    
                except Exception as e:
                    
                    component_pnls.append({
                        'handle': swap_handle,
                        'instrument': swap_info['instrument'],
                        'pnl': 0.0,
                        'error': str(e)
                    })
            
            self.last_pnl = total_pnl
            
            
            return {
                'pnl': total_pnl, 
                'error': None,
                'components': component_pnls
            }
            
        except Exception as e:
            error_msg = f"xc.PresentValue error for {self.handle}: {str(e)}"
            
            return {'pnl': 0.0, 'error': error_msg}
    
    def calculate_array_pnl(self):
        """
        Calculate PnL array over multiple dates from insertion date to today
        
        Uses self.insertion_date as start date and today as end date
        
        Returns:
            dict with pnl_array: list of (date, pnl) tuples
        """
        if not self.xc_created or not self.xc_swaps:
            error_msg = f"XC swaps for {self.handle} not created, cannot calculate P&L"
            
            return {'pnl_array': [], 'error': error_msg}
        
        try:
            from loader import get_available_dates, yymmdd_to_datetime
            
            # Convert insertion_date (YYYY-MM-DD) to YYMMDD format for filtering
            if self.insertion_date:
                start_dt_converted = datetime.strptime(self.insertion_date, '%Y-%m-%d')
                start_date = start_dt_converted.strftime('%y%m%d')
            else:
                # Default to 30 days ago if no insertion date
                start_dt_converted = datetime.now() - timedelta(days=30)
                start_date = start_dt_converted.strftime('%y%m%d')
            
            # End date is today
            end_dt_converted = datetime.now()
            end_date = end_dt_converted.strftime('%y%m%d')
            
            
            
            
            # Get all available dates from loader
            all_dates = get_available_dates()
            
            # Filter dates that fall within our range (inclusive)
            start_dt = yymmdd_to_datetime(start_date)
            end_dt = yymmdd_to_datetime(end_date)
            
            filtered_dates = []
            for date_str in all_dates:
                date_dt = yymmdd_to_datetime(date_str)
                if start_dt <= date_dt <= end_dt:
                    filtered_dates.append(date_str)
            
            
            
            if not filtered_dates:
                return {
                    'pnl_array': [],
                    'error': f'No curve dates available between {start_date} and {end_date}'
                }
            
            # Calculate PnL for each date
            pnl_array = []
            
            for date_str in sorted(filtered_dates):
                # Get bundle name for this date
                from loader import get_bundle_name
                bundle_name = get_bundle_name(date_str)
                
                if not bundle_name:
                    
                    continue
                
                # Calculate total PnL for this date
                total_pnl = 0.0
                
                for swap_info in self.xc_swaps:
                    swap_handle = swap_info['handle']
                    
                    try:
                        pnl = xc.PresentValue(bundle_name, swap_handle)
                        pnl_value = float(pnl)
                        total_pnl += pnl_value
                    except Exception as e:
                        
                        continue
                
                # Convert date to datetime for output
                date_dt = yymmdd_to_datetime(date_str)
                pnl_array.append((date_dt, total_pnl))
                
            
            
            
            return {
                'pnl_array': pnl_array,
                'error': None,
                'start_date': start_date,
                'end_date': end_date,
                'num_dates': len(pnl_array)
            }
            
        except Exception as e:
            error_msg = f"Error calculating PnL array: {str(e)}"
            
            return {
                'pnl_array': [],
                'error': error_msg
            }

class XCFuturesPosition:
    """Represents a futures position for P&L calculation"""
    
    def __init__(self, handle: str, price: float, size, 
                 instrument: str, component_rates: Dict[str, float] = None,
                 insertion_date: str = None):
        self.handle = handle
        self.price = price  # Entry price (can be spread price for expressions)
        # Size can be either:
        # - Single float: for simple expressions (e.g., 25)
        # - List of floats: for multi-component expressions (e.g., [25, 30])
        self.size = size
        self.instrument = instrument  # Futures instrument name or expression
        self.last_pnl = 0.0
        self.insertion_date = insertion_date or datetime.now().strftime('%Y-%m-%d')  # Default to today
        
        # New attributes for handling futures expressions
        self.components = []  # List of component dictionaries (instrument, coefficient)
        self.component_rates = component_rates or {}  # Prices for each component
        self.component_coeff = {}  # Coefficients for each component (redundant with components, but kept for compatibility)
        self.component_sizes = {}  # Individual sizes for each component
        self.futures_built = False  # Track if futures expression has been built
    
    def build_futures_expression(self, futures_tick_data: pd.DataFrame = None):
        """Build the futures expression by parsing components and getting prices"""
        try:
            
            
            
            
            
            # Parse the instrument expression if not already done
            if not self.components:
                self.components = parse_futures_expression(self.instrument)
                if not self.components:
                    
                    return False
            # Get futures tick data if not provided
            if futures_tick_data is None or futures_tick_data.empty:
                # Extract unique instrument names from components
                unique_instruments = list(set([comp['instrument'] for comp in self.components]))
                
                
                futures_tick_data = get_futures_details(unique_instruments)
                
                if futures_tick_data is None or futures_tick_data.empty:
                    
                    return False
            
            # CRITICAL FIX: Always recalculate component prices using self.price
            # This ensures the spread price from the modal is used correctly
            
            self.component_rates = solve_futures_component_prices(
                self.components, 
                self.price, 
                futures_tick_data
            )
            
            
            self.futures_built = True
            
            return True
            
        except Exception as e:
            
            self.futures_built = False
            return False
        
    def calculate_pnl(self, futures_tick_data: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Calculate single-date P&L for futures position using tick data
        
        Args:
            futures_tick_data: DataFrame with futures contract details (tick size, tick value, current px_mid)
        
        Returns:
            dict with single PnL value
        """
        try:
            
            
            
            
            
            # Build the futures expression if not already built
            if not self.futures_built:
                if not self.build_futures_expression(futures_tick_data):
                    error_msg = f"Failed to build futures expression for {self.instrument}"
                    
                    return {'pnl': 0.0, 'error': error_msg}
            
            if futures_tick_data is None or futures_tick_data.empty:
                # Try to get futures data if not provided
                unique_instruments = list(set([comp['instrument'] for comp in self.components]))
                futures_tick_data = get_futures_details(unique_instruments)
                
                if futures_tick_data is None or futures_tick_data.empty:
                    error_msg = f"No futures tick data available for {self.instrument}"
                    
                    return {'pnl': 0.0, 'error': error_msg}
            
            # Determine if we have individual component sizes
            has_individual_sizes = isinstance(self.size, list) and len(self.size) == len(self.components)
                           
            
            # Calculate P&L for each component and sum them up
            total_pnl = 0.0
            component_pnls = []
            
            for idx, comp in enumerate(self.components):
                instrument = comp['instrument']
                coefficient = comp['coefficient']
                
                # Get the size for this component
                if has_individual_sizes:
                    component_size = self.size[idx]
                else:
                    component_size = self.size if isinstance(self.size, (int, float)) else 0
                
                # Check if instrument exists in the tick data
                if instrument not in futures_tick_data.index:
                    error_msg = f"Instrument {instrument} not found in futures tick data"
                    
                    component_pnls.append({
                        'instrument': instrument,
                        'pnl': 0.0,
                        'error': error_msg
                    })
                    continue
                
                # Get futures contract details
                instrument_data = futures_tick_data.loc[instrument]
                fut_tick_size = instrument_data['fut_tick_size']
                fut_tick_val = instrument_data['fut_tick_val']
                px_mid = instrument_data['px_mid']
                
                # Get the component price
                component_price = self.component_rates.get(instrument, px_mid)
                
                
                
                
                
                
                
                
                
                # Calculate P&L for this component: (entry_price - px_mid) / tick_size * tick_value * size * coefficient
                price_diff = component_price - px_mid
                tick_count = price_diff / fut_tick_size
                component_pnl = tick_count * fut_tick_val * component_size * coefficient
                
                
                
                
                
                
                total_pnl += component_pnl
                
                component_pnls.append({
                    'instrument': instrument,
                    'coefficient': coefficient,
                    'size': component_size,
                    'pnl': component_pnl,
                    'entry_price': component_price,
                    'px_mid': px_mid,
                    'price_diff': price_diff,
                    'error': None
                })
            
            self.last_pnl = total_pnl
            
            
            
            return {
                'pnl': total_pnl,
                'error': None,
                'components': component_pnls,
                'details': {
                    'total_components': len(self.components),
                    'size': self.size,
                    'has_individual_sizes': has_individual_sizes
                }
            }
            
        except Exception as e:
            error_msg = f"Error calculating futures P&L for {self.handle}: {str(e)}"
            
            return {'pnl': 0.0, 'error': error_msg}
    
    def calculate_array_pnl(self, futures_tick_data: pd.DataFrame, historical_prices: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate PnL array over multiple dates from insertion date to today
        
        Uses self.insertion_date as start date and today as end date
        
        Args:
            futures_tick_data: DataFrame with futures contract details (tick size, tick value)
            historical_prices: DataFrame with historical prices (rows=dates, columns=instrument names)
        
        Returns:
            dict with pnl_array: list of (date, pnl) tuples
        """
        print(f"\n🐛 DEBUG: XCFuturesPosition.calculate_array_pnl CALLED")
        print(f"   - handle: {self.handle}")
        print(f"   - instrument: {self.instrument}")
        print(f"   - insertion_date: {self.insertion_date}")
        print(f"   - futures_built: {self.futures_built}")
        print(f"   - futures_tick_data is None: {futures_tick_data is None}")
        print(f"   - futures_tick_data.empty: {futures_tick_data.empty if futures_tick_data is not None else 'N/A'}")
        print(f"   - historical_prices is None: {historical_prices is None}")
        print(f"   - historical_prices.empty: {historical_prices.empty if historical_prices is not None else 'N/A'}")
        
        if not self.futures_built:
            error_msg = f"Futures expression for {self.handle} not built, cannot calculate P&L"
            print(f"   ❌ ERROR: {error_msg}")
            return {'pnl_array': [], 'error': error_msg}
        
        try:
            # Convert insertion_date (YYYY-MM-DD) to datetime.date for filtering
            if self.insertion_date:
                start_date = datetime.strptime(self.insertion_date, '%Y-%m-%d').date()
            else:
                # Default to 30 days ago if no insertion date
                start_date = (datetime.now() - timedelta(days=30)).date()
            
            # End date is today (as date, not datetime)
            end_date = datetime.now().date()
            
            
            
            
            # Build the futures expression if not already built
            if not self.futures_built:
                if not self.build_futures_expression(futures_tick_data):
                    error_msg = f"Failed to build futures expression for {self.instrument}"
                    
                    return {'pnl_array': [], 'error': error_msg}
            
            if futures_tick_data is None or futures_tick_data.empty:
                error_msg = "No futures tick data provided for tick size/value information"
                print(f"   ❌ ERROR: {error_msg}")
                return {'pnl_array': [], 'error': error_msg}
            
            if historical_prices is None or historical_prices.empty:
                error_msg = "No historical prices DataFrame provided"
                print(f"   ❌ ERROR: {error_msg}")
                return {'pnl_array': [], 'error': error_msg}
            
            print(f"   ✓ Both futures_tick_data and historical_prices are available")
            print(f"   - futures_tick_data shape: {futures_tick_data.shape}")
            print(f"   - historical_prices shape: {historical_prices.shape}")
            
            # Filter dates that fall within our range (inclusive)
            # historical_prices index should be datetime objects
            mask = (historical_prices.index >= start_date) & (historical_prices.index <= end_date)
            filtered_prices = historical_prices[mask]
            
            if filtered_prices.empty:
                error_msg = f"No prices found between {start_date} and {end_date}"
                
                return {'pnl_array': [], 'error': error_msg}
            
            
            
            # Determine if we have individual component sizes
            has_individual_sizes = isinstance(self.size, list) and len(self.size) == len(self.components)
            
            # Calculate PnL for each date
            pnl_array = []
            
            for date_idx, date in enumerate(filtered_prices.index):
                total_pnl = 0.0
                
                # Calculate PnL for each component
                for idx, comp in enumerate(self.components):
                    instrument = comp['instrument']
                    coefficient = comp['coefficient']
                    
                    # Get the size for this component
                    if has_individual_sizes:
                        component_size = self.size[idx]
                    else:
                        component_size = self.size if isinstance(self.size, (int, float)) else 0
                    
                    # Check if instrument exists in tick data (for tick size/value)
                    if instrument not in futures_tick_data.index:
                        
                        continue
                    
                    # Check if instrument exists in historical prices
                    if instrument not in filtered_prices.columns:
                        
                        continue
                    
                    # Get futures contract details (tick size/value from tick data)
                    instrument_data = futures_tick_data.loc[instrument]
                    fut_tick_size = instrument_data['fut_tick_size']
                    fut_tick_val = instrument_data['fut_tick_val']
                    
                    # Get the market price for this date from historical_prices
                    px_mid = filtered_prices.loc[date, instrument]
                    
                    # Skip if price is NaN
                    if pd.isna(px_mid):
                        
                        continue
                    
                    # Get the entry price for this component
                    component_price = self.component_rates.get(instrument, px_mid)
                    
                    # Calculate P&L: (entry_price - px_mid) / tick_size * tick_value * size * coefficient
                    price_diff = component_price - px_mid
                    tick_count = price_diff / fut_tick_size
                    component_pnl = tick_count * fut_tick_val * component_size * coefficient
                    
                    total_pnl += component_pnl
                
                # Add this date's PnL to the array
                pnl_array.append((date, total_pnl))
                
                    
            
            
            
            return {
                'pnl_array': pnl_array,
                'error': None,
                'start_date': start_date,
                'end_date': end_date,
                'num_dates': len(pnl_array)
            }
            
        except Exception as e:
            error_msg = f"Error calculating futures PnL array: {str(e)}"
            
            import traceback
            traceback.print_exc()
            return {
                'pnl_array': [],
                'error': error_msg
            }

class Trade:
    """Object representing a single trade with XC swap positions"""
    
    def __init__(self, trade_id: str, typology=None, secondary_typology: str = None, is_temporary: bool = False, group_id: str = None):
        self.trade_id = trade_id
        self.typology = typology if isinstance(typology, list) else [typology] if typology else []  # List of trade types
        self.secondary_typology = secondary_typology  # for EFP trades (e.g., 'futures')
        self.is_temporary = is_temporary  # Flag to mark unsaved trades
        self.group_id = group_id  # Group ID field for trade grouping
        
        # For EFP trades, we have dual data structures:
        # Primary (swap) and Secondary (futures)
        self.prices = []  # Primary prices (swap prices for EFP)
        self.sizes = []  # Primary sizes (swap PV01 for EFP)
        self.instrument_details = []  # Primary instruments (swap expressions for EFP)
        self.positions = []  # Primary positions (XCSwapPosition objects for EFP)
        
        # Secondary data structures (for EFP futures leg)
        self.prices_secondary = []  # Secondary prices (futures prices for EFP)
        self.sizes_secondary = []  # Secondary sizes (list of lists for futures - each position can have multiple futures)
        self.instrument_details_secondary = []  # Secondary instruments (futures expressions for EFP)
        self.positions_secondary = []  # Secondary positions (XCFuturesPosition objects for EFP)
        
        # Position insertion dates arrays
        self.primary_pos_insertion_dt = []  # Insertion dates for primary positions
        self.secondary_pos_insertion_dt = []  # Insertion dates for secondary positions
        
        self.pnl = 0
        self.positions_pnl = 0
        
        # PnL array attribute - calculated and stored after positions are created
        self.pnl_array = []  # List of (date, pnl) tuples
        self.pnl_array_primary = []  # List of (date, pnl) tuples for primary positions only
        self.pnl_array_secondary = []  # List of (date, pnl) tuples for secondary positions only
        
    def add_position(self, price: float, size, instrument: str = None, position_type: str = 'primary'):
        """
        Add a position to the trade
        
        Args:
            price: Price for the position
            size: Size for the position (can be float or list of floats for futures with multiple components)
            instrument: Instrument for this position (optional, will use from instrument_details if not provided)
            position_type: 'primary' for main leg (swap for EFP), 'secondary' for secondary leg (futures for EFP)
        """
        if position_type == 'secondary':
            # Add to secondary lists (futures for EFP)
            self.prices_secondary.append(price)
            self.sizes_secondary.append(size)
            if instrument:
                if instrument not in self.instrument_details_secondary:
                    self.instrument_details_secondary.append(instrument)
        else:
            # Add to primary lists (swap for EFP, or standard trades)
            self.prices.append(price)
            self.sizes.append(size)
            if instrument:
                if instrument not in self.instrument_details:
                    self.instrument_details.append(instrument)
    
    def add_entry(self, price: float, size: float):
        """Legacy method - now calls add_position for backward compatibility"""
        self.add_position(price, size)
    
    def add_exit(self, price: float, size: float):
        """Legacy method - now calls add_position for backward compatibility"""
        self.add_position(price, size)
    
    def get_weighted_average_entry(self) -> float:
        """Calculate weighted average entry price - temporarily returns 0"""
        return 0.0
    
    def get_weighted_average_exit(self) -> float:
        """Calculate weighted average exit price - temporarily returns 0"""
        return 0.0
    
    def get_weighted_average_price(self) -> float: 
        return 0.0

    def create_positions(self, futures_tick_data: pd.DataFrame = None, historical_prices: pd.DataFrame = None):
        """
        Create positions for all prices and sizes based on trade typology
        
        Args:
            futures_tick_data: DataFrame with futures contract details (optional, from portfolio)
            historical_prices: DataFrame with historical futures prices (optional, from portfolio)
        """
        self.positions = []
        self.positions_secondary = []
        
        # Get instrument details and typology
        if not self.instrument_details or not self.typology:
            
            return False
            
        trade_type = self.typology[0].lower() if self.typology else 'swap'
        
        
        
        
        
        # Handle EFP trades
        if trade_type == 'efp':
            
            
            # Create primary positions (swap leg)
            if self.instrument_details:
                instrument = self.instrument_details[0]
                
                
                for i, (price, size) in enumerate(zip(self.prices, self.sizes)):
                    handle = f"{self.trade_id}_swap_position_{i}"
                    
                    
                    
                    
                    
                    # Get insertion date for this position
                    insertion_date = None
                    if hasattr(self, 'primary_pos_insertion_dt') and i < len(self.primary_pos_insertion_dt):
                        insertion_date = self.primary_pos_insertion_dt[i]
                    
                    position = XCSwapPosition(
                        handle=handle,
                        price=price,
                        size=size,
                        instrument=instrument,
                        insertion_date=insertion_date
                    )
                    
                    if position.create_xc_swaps():
                        self.positions.append(position)
                                                
            
            # Create secondary positions (futures leg)
            if self.instrument_details_secondary and self.secondary_typology in ['futures', 'future']:
                instrument = self.instrument_details_secondary[0]
                
                
                for i, (price, size) in enumerate(zip(self.prices_secondary, self.sizes_secondary)):
                    handle = f"{self.trade_id}_futures_position_{i}"
                    
                    
                    
                    
                    
                    # Get insertion date for this position
                    insertion_date = None
                    if hasattr(self, 'secondary_pos_insertion_dt') and i < len(self.secondary_pos_insertion_dt):
                        insertion_date = self.secondary_pos_insertion_dt[i]
                    
                    position = XCFuturesPosition(
                        handle=handle,
                        price=price,
                        size=size,
                        instrument=instrument,
                        insertion_date=insertion_date
                    )
                    
                    if position.build_futures_expression():
                        self.positions_secondary.append(position)
                        
                        
            
            
            # DON'T return True here - let it fall through to the array calculation at the bottom!
        
        # Handle standard swap trades
        elif trade_type == 'swap':
            instrument = self.instrument_details[0]
            
            
            for i, (price, size) in enumerate(zip(self.prices, self.sizes)):
                handle = f"{self.trade_id}_position_{i}"
                
                
                
                
                
                # Get insertion date for this position
                insertion_date = None
                if hasattr(self, 'primary_pos_insertion_dt') and i < len(self.primary_pos_insertion_dt):
                    insertion_date = self.primary_pos_insertion_dt[i]
                    
                
                position = XCSwapPosition(
                    handle=handle,
                    price=price,
                    size=size,
                    instrument=instrument,
                    insertion_date=insertion_date
                )
                
                if position.create_xc_swaps():
                    self.positions.append(position)
                    
                    
        
        # Handle standard futures trades
        elif trade_type == 'future':
            instrument = self.instrument_details[0]
            
            
            for i, (price, size) in enumerate(zip(self.prices, self.sizes)):
                handle = f"{self.trade_id}_position_{i}"
                
                
                
                
                
                # Get insertion date for this position
                insertion_date = None
                if hasattr(self, 'primary_pos_insertion_dt') and i < len(self.primary_pos_insertion_dt):
                    insertion_date = self.primary_pos_insertion_dt[i]
                    
                
                position = XCFuturesPosition(
                    handle=handle,
                    price=price,
                    size=size,
                    instrument=instrument,
                    insertion_date=insertion_date
                )
                
                if position.build_futures_expression():
                    self.positions.append(position)
                    
        else:
            return False
        
        
        
        # Calculate and store PnL arrays for all positions
        
        try:
            array_pnl_result = self.calculate_array_pnl(
                futures_tick_data=futures_tick_data,
                historical_prices=historical_prices
            )
            
            if array_pnl_result.get('error'):
                pass
            else:
                # Store the PnL arrays in the trade object
                self.pnl_array = array_pnl_result.get('pnl_array', [])
                self.pnl_array_primary = array_pnl_result.get('primary_pnl_array', [])
                self.pnl_array_secondary = array_pnl_result.get('secondary_pnl_array', [])
                
                
                
                
                
                
        except Exception as e:
            
            # Initialize empty arrays on error
            self.pnl_array = []
            self.pnl_array_primary = []
            self.pnl_array_secondary = []
        
        # Print PnL arrays to console
        print(f"\n{'='*80}")
        print(f"Trade: {self.trade_id}")
        print(f"{'='*80}")
        print(f"self.pnl_array = {self.pnl_array}")
        print(f"self.pnl_array_primary = {self.pnl_array_primary}")
        print(f"self.pnl_array_secondary = {self.pnl_array_secondary}")
        print(f"{'='*80}\n")
        
        return True
    
    def calculate_pnl(self, live_price: float = None, use_xc: bool = True, curve_handle: str = None, 
                     futures_tick_data: pd.DataFrame = None) -> Dict[str, float]:
        """Calculate PnL for the trade using position-specific methods"""
        
        if self.positions or self.positions_secondary:
            # Use position-based P&L calculation
            total_pnl = 0.0
            primary_pnl = 0.0  # Track primary (swap) P&L separately
            secondary_pnl = 0.0  # Track secondary (futures) P&L separately
            position_pnls = []
            errors = []
            
            # Calculate P&L for primary positions
            for position in self.positions:
                if isinstance(position, XCSwapPosition):
                    pnl_result = position.calculate_pnl(curve_handle)
                elif isinstance(position, XCFuturesPosition):
                    pnl_result = position.calculate_pnl(futures_tick_data)
                else:
                    pnl_result = {'pnl': 0.0, 'error': f'Unknown position type: {type(position)}'}
                
                pnl_value = pnl_result['pnl']
                pnl_error = pnl_result['error']
                
                total_pnl += pnl_value
                primary_pnl += pnl_value  # Add to primary P&L
                position_pnls.append({
                    'handle': position.handle,
                    'pnl': pnl_value,
                    'error': pnl_error,
                    'position_class': type(position).__name__,
                    'position_type': 'primary'
                })
                
                if pnl_error:
                    errors.append(f"{position.handle}: {pnl_error}")
            
            # Calculate P&L for secondary positions (EFP futures leg)
            for position in self.positions_secondary:
                if isinstance(position, XCFuturesPosition):
                    pnl_result = position.calculate_pnl(futures_tick_data)
                elif isinstance(position, XCSwapPosition):
                    pnl_result = position.calculate_pnl(curve_handle)
                else:
                    pnl_result = {'pnl': 0.0, 'error': f'Unknown position type: {type(position)}'}
                
                pnl_value = pnl_result['pnl']
                pnl_error = pnl_result['error']
                
                total_pnl += pnl_value
                secondary_pnl += pnl_value  # Add to secondary P&L
                position_pnls.append({
                    'handle': position.handle,
                    'pnl': pnl_value,
                    'error': pnl_error,
                    'position_class': type(position).__name__,
                    'position_type': 'secondary'
                })
                
                if pnl_error:
                    errors.append(f"{position.handle}: {pnl_error}")
            
            return {
                "realized_pnl": 0.0,  # Position-based gives total P&L
                "unrealized_pnl": total_pnl,
                "total_pnl": total_pnl,
                "primary_pnl": primary_pnl,  # Separate primary P&L
                "secondary_pnl": secondary_pnl,  # Separate secondary P&L
                "positions": position_pnls,
                "method": "position_based",
                "errors": errors if errors else None
            }
        else:
            # No positions created yet
            return {
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "primary_pnl": 0.0,
                "secondary_pnl": 0.0,
                "method": "no_positions"
            }
    
    def calculate_array_pnl(self, futures_tick_data: pd.DataFrame = None, 
                           historical_prices: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Calculate PnL array over multiple dates by combining arrays from all positions
        
        Uses date INTERSECTION across all positions to ensure we only include dates
        where all positions have P&L data.
        
        Args:
            futures_tick_data: DataFrame with futures contract details (for futures positions)
            historical_prices: DataFrame with historical futures prices (for futures positions)
        
        Returns:
            dict with:
            - pnl_array: List of (date, pnl) tuples
            - error: Error message if any
            - primary_pnl_array: List of (date, pnl) tuples for primary positions only
            - secondary_pnl_array: List of (date, pnl) tuples for secondary positions only
        """
        try:
            print(f"\n{'='*80}")
            print(f"🔍 CALCULATE_ARRAY_PNL CALLED FOR TRADE: {self.trade_id}")
            print(f"Trade typology: {self.typology}")
            print(f"Secondary typology: {self.secondary_typology}")
            print(f"Primary positions count: {len(self.positions)}")
            print(f"Secondary positions count: {len(self.positions_secondary)}")
            print(f"futures_tick_data provided: {futures_tick_data is not None and not futures_tick_data.empty if futures_tick_data is not None else False}")
            print(f"historical_prices provided: {historical_prices is not None and not historical_prices.empty if historical_prices is not None else False}")
            print(f"{'='*80}\n")
            
            if not self.positions and not self.positions_secondary:
                error_msg = f"No positions created for trade {self.trade_id}"
                print(f"❌ ERROR: {error_msg}")
                return {
                    'pnl_array': [],
                    'primary_pnl_array': [],
                    'secondary_pnl_array': [],
                    'error': error_msg
                }
            
            # Store all PnL arrays from each position as dictionaries: {date -> pnl}
            primary_position_pnls = []  # List of {date -> pnl} dicts for primary positions
            secondary_position_pnls = []  # List of {date -> pnl} dicts for secondary positions
            
            # Collect PnL arrays from primary positions (swaps or futures)
            for i, position in enumerate(self.positions):
                print(f"\n{'='*60}")
                print(f"PRIMARY POSITION {i}: {position.handle}")
                print(f"Position type: {type(position).__name__}")
                print(f"Position instrument: {position.instrument}")
                print(f"Position insertion_date: {position.insertion_date}")
                print(f"{'='*60}")
                
                try:
                    if isinstance(position, XCSwapPosition):
                        print(f"📊 Calling calculate_array_pnl for XCSwapPosition...")
                        # Call swap position calculate_array_pnl (no params needed)
                        result = position.calculate_array_pnl()
                        print(f"✓ Result received: error={result.get('error')}, num_dates={len(result.get('pnl_array', []))}")
                    elif isinstance(position, XCFuturesPosition):
                        print(f"📊 Calling calculate_array_pnl for XCFuturesPosition...")
                        print(f"   - futures_tick_data shape: {futures_tick_data.shape if futures_tick_data is not None else 'None'}")
                        print(f"   - historical_prices shape: {historical_prices.shape if historical_prices is not None else 'None'}")
                        # Call futures position calculate_array_pnl (needs tick data and historical prices)
                        result = position.calculate_array_pnl(futures_tick_data, historical_prices)
                        print(f"✓ Result received: error={result.get('error')}, num_dates={len(result.get('pnl_array', []))}")
                    else:
                        print(f"❌ Unknown position type: {type(position).__name__}")
                        continue
                    
                    if result.get('error'):
                        print(f"❌ ERROR: {result['error']}")
                        continue
                    
                    # Convert array list to dictionary, normalizing dates to datetime.date
                    pnl_dict = {}
                    for date, pnl in result['pnl_array']:
                        # Normalize to datetime.date for consistency
                        if isinstance(date, datetime):
                            date_key = date.date()
                        else:
                            date_key = date
                        pnl_dict[date_key] = pnl
                    primary_position_pnls.append(pnl_dict)
                    
                    print(f"✓ Array generated with {len(pnl_dict)} dates")
                    if len(pnl_dict) > 0:
                        # Show first and last date
                        dates = sorted(pnl_dict.keys())
                        print(f"  First date: {dates[0]} → PnL: {pnl_dict[dates[0]]:.2f}")
                        print(f"  Last date: {dates[-1]} → PnL: {pnl_dict[dates[-1]]:.2f}")
                        
                except Exception as e:
                    print(f"❌ EXCEPTION calculating array PnL: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            print(f"\n{'='*60}")
            print(f"TOTAL PRIMARY POSITIONS COLLECTED: {len(primary_position_pnls)}")
            print(f"{'='*60}\n")
            
            # Collect PnL arrays from secondary positions (futures for EFP)
            print(f"\n{'='*60}")
            print(f"🔍 PROCESSING SECONDARY POSITIONS (EFP FUTURES LEG)")
            print(f"Total secondary positions: {len(self.positions_secondary)}")
            print(f"{'='*60}\n")
            
            for i, position in enumerate(self.positions_secondary):
                print(f"\n{'='*60}")
                print(f"SECONDARY POSITION {i}: {position.handle}")
                print(f"Position type: {type(position).__name__}")
                print(f"Position instrument: {position.instrument}")
                print(f"Position insertion_date: {position.insertion_date}")
                print(f"Position futures_built: {position.futures_built}")
                print(f"Position components: {len(position.components)}")
                print(f"{'='*60}")
                
                try:
                    if isinstance(position, XCFuturesPosition):
                        print(f"📊 Calling calculate_array_pnl for XCFuturesPosition (SECONDARY)...")
                        print(f"   - futures_tick_data provided: {futures_tick_data is not None and not futures_tick_data.empty if futures_tick_data is not None else False}")
                        print(f"   - historical_prices provided: {historical_prices is not None and not historical_prices.empty if historical_prices is not None else False}")
                        
                        # DEBUG: Check if futures_tick_data has required instruments
                        if futures_tick_data is not None and not futures_tick_data.empty:
                            print(f"   - futures_tick_data columns: {list(futures_tick_data.columns)}")
                            print(f"   - futures_tick_data index (instruments): {list(futures_tick_data.index)}")
                            for comp in position.components:
                                instrument = comp['instrument']
                                in_tick_data = instrument in futures_tick_data.index
                                print(f"   - Component {instrument} in tick_data: {in_tick_data}")
                        
                        # DEBUG: Check if historical_prices has required instruments
                        if historical_prices is not None and not historical_prices.empty:
                            print(f"   - historical_prices columns (instruments): {list(historical_prices.columns)}")
                            print(f"   - historical_prices shape: {historical_prices.shape}")
                            for comp in position.components:
                                instrument = comp['instrument']
                                in_hist_prices = instrument in historical_prices.columns
                                print(f"   - Component {instrument} in historical_prices: {in_hist_prices}")
                        
                        # Call futures position calculate_array_pnl
                        result = position.calculate_array_pnl(futures_tick_data, historical_prices)
                        print(f"✓ Result received: error={result.get('error')}, num_dates={len(result.get('pnl_array', []))}")
                    elif isinstance(position, XCSwapPosition):
                        print(f"📊 Calling calculate_array_pnl for XCSwapPosition (SECONDARY)...")
                        # Call swap position calculate_array_pnl
                        result = position.calculate_array_pnl()
                        print(f"✓ Result received: error={result.get('error')}, num_dates={len(result.get('pnl_array', []))}")
                    else:
                        print(f"❌ Unknown position type: {type(position).__name__}")
                        continue
                    
                    if result.get('error'):
                        print(f"❌ ERROR: {result['error']}")
                        continue
                    
                    # Convert array list to dictionary, normalizing dates to datetime.date
                    pnl_dict = {}
                    for date, pnl in result['pnl_array']:
                        # Normalize to datetime.date for consistency
                        if isinstance(date, datetime):
                            date_key = date.date()
                        else:
                            date_key = date
                        pnl_dict[date_key] = pnl
                    secondary_position_pnls.append(pnl_dict)
                    
                    print(f"✓ Array generated with {len(pnl_dict)} dates")
                    if len(pnl_dict) > 0:
                        # Show first and last date
                        dates = sorted(pnl_dict.keys())
                        print(f"  First date: {dates[0]} → PnL: {pnl_dict[dates[0]]:.2f}")
                        print(f"  Last date: {dates[-1]} → PnL: {pnl_dict[dates[-1]]:.2f}")
                        
                except Exception as e:
                    print(f"❌ EXCEPTION calculating array PnL: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            print(f"\n{'='*60}")
            print(f"TOTAL SECONDARY POSITIONS COLLECTED: {len(secondary_position_pnls)}")
            print(f"{'='*60}\n")
            # Find UNION of dates across ALL positions (primary and secondary)
            all_position_pnls = primary_position_pnls + secondary_position_pnls
            
            if not all_position_pnls:
                error_msg = f"No valid P&L arrays from any positions for trade {self.trade_id}"
                
                return {
                    'pnl_array': [],
                    'primary_pnl_array': [],
                    'secondary_pnl_array': [],
                    'error': error_msg
                }
            
            # Start with empty set and take union of all dates
            all_dates = set()
            
            # Union all dates from all positions
            for pnl_dict in all_position_pnls:
                all_dates = all_dates.union(set(pnl_dict.keys()))
            
            
            
            if not all_dates:
                error_msg = f"No dates found in any positions for trade {self.trade_id}"
                
                return {
                    'pnl_array': [],
                    'primary_pnl_array': [],
                    'secondary_pnl_array': [],
                    'error': error_msg
                }
            
            # Sort dates
            sorted_dates = sorted(list(all_dates))
            
            # Calculate combined P&L for each common date
            pnl_array = []
            primary_pnl_array = []
            secondary_pnl_array = []
            
            for date in sorted_dates:
                # Sum P&L from all primary positions for this date
                primary_pnl = sum(pnl_dict[date] for pnl_dict in primary_position_pnls if date in pnl_dict)
                
                # Sum P&L from all secondary positions for this date
                secondary_pnl = sum(pnl_dict[date] for pnl_dict in secondary_position_pnls if date in pnl_dict)
                
                # Total P&L is sum of primary and secondary
                total_pnl = primary_pnl + secondary_pnl
                
                pnl_array.append((date, total_pnl))
                primary_pnl_array.append((date, primary_pnl))
                secondary_pnl_array.append((date, secondary_pnl))
                                       
            return {
                'pnl_array': pnl_array,
                'primary_pnl_array': primary_pnl_array,
                'secondary_pnl_array': secondary_pnl_array,
                'error': None,
                'num_dates': len(pnl_array),
                'num_primary_positions': len(primary_position_pnls),
                'num_secondary_positions': len(secondary_position_pnls)
            }
            
        except Exception as e:
            error_msg = f"Error calculating array P&L for trade {self.trade_id}: {str(e)}"
            
            import traceback
            traceback.print_exc()
            return {
                'pnl_array': [],
                'primary_pnl_array': [],
                'secondary_pnl_array': [],
                'error': error_msg
            }

class Portfolio:
    """Portfolio management class"""
    
    def __init__(self, storage_dir="trades"):
        self.trades = {}  # Dict of trade_id -> Trade objects
        
        # Set up storage directory and file path
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.storage_dir = os.path.join(script_dir, storage_dir)
        self.storage_file = os.path.join(self.storage_dir, "portfolio.json")
        
        # Create trades directory if it doesn't exist
        os.makedirs(self.storage_dir, exist_ok=True)
        
        # Futures tick data attribute - stored at portfolio level
        self.futures_tick_data = None
        
        # Load existing trades from file
        self.load_from_file()
        
    def add_trade(self, trade: Trade):
        """Add a trade to the portfolio (does not auto-save)"""
        self.trades[trade.trade_id] = trade
    
    def remove_trade(self, trade_id: str):
        """Remove a trade from the portfolio (does not auto-save)"""
        if trade_id in self.trades:
            del self.trades[trade_id]
            self.save_to_file()  # Auto-save after removing
    
    def save_to_file(self):
        """Save portfolio to JSON file"""
        data = {
            'portfolio_metadata': {
                'last_pnl_update': getattr(self, 'last_pnl_update', None),
                'total_portfolio_pnl': getattr(self, 'total_portfolio_pnl', 0.0)
            },
            'trades': {}
        }
        
        for trade_id, trade in self.trades.items():
            # Determine secondary_typology: 'futures' for EFP trades, None otherwise
            secondary_typology = trade.secondary_typology
            if not secondary_typology and trade.typology and 'efp' in [t.lower() for t in trade.typology]:
                secondary_typology = 'futures'
            
            trade_data = {
                'trade_id': trade.trade_id,
                'typology': trade.typology,
                'secondary_typology': secondary_typology,
                'group_id': getattr(trade, 'group_id', None),
                'prices': trade.prices,
                'sizes': trade.sizes,
                'instrument_details': trade.instrument_details,
                'stored_pnl': getattr(trade, 'stored_pnl', 0.0),
                'pnl_timestamp': getattr(trade, 'pnl_timestamp', None),
                # Always include secondary attributes (empty lists if not present)
                'prices_secondary': getattr(trade, 'prices_secondary', []),
                'sizes_secondary': getattr(trade, 'sizes_secondary', []),
                'instrument_details_secondary': getattr(trade, 'instrument_details_secondary', []),
                # Include separate P&L values for EFP trades
                'stored_pnl_primary': getattr(trade, 'stored_pnl_primary', None),
                'stored_pnl_secondary': getattr(trade, 'stored_pnl_secondary', None),
                # Include insertion date arrays
                'primary_pos_insertion_dt': getattr(trade, 'primary_pos_insertion_dt', []),
                'secondary_pos_insertion_dt': getattr(trade, 'secondary_pos_insertion_dt', [])
            }
            
            # 🐛 DEBUG: Log insertion dates being saved to JSON
            primary_dates = getattr(trade, 'primary_pos_insertion_dt', [])
            secondary_dates = getattr(trade, 'secondary_pos_insertion_dt', [])
            
            
            
            
            data['trades'][trade_id] = trade_data
        
        with open(self.storage_file, 'w') as f:
            json.dump(data, f, indent=2)
                
    def load_from_file(self):
        """Load portfolio from JSON file"""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    data = json.load(f)
                
                # Clear existing trades
                self.trades.clear()
                
                # Load portfolio metadata if it exists (new format)
                if 'portfolio_metadata' in data:
                    metadata = data['portfolio_metadata']
                    self.last_pnl_update = metadata.get('last_pnl_update')
                    self.total_portfolio_pnl = metadata.get('total_portfolio_pnl', 0.0)
                    trades_data = data.get('trades', {})
                else:
                    # Old format - no metadata
                    self.last_pnl_update = None
                    self.total_portfolio_pnl = 0.0
                    trades_data = data
                
                # Load trades from file
                for trade_data in trades_data.values():
                    trade = Trade(
                        trade_data['trade_id'],
                        trade_data['typology'],
                        trade_data.get('secondary_typology'),
                        group_id=trade_data.get('group_id')
                    )
                    
                    # Handle both old format (entry/exit arrays) and new format (prices/sizes arrays)
                    if 'prices' in trade_data and 'sizes' in trade_data:
                        # New format - use prices and sizes directly
                        trade.prices = trade_data.get('prices', [])
                        trade.sizes = trade_data.get('sizes', [])                       
                   
                    # Handle both old format (string) and new format (list) for instrument_details
                    instrument_details = trade_data.get('instrument_details', [])
                    if isinstance(instrument_details, str):
                        trade.instrument_details = [instrument_details] if instrument_details else []
                    else:
                        trade.instrument_details = instrument_details
                    
                    # Load secondary data structures (for EFP trades)
                    trade.prices_secondary = trade_data.get('prices_secondary', [])
                    trade.sizes_secondary = trade_data.get('sizes_secondary', [])
                    
                    instrument_details_secondary = trade_data.get('instrument_details_secondary', [])
                    if isinstance(instrument_details_secondary, str):
                        trade.instrument_details_secondary = [instrument_details_secondary] if instrument_details_secondary else []
                    else:
                        trade.instrument_details_secondary = instrument_details_secondary
                    
                    # CRITICAL FIX: Always initialize positions_secondary as empty list
                    trade.positions_secondary = []
                    
                    # Load stored P&L data (new format)
                    trade.stored_pnl = trade_data.get('stored_pnl', 0.0)
                    trade.pnl_timestamp = trade_data.get('pnl_timestamp')
                    
                    # Load insertion date arrays
                    trade.primary_pos_insertion_dt = trade_data.get('primary_pos_insertion_dt', [])
                    trade.secondary_pos_insertion_dt = trade_data.get('secondary_pos_insertion_dt', [])
                    
                    self.trades[trade.trade_id] = trade                
                     
        except Exception as e:
            self.trades = {}  # Start with empty portfolio if loading fails
    
    def initialize_positions(self):
        """Initialize positions for all trades in the portfolio using new position system"""
        
        
        # Get futures tick data if we have futures trades and it's not already loaded
        if self.futures_tick_data is None:
            futures_instruments = get_futures_instrument_names(self)
            if futures_instruments:
                
                self.futures_tick_data = get_futures_details(futures_instruments)
        
        # Get historical prices for futures if needed
        historical_prices = None
        if self.futures_tick_data is not None and not self.futures_tick_data.empty:
            historical_prices = get_futures_history(self)
        
        total_positions = 0
        successful_trades = 0
        
        for trade_id, trade in self.trades.items():
            
            
            # Pass futures_tick_data and historical_prices to create_positions
            if trade.create_positions(
                futures_tick_data=self.futures_tick_data,
                historical_prices=historical_prices
            ):
                successful_trades += 1
                total_positions += len(trade.positions) + len(trade.positions_secondary)
                
                
        
        
        return successful_trades, total_positions

    def calculate_portfolio_pnl_xc(self, curve_handle: str = None, futures_tick_data: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Calculate portfolio P&L for both futures and swap trades
        Saves PnL of each trade to JSON and returns PnL information
        """
        try:
            
            
            # Generate today's curve bundle name if not provided
            if curve_handle is None:
                today_yymmdd = datetime.now().strftime("%y%m%d")
                curve_handle = f"{today_yymmdd}_core_bundle"
            
            # Get futures tick data if not provided and we have futures trades
            if futures_tick_data is None:
                futures_instruments = get_futures_instrument_names(self)
                if futures_instruments:
                    
                    futures_tick_data = get_futures_details(futures_instruments)
                else:
                    futures_tick_data = pd.DataFrame()
            
            total_pnl = 0.0
            trade_pnls = {}
            current_timestamp = datetime.now().isoformat()
            
            for trade_id, trade in self.trades.items():
                
                
                # Use trade.calculate_pnl() directly
                pnl_result = trade.calculate_pnl(
                    curve_handle=curve_handle,
                    futures_tick_data=futures_tick_data
                )
                
                # Extract the PnL values
                trade_pnl = pnl_result.get('total_pnl', 0.0)
                primary_pnl = pnl_result.get('primary_pnl', 0.0)
                secondary_pnl = pnl_result.get('secondary_pnl', 0.0)
                
                # Save P&L to the trade object
                trade.stored_pnl = trade_pnl
                trade.pnl_timestamp = current_timestamp
                
                # For EFP trades, also store separate P&L values
                if trade.typology and 'efp' in [t.lower() for t in trade.typology]:
                    trade.stored_pnl_primary = primary_pnl
                    trade.stored_pnl_secondary = secondary_pnl
                    
                
                # Add to total
                total_pnl += trade_pnl
                
                # Store trade details
                trade_type = 'unknown'
                if trade.typology:
                    if 'swap' in [t.lower() for t in trade.typology]:
                        trade_type = 'swap'
                    elif 'future' in [t.lower() for t in trade.typology]:
                        trade_type = 'futures'
                
                trade_pnls[trade_id] = {
                    'pnl': trade_pnl,
                    'trade_type': trade_type,
                    'method': pnl_result.get('method', 'unknown'),
                    'positions': len(trade.positions) if hasattr(trade, 'positions') else 0,
                    'timestamp': current_timestamp,
                    'errors': pnl_result.get('errors', None)
                }
                
                
            
            # Update portfolio-level metadata
            self.last_pnl_update = current_timestamp
            self.total_portfolio_pnl = total_pnl
            
            # Save to JSON file
            
            self.save_to_file()
            
            
            
            return {
                'success': True,
                'total_pnl': total_pnl,
                'trade_pnls': trade_pnls,
                'timestamp': current_timestamp,
                'curve_handle': curve_handle,
                'method': 'combined_futures_and_swaps',
                'trades_processed': len(self.trades),
                'message': f'Portfolio P&L: ${total_pnl:,.2f} ({len(self.trades)} trades processed)'
            }
            
        except Exception as e:
            error_msg = f"Error in portfolio P&L calculation: {str(e)}"
            
            return {
                'success': False,
                'error': error_msg,
                'total_pnl': 0.0,
                'timestamp': datetime.now().isoformat()
            }

    def get_trade_details(self, trade_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific trade"""
        if trade_id not in self.trades:
            return {"error": f"Trade {trade_id} not found"}
        
        trade = self.trades[trade_id]
        
        # Use stored P&L if available, otherwise calculate on-the-fly
        stored_pnl = getattr(trade, 'stored_pnl', None)
        pnl_timestamp = getattr(trade, 'pnl_timestamp', None)
        
        if stored_pnl is not None:
            # Use stored P&L values from JSON
            pnl = {
                "realized_pnl": 0.0,
                "unrealized_pnl": stored_pnl,
                "total_pnl": stored_pnl,
                "method": "stored_from_json",
                "timestamp": pnl_timestamp
            }
            
        else:
            # Fallback to calculating P&L on-the-fly (without live price)
            pnl = trade.calculate_pnl()
            pnl["method"] = "calculated_on_demand"
            
        return {
            "trade_id": trade.trade_id,
            "typology": trade.typology,
            "group_id": getattr(trade, 'group_id', None),
            "instrument_details": trade.instrument_details,
            "prices": trade.prices,
            "sizes": trade.sizes,
            "pnl": pnl,
            # Add secondary data structures for EFP trades
            "prices_secondary": getattr(trade, 'prices_secondary', []),
            "sizes_secondary": getattr(trade, 'sizes_secondary', []),
            "instrument_details_secondary": getattr(trade, 'instrument_details_secondary', []),
            # CRITICAL FIX: Include insertion date arrays
            "primary_pos_insertion_dt": getattr(trade, 'primary_pos_insertion_dt', []),
            "secondary_pos_insertion_dt": getattr(trade, 'secondary_pos_insertion_dt', [])
        }

    def update_realtime_pnl(self) -> Dict[str, Any]:
        
        
        self.load_from_file()
        
        # Clear any existing XC positions to force recreation with updated instrument details
        for trade in self.trades.values():
            trade.position = []
        
        # Initialize XC positions for all trades with fresh instrument details
        successful_trades, total_positions = self.initialize_positions()
        
        if total_positions == 0:
            return {
                'success': False,
                'error': 'No XC positions could be created',
                'timestamp': datetime.now().isoformat()
            }
        
        # Calculate P&L using XC
        pnl_result = self.calculate_portfolio_pnl_xc()
        
        # Store P&L results in each trade with timestamp
        current_timestamp = datetime.now().isoformat()
        total_portfolio_pnl = 0.0
        
        for trade_id, trade in self.trades.items():
            if trade_id in pnl_result['trade_pnls']:
                trade_pnl = pnl_result['trade_pnls'][trade_id]['pnl']
                trade.stored_pnl = trade_pnl
                trade.pnl_timestamp = current_timestamp
                total_portfolio_pnl += trade_pnl
                
            else:
                trade.stored_pnl = 0.0
                trade.pnl_timestamp = current_timestamp
        
        # Store portfolio-level metadata
        self.last_pnl_update = current_timestamp
        self.total_portfolio_pnl = total_portfolio_pnl
        
        # Save to file
        self.save_to_file()
        
        
        
        return {
            'success': True,
            'total_pnl': total_portfolio_pnl,
            'timestamp': current_timestamp,
            'trades_updated': len([t for t in self.trades.values() if hasattr(t, 'stored_pnl')]),
            'total_positions': total_positions,
            'message': f'P&L updated for {successful_trades} trades with {total_positions} positions'
        }

def parse_instrument_dates(instrument: str) -> Dict[str, str]:
    """
    Parse instrument string to extract start and end dates
    Examples:
    - aud.5y5y -> 5 years forward starting, 5 years tenor
    - aud.5y5y.10y10y -> Complex structure
    """
    try:
        # Get today's date as reference
        today = datetime.now()
        
        # Parse the instrument syntax
        # Format: currency.forward_period.tenor or currency.tenor
        parts = instrument.lower().split('.')
        
        if len(parts) < 2:
            return None
            
        currency = parts[0]
        
        # Handle different formats
        if len(parts) == 2:
            # Simple format: aud.5y5y (forward + tenor)
            period_str = parts[1]
            
            # Parse forward period and tenor (e.g., "5y5y")
            if re.match(r'\d+[ymd]\d+[ymd]', period_str):
                # Extract forward period and tenor
                match = re.match(r'(\d+)([ymd])(\d+)([ymd])', period_str)
                if match:
                    forward_num, forward_unit, tenor_num, tenor_unit = match.groups()
                    
                    # Calculate start date (today + forward period)
                    if forward_unit == 'y':
                        start_date = today + timedelta(days=int(forward_num) * 365)
                    elif forward_unit == 'm':
                        start_date = today + timedelta(days=int(forward_num) * 30)
                    else:  # days
                        start_date = today + timedelta(days=int(forward_num))
                    
                    # Calculate end date (start + tenor)
                    if tenor_unit == 'y':
                        end_date = start_date + timedelta(days=int(tenor_num) * 365)
                    elif tenor_unit == 'm':
                        end_date = start_date + timedelta(days=int(tenor_num) * 30)
                    else:  # days
                        end_date = start_date + timedelta(days=int(tenor_num))
                    
                    return {
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'forward_period': f"{forward_num}{forward_unit}",
                        'tenor': f"{tenor_num}{tenor_unit}"
                    }
            
            # Simple tenor format: aud.5y (just tenor, starts today)
            elif re.match(r'\d+[ymd]', period_str):
                match = re.match(r'(\d+)([ymd])', period_str)
                if match:
                    tenor_num, tenor_unit = match.groups()
                    
                    start_date = today
                    
                    # Calculate end date
                    if tenor_unit == 'y':
                        end_date = start_date + timedelta(days=int(tenor_num) * 365)
                    elif tenor_unit == 'm':
                        end_date = start_date + timedelta(days=int(tenor_num) * 30)
                    else:  # days
                        end_date = start_date + timedelta(days=int(tenor_num))
                    
                    return {
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'forward_period': '0d',
                        'tenor': f"{tenor_num}{tenor_unit}"
                    }
        
        # Default fallback for complex instruments
        return {
            'start_date': today.strftime('%Y-%m-%d'),
            'end_date': (today + timedelta(days=5*365)).strftime('%Y-%m-%d'),  # 5 year default
            'forward_period': '0d',
            'tenor': '5y'
        }
        
    except Exception as e:
        
        return None

def get_template_from_instrument(instrument: str) -> str:
    """
    Map instrument to XC template name based on actual swap_functions.py templates
    Examples:
    - aud.5y5y -> AUDIRS-SS
    - usd.5y5y -> USDSOFR
    - eur.5y5y -> EURIRS-AS
    """
    try:
        currency = instrument.lower().split('.')[0]
        
        # Template mapping based on actual CURRENCY_CONFIG from swap_functions.py
        template_map = {
            'aud': 'AUDIRS-SS',
            'audbs': 'BBSW-SOFR',
            'audxc': 'AONIA-SOFR',
            'audbob': 'AUDBOB-3M',
            'aud6s3s': 'AUDBASIS-6X3',
            'gbp': 'GBPOIS',
            'gbpxc': 'SONIA-SOFR',
            'usd': 'USDSOFR',
            'eur': 'EURIRS-AS',
            'eurxc': 'ESTR-SOFR',
            'eurbob': 'EURESTR-EURIBOR3M',
            'eur6s3s': 'EURBASIS-6X3',
            'jpy': 'JPYOIS',
            'jpyxc': 'TONAR-SOFR',
            'cad': 'CADOIS',
            'cadxc': 'CORRA-SOFR',
            'nzd': 'NZDIRS-SQ',
            'nzdbs': 'BKBM-SOFR',
            'nzdxc': 'NZOCR-SOFR'
        }
        
        return template_map.get(currency, 'AUDIRS-SS')  # Default to AUD
        
    except Exception as e:
        
        return 'AUDIRS-SS'  # Default fallback

def parse_complex_expression(expression: str) -> List[Dict[str, Any]]:
    """
    Parse complex arithmetic expressions and return individual swap components.
    
    Examples:
    - "aud.10y10y-aud.5y5y" -> Two swaps with coefficients +1 and -1
    - "2*aud.5y5y + eur.10y10y - gbp.2y2y" -> Three swaps with coefficients +2, +1, -1
    - "aud.5y5y.10y10y.20y10y" -> Butterfly: 2*10y10y - 5y5y - 20y10y
    
    Returns:
        List of dictionaries, each containing:
        - instrument: The individual instrument (e.g., "aud.5y5y")
        - coefficient: The multiplier for this instrument (+1, -1, +2, etc.)
        - start_date: Start date for the swap
        - end_date: End date for the swap
        - template: XC template name
    """
    
    try:
        
        
        # Handle built-in structures first (butterfly, spreads)
        if '.' in expression and not any(op in expression for op in ['+', '-', '*', '/']):
            # Check for built-in structures
            parts = expression.lower().split('.')
            
            if len(parts) == 3:
                currency, part1, part2 = parts
                
                # Check if it's a fixed-date swap: aud.130526.1y (DDMMYY.tenor format)
                if re.match(r'^\d{6}$', part1) and re.match(r'^\d+[ymd]$', part2):
                    # Fixed-date swap - single instrument, not a spread
                    dates = parse_instrument_dates(expression)
                    if dates:
                        return [{
                            'instrument': expression,
                            'coefficient': 1.0,
                            'start_date': dates['start_date'],
                            'end_date': dates['end_date'],
                            'template': get_template_from_instrument(expression)
                        }]
                
                # Spot spread: aud.5y.10y -> aud.0y10y - aud.0y5y
                elif re.match(r'^\d+[ymd]$', part1) and re.match(r'^\d+[ymd]$', part2):
                    instrument1 = f"{currency}.0y{part2}"  # Long end
                    instrument2 = f"{currency}.0y{part1}"  # Short end
                    
                    components = []
                    
                    # Add long end with +1 coefficient
                    dates1 = parse_instrument_dates(instrument1)
                    if dates1:
                        components.append({
                            'instrument': instrument1,
                            'coefficient': 1.0,
                            'start_date': dates1['start_date'],
                            'end_date': dates1['end_date'],
                            'template': get_template_from_instrument(instrument1)
                        })
                    
                    # Add short end with -1 coefficient
                    dates2 = parse_instrument_dates(instrument2)
                    if dates2:
                        components.append({
                            'instrument': instrument2,
                            'coefficient': -1.0,
                            'start_date': dates2['start_date'],
                            'end_date': dates2['end_date'],
                            'template': get_template_from_instrument(instrument2)
                        })
                    
                    return components
                
                # Forward spread: aud.5y5y.10y10y -> aud.10y10y - aud.5y5y
                else:
                    instrument1 = f"{currency}.{part2}"  # Long end
                    instrument2 = f"{currency}.{part1}"  # Short end
                    
                    components = []
                    
                    # Add long end with +1 coefficient
                    dates1 = parse_instrument_dates(instrument1)
                    if dates1:
                        components.append({
                            'instrument': instrument1,
                            'coefficient': 1.0,
                            'start_date': dates1['start_date'],
                            'end_date': dates1['end_date'],
                            'template': get_template_from_instrument(instrument1)
                        })
                    
                    # Add short end with -1 coefficient
                    dates2 = parse_instrument_dates(instrument2)
                    if dates2:
                        components.append({
                            'instrument': instrument2,
                            'coefficient': -1.0,
                            'start_date': dates2['start_date'],
                            'end_date': dates2['end_date'],
                            'template': get_template_from_instrument(instrument2)
                        })
                    
                    return components
            
            elif len(parts) == 4:
                # Butterfly: aud.5y5y.10y10y.20y10y -> 2*10y10y - 5y5y - 20y10y
                currency, tenor1, tenor2, tenor3 = parts
                
                instrument1 = f"{currency}.{tenor1}"  # Wing 1: -1
                instrument2 = f"{currency}.{tenor2}"  # Body: +2
                instrument3 = f"{currency}.{tenor3}"  # Wing 2: -1
                
                components = []
                
                # Add body with +2 coefficient
                dates2 = parse_instrument_dates(instrument2)
                if dates2:
                    components.append({
                        'instrument': instrument2,
                        'coefficient': 2.0,
                        'start_date': dates2['start_date'],
                        'end_date': dates2['end_date'],
                        'template': get_template_from_instrument(instrument2)
                    })
                
                # Add wing 1 with -1 coefficient
                dates1 = parse_instrument_dates(instrument1)
                if dates1:
                    components.append({
                        'instrument': instrument1,
                        'coefficient': -1.0,
                        'start_date': dates1['start_date'],
                        'end_date': dates1['end_date'],
                        'template': get_template_from_instrument(instrument1)
                    })
                
                # Add wing 2 with -1 coefficient
                dates3 = parse_instrument_dates(instrument3)
                if dates3:
                    components.append({
                        'instrument': instrument3,
                        'coefficient': -1.0,
                        'start_date': dates3['start_date'],
                        'end_date': dates3['end_date'],
                        'template': get_template_from_instrument(instrument3)
                    })
                
                return components
            
            else:
                # Single instrument
                dates = parse_instrument_dates(expression)
                if dates:
                    return [{
                        'instrument': expression,
                        'coefficient': 1.0,
                        'start_date': dates['start_date'],
                        'end_date': dates['end_date'],
                        'template': get_template_from_instrument(expression)
                    }]
        
        # Handle arithmetic expressions: aud.10y10y-aud.5y5y, 2*aud.5y5y + eur.10y10y - gbp.2y2y
        
        # Find all instrument patterns in the expression
        # Pattern matches: currency.tenor or currency.date.tenor
        instrument_pattern = r'[a-z0-9]+\.(?:\d{6}\.\d+[ymd]|\d+[ymd]\d+[ymd]|\d+[ymd])'
        instruments = re.findall(instrument_pattern, expression.lower())
        
        if not instruments:
            
            return []
        
        
        
        # Parse the expression to extract coefficients
        components = []
        
        # Replace instruments with placeholders and track positions
        temp_expr = expression.lower()
        instrument_positions = {}
        
        for i, instrument in enumerate(instruments):
            placeholder = f"__INST_{i}__"
            instrument_positions[placeholder] = instrument
            temp_expr = temp_expr.replace(instrument, placeholder, 1)  # Replace only first occurrence
        
        
        
        # Split by + and - while keeping the operators
        # This regex splits on + or - but keeps them in the result
        parts = re.split(r'(\+|\-)', temp_expr)
        parts = [part.strip() for part in parts if part.strip()]
        
        
        
        # Process each part to extract coefficient and instrument
        current_sign = 1  # Start with positive
        
        for part in parts:
            if part == '+':
                current_sign = 1
                continue
            elif part == '-':
                current_sign = -1
                continue
            
            # Extract coefficient and instrument placeholder
            coefficient = current_sign
            
            # Check for explicit coefficient (e.g., "2*__INST_0__")
            if '*' in part:
                coeff_part, inst_part = part.split('*', 1)
                try:
                    coefficient = float(coeff_part.strip()) * current_sign
                except ValueError:
                    coefficient = current_sign
                instrument_placeholder = inst_part.strip()
            else:
                instrument_placeholder = part.strip()
            
            # Find the actual instrument
            if instrument_placeholder in instrument_positions:
                instrument = instrument_positions[instrument_placeholder]
                
                # Get dates and template for this instrument
                dates = parse_instrument_dates(instrument)
                if dates:
                    components.append({
                        'instrument': instrument,
                        'coefficient': coefficient,
                        'start_date': dates['start_date'],
                        'end_date': dates['end_date'],
                        'template': get_template_from_instrument(instrument)
                    })
                    
                    
        
        return components
        
    except Exception as e:
        
        return []

def solve_component_rates(components: List[Dict[str, Any]], spread_price: float) -> Dict[str, float]:
    """
    Solve for individual component rates given a spread price.
    
    For expression like A - B = X, given X (spread_price):
    - Get par rate for A, use that as rate for A
    - Solve for B: B = A - X
    
    For expression like 2*A - B - C = X:
    - Get par rates for A and B
    - Solve for C: C = 2*A - B - X
    
    Args:
        components: List of component dictionaries from parse_complex_expression
        spread_price: The observed spread price (e.g., 0.15 for 15bp)
        
    Returns:
        Dictionary mapping instrument -> rate to use for XC StandardSwap creation
    """
    try:
        
        
        # Get par rates for all components
        par_rates = {}
        for comp in components:
            instrument = comp['instrument']
            try:
                df, error = get_swap_data(instrument)
                if error or df is None or df.empty:
                    
                    par_rates[instrument] = 3.0  # Default fallback
                else:
                    par_rate = df['Rate'].iloc[-1]  # Most recent rate
                    par_rates[instrument] = par_rate
                    
            except Exception as e:
                
                par_rates[instrument] = 3.0  # Default fallback
        
        # Find the component with the largest absolute coefficient to solve for
        # This will be the "unknown" we solve for
        max_coeff = 0
        solve_for_instrument = None
        
        for comp in components:
            abs_coeff = abs(comp['coefficient'])
            if abs_coeff > max_coeff:
                max_coeff = abs_coeff
                solve_for_instrument = comp['instrument']
        
        if not solve_for_instrument:
            
            return par_rates
        
        
        
        # Calculate the sum of all other components
        other_sum = 0.0
        for comp in components:
            if comp['instrument'] != solve_for_instrument:
                other_sum += comp['coefficient'] * par_rates[comp['instrument']]
        
        # Solve for the unknown component
        # Formula: coeff_unknown * rate_unknown = spread_price - other_sum
        solve_coeff = None
        for comp in components:
            if comp['instrument'] == solve_for_instrument:
                solve_coeff = comp['coefficient']
                break
        
        if solve_coeff == 0:
            
            return par_rates
        
        solved_rate = (spread_price - other_sum) / solve_coeff
        
        
        
        
        # Update the rates dictionary
        component_rates = par_rates.copy()
        component_rates[solve_for_instrument] = solved_rate
        
        # Verify the calculation
        verification = sum(comp['coefficient'] * component_rates[comp['instrument']] for comp in components)
        
        
        return component_rates
        
    except Exception as e:
        
        # Return par rates as fallback
        return {comp['instrument']: 3.0 for comp in components}

def calculate_swap_portfolio_pnl(portfolio) -> Dict[str, Any]:
    """
    Calculate P&L for swap trades only using XC positions
    Assumes curves have already been loaded
    
    Args:
        portfolio: Portfolio object containing trades
        
    Returns:
        Dictionary with P&L results for swap trades only
    """
    try:
        
        
        # Check if curves are loaded
        from loader import is_curves_loaded
        if not is_curves_loaded():
            return {
                'success': False,
                'error': 'Curves not loaded, cannot calculate P&L',
                'timestamp': datetime.now().isoformat()
            }
        
        trades_updated = 0
        total_portfolio_pnl = 0.0
        swap_trades_processed = 0
        
        for trade_id, trade in portfolio.trades.items():
            # Filter for swap trades only
            if not trade.typology or 'swap' not in [t.lower() for t in trade.typology]:
                
                continue
                
            swap_trades_processed += 1
            
            
            # Get the first instrument for P&L calculations
            if not trade.instrument_details or len(trade.instrument_details) == 0:
                
                continue
                
            instrument = trade.instrument_details[0]
            
            
            trade_total_pnl = 0.0
            position_count = 0
            
            # Calculate P&L for all entry positions
            for i, (price, size) in enumerate(zip(trade.entry_prices, trade.entry_sizes)):
                if price and size:  # Only process non-zero positions
                    
                    
                    try:
                        temp_handle = f"{trade_id}_entry_{i}_pnl_calc"
                        
                        # Convert price from decimal (0.0315) to percentage (3.15) if needed
                        price_percentage = price * 100 if price < 1 else price
                        
                        position = XCSwapPosition(
                            handle=temp_handle,
                            price=price_percentage,
                            size=size,
                            instrument=instrument
                        )
                        
                        if position.create_xc_swaps():
                            pnl_result = position.calculate_pnl()
                            position_pnl = pnl_result['pnl']
                            trade_total_pnl += position_pnl
                            position_count += 1
                            
                            
                            
                    except Exception as e:
                        return e
                        
            
            # Calculate P&L for all exit positions
            for i, (price, size) in enumerate(zip(trade.exit_prices, trade.exit_sizes)):
                if price and size:  # Only process non-zero positions
                    
                    
                    try:
                        temp_handle = f"{trade_id}_exit_{i}_pnl_calc"
                        
                        # Convert price from decimal to percentage if needed
                        price_percentage = price * 100 if price < 1 else price
                        
                        position = XCSwapPosition(
                            handle=temp_handle,
                            price=price_percentage,
                            size=size,
                            instrument=instrument
                        )
                        
                        if position.create_xc_swaps():
                            pnl_result = position.calculate_pnl()
                            position_pnl = pnl_result['pnl']
                            trade_total_pnl += position_pnl
                            position_count += 1
                            
                            
                            
                    except Exception as e:
                        pass
            
            # Store the calculated P&L in the trade object
            if position_count > 0:
                trade.stored_pnl = trade_total_pnl
                trade.pnl_timestamp = datetime.now().isoformat()
                total_portfolio_pnl += trade_total_pnl
                trades_updated += 1
                
                
        
        # Update portfolio-level metadata
        portfolio.last_pnl_update = datetime.now().isoformat()
        portfolio.total_portfolio_pnl = total_portfolio_pnl
        
        # Save the updated portfolio to JSON file
        
        portfolio.save_to_file()
        
        
        
        
        return {
            'success': True,
            'total_pnl': total_portfolio_pnl,
            'timestamp': portfolio.last_pnl_update,
            'trades_updated': trades_updated,
            'swap_trades_processed': swap_trades_processed,
            'total_positions': sum(len(trade.entry_prices) + len(trade.exit_prices) 
                                 for trade in portfolio.trades.values() 
                                 if trade.typology and 'swap' in [t.lower() for t in trade.typology]),
            'message': f"P&L calculated for {trades_updated} swap trades"
        }
        
    except Exception as e:
        
        return {'success': False, 'error': str(e)}

def get_futures_instrument_names(portfolio) -> List[str]:
    """
    Extract all unique futures instrument names from the portfolio
    
    Args:
        portfolio: Portfolio object containing trades
        
    Returns:
        List of unique futures instrument names
    """
    try:
        futures_instruments = set()
        
        for trade_id, trade in portfolio.trades.items():
            # Check if trade has futures typology (standard futures trades)
            if trade.typology and 'future' in [t.lower() for t in trade.typology]:
                # Add all instrument details for this futures trade
                for instrument in trade.instrument_details:
                    if instrument:
                        futures_instruments.add(instrument)
            
            # Check if trade is EFP with secondary futures leg
            if trade.typology and 'efp' in [t.lower() for t in trade.typology]:
                if hasattr(trade, 'instrument_details_secondary') and trade.instrument_details_secondary:
                    # Add all secondary instrument details for EFP trades
                    for instrument in trade.instrument_details_secondary:
                        if instrument:
                            futures_instruments.add(instrument)
                            
        
        futures_list = list(futures_instruments)
        
        return futures_list
        
    except Exception as e:
        
        return []

def get_futures_history(portfolio) -> pd.DataFrame:
    """
    Get historical futures prices using Bloomberg BDH
    
    Automatically determines date range:
    - Start date: Earliest insertion date across all positions in the portfolio
    - End date: Today
    
    Args:
        portfolio: Portfolio object containing trades
    
    Returns:
        DataFrame with:
        - Index: datetime dates
        - Columns: futures instrument names (as they appear in the expressions)
        - Values: historical prices (px_last)
    """
    try:
        # Find the earliest insertion date across all trades' positions
        earliest_date = None
        
        for trade in portfolio.trades.values():
            # Check primary positions
            if hasattr(trade, 'primary_pos_insertion_dt'):
                for date_str in trade.primary_pos_insertion_dt:
                    if date_str:
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                            if earliest_date is None or date_obj < earliest_date:
                                earliest_date = date_obj
                        except:
                            pass
            
            # Check secondary positions (for EFP trades)
            if hasattr(trade, 'secondary_pos_insertion_dt'):
                for date_str in trade.secondary_pos_insertion_dt:
                    if date_str:
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                            if earliest_date is None or date_obj < earliest_date:
                                earliest_date = date_obj
                        except:
                            pass
        
        # Use earliest date or fall back to 30 days ago
        if earliest_date:
            start_date = earliest_date.strftime('%Y%m%d')
            
        else:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            
        
        # End date is today
        end_date = datetime.now().strftime('%Y%m%d')
        
        
        
        # Get all futures instrument names from the portfolio
        futures_instruments = get_futures_instrument_names(portfolio)
        
        if not futures_instruments:
            
            return pd.DataFrame()
        
        # Use Bloomberg BDH to get historical data
        # BDH returns DataFrame with MultiIndex columns (ticker, field)
        
        hist_df = blp.bdh(
            tickers=futures_instruments,
            flds=['px_last'],
            start_date=start_date,
            end_date=end_date
        )
        
        if hist_df is None or hist_df.empty:
            
            return pd.DataFrame()
        
        # BDH returns MultiIndex columns: (ticker, field)
        # We need to flatten to just ticker names
        if isinstance(hist_df.columns, pd.MultiIndex):
            # Extract just the ticker level (level 0)
            hist_df.columns = hist_df.columns.get_level_values(0)
        
        
        
        
        
        
        # Display sample data (first 3 and last 3 rows)
        
        return hist_df
        
    except Exception as e:
        
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def get_fx_detail_df(fx_instruments: List[str]) -> pd.DataFrame:
    """
    Get FX futures contract details using Bloomberg BDP
    
    Args:
        fx_instruments: List of FX instrument names (e.g., ["eurusd curncy"])
        
    Returns:
        DataFrame with columns: fut_tick_size, fut_tick_val, px_mid
        Index: instrument names
    """
    try:
        
        
        if not fx_instruments:
            
            return pd.DataFrame()
        
        # Create the result DataFrame with the required columns
        result_df = pd.DataFrame(index=fx_instruments, columns=['fut_tick_size', 'fut_tick_val', 'px_mid'])
        
        # Get FX rates needed for tick value calculation
        fx_tickers_needed = []
        
        for instrument in fx_instruments:
            # Extract last 3 characters before "curncy" (e.g., "usd" from "audusd curncy")
            # Split by space and take the first part, then get last 3 characters
            instrument_base = instrument.split()[0] if ' ' in instrument else instrument
            if len(instrument_base) >= 6:  # Need at least 6 chars for xxxyyy pattern
                last_three = instrument_base[-3:].upper()  # Last 3 characters
                fx_ticker = f"{last_three}AUD Curncy"
                fx_tickers_needed.append(fx_ticker)
                
        
        # Get the FX rates
        fx_rates = {}
        if fx_tickers_needed:
            try:
                
                fx_df = blp.bdp(tickers=fx_tickers_needed, flds=['px_mid'])
                
                for fx_ticker in fx_df.index:
                    fx_rate = fx_df.loc[fx_ticker, 'px_mid']
                    currency = fx_ticker.replace('AUD Curncy', '').strip()
                    fx_rates[currency] = fx_rate
                    
                    
            except Exception as e:
                
                # Use default rate of 1.0 if we can't get FX data
                for instrument in fx_instruments:
                    instrument_base = instrument.split()[0] if ' ' in instrument else instrument
                    if len(instrument_base) >= 6:
                        last_three = instrument_base[-3:].upper()
                        fx_rates[last_three] = 1.0
        
        # Get px_mid for the original instruments
        try:
            
            px_df = blp.bdp(tickers=fx_instruments, flds=['px_mid'])
            
            for instrument in fx_instruments:
                if instrument in px_df.index:
                    result_df.loc[instrument, 'px_mid'] = px_df.loc[instrument, 'px_mid']
                    
        except Exception as e:
            pass
        
        # Calculate tick size and tick value for each instrument
        for instrument in fx_instruments:
            # Tick size is always 0.0001 for FX
            result_df.loc[instrument, 'fut_tick_size'] = 0.0001
            
            # Tick value = price for "xxxAUD curncy" * 100 (where xxx is last 3 chars)
            instrument_base = instrument.split()[0] if ' ' in instrument else instrument
            if len(instrument_base) >= 6:
                last_three = instrument_base[-3:].upper()  # Last 3 characters
                fx_rate = fx_rates.get(last_three, 1.0)
                tick_value = fx_rate * 100
                result_df.loc[instrument, 'fut_tick_val'] = tick_value
                
                
                
                
                
        
        return result_df
        
    except Exception as e:
        
        return pd.DataFrame()

def get_futures_details(futures_instruments: List[str]) -> pd.DataFrame:
    """
    Get futures contract details using Bloomberg BDP
    Routes to FX handler if first element ends in "curncy"
    
    Args:
        futures_instruments: List of futures instrument names
        
    Returns:
        DataFrame with columns: fut_tick_size, fut_tick_val, px_mid
        Index: instrument names
    """
    try:
        
        
        if not futures_instruments:
            
            return pd.DataFrame()
        
        # Check if first element ends in "curncy" - if so, route to FX handler
        if futures_instruments[0].lower().endswith("curncy"):
            
            return get_fx_detail_df(futures_instruments)
    
        # Original logic for non-FX futures
        # Define the fields we want to retrieve (including currency)
        fields = ['fut_tick_size', 'fut_tick_val', 'px_mid', 'crncy']
        
        
        
        
        # Use Bloomberg BDP to get the data - correct parameter names are 'tickers' and 'flds'
        df = blp.bdp(tickers=futures_instruments, flds=fields)
        
        # CURRENCY CONVERSION: Adjust fut_tick_val by currency to convert to AUD
        if 'crncy' in df.columns and 'fut_tick_val' in df.columns:
            
            
            # Create currency tickers for FX rates
            ccy_tickers = []
            unique_currencies = df['crncy'].dropna().unique()
            
            for ccy in unique_currencies:
                if ccy.upper() != 'AUD':  # Don't need FX rate for AUD
                    fx_ticker = f"{ccy.upper()}AUD Curncy"
                    ccy_tickers.append(fx_ticker)
                    
            
            # Get FX rates if we have non-AUD currencies
            fx_rates = {}
            if ccy_tickers:
                try:
                    
                    fx_df = blp.bdp(tickers=ccy_tickers, flds=['px_mid'])
                    
                    for fx_ticker in fx_df.index:
                        fx_rate = fx_df.loc[fx_ticker, 'px_mid']
                        currency = fx_ticker.replace('AUD Curncy', '').strip()
                        fx_rates[currency] = fx_rate
                        
                        
                except Exception as e:
                    
                    
                    for ccy in unique_currencies:
                        if ccy.upper() != 'AUD':
                            fx_rates[ccy.upper()] = 1.0
            
            # Apply currency conversion to fut_tick_val
            
            for instrument in df.index:
                if pd.notna(df.loc[instrument, 'crncy']) and pd.notna(df.loc[instrument, 'fut_tick_val']):
                    currency = df.loc[instrument, 'crncy'].upper()
                    original_tick_val = df.loc[instrument, 'fut_tick_val']
                    
                    if currency == 'AUD':
                        # Already in AUD, no conversion needed
                        converted_tick_val = original_tick_val
                        
                    else:
                        # Convert to AUD using FX rate
                        fx_rate = fx_rates.get(currency, 1.0)
                        converted_tick_val = original_tick_val * fx_rate
                        df.loc[instrument, 'fut_tick_val'] = converted_tick_val
                        

        
        return df
        
    except Exception as e:
        
        return pd.DataFrame()

def calculate_futures_portfolio_pnl(portfolio, futures_tick_data: pd.DataFrame = None) -> Dict[str, Any]:
    """
    Calculate P&L for futures trades only using futures tick data
    
    Args:
        portfolio: Portfolio object containing trades
        futures_tick_data: DataFrame with futures contract details
        
    Returns:
        Dictionary with P&L results for futures trades only
    """
    try:
        
        
        trades_updated = 0
        total_portfolio_pnl = 0.0
        futures_trades_processed = 0
        
        for trade_id, trade in portfolio.trades.items():
            # Filter for futures trades only
            if not trade.typology or 'future' not in [t.lower() for t in trade.typology]:
                
                continue
                
            futures_trades_processed += 1
            
            
            # Get the first instrument for P&L calculations
            if not trade.instrument_details or len(trade.instrument_details) == 0:
                
                continue
                
            instrument = trade.instrument_details[0]
            
            
            trade_total_pnl = 0.0
            position_count = 0
            
            # Calculate P&L for all entry positions
            for i, (price, size) in enumerate(zip(trade.entry_prices, trade.entry_sizes)):
                if price and size:  # Only process non-zero positions
                    
                    
                    try:
                        temp_handle = f"{trade_id}_entry_{i}_pnl_calc"
                        
                        position = XCFuturesPosition(
                            handle=temp_handle,
                            price=price,
                            size=size,
                            instrument=instrument
                        )
                        
                        pnl_result = position.calculate_pnl(futures_tick_data)
                        position_pnl = pnl_result['pnl']
                        trade_total_pnl += position_pnl
                        position_count += 1
                        
                            
                    except Exception as e:
                        pass
                        
            
            # Calculate P&L for all exit positions
            for i, (price, size) in enumerate(zip(trade.exit_prices, trade.exit_sizes)):
                if price and size:  # Only process non-zero positions
                    
                    
                    try:
                        temp_handle = f"{trade_id}_exit_{i}_pnl_calc"
                        
                        position = XCFuturesPosition(
                            handle=temp_handle,
                            price=price,
                            size=size,
                            instrument=instrument
                        )
                        
                        pnl_result = position.calculate_pnl(futures_tick_data)
                        position_pnl = pnl_result['pnl']
                        trade_total_pnl += position_pnl
                        position_count += 1
                        
                            
                    except Exception as e:
                        pass
                        
            
            # Store the calculated P&L in the trade object
            if position_count > 0:
                trade.stored_pnl = trade_total_pnl
                trade.pnl_timestamp = datetime.now().isoformat()
                total_portfolio_pnl += trade_total_pnl
                trades_updated += 1
                
                
        
        # Update portfolio-level metadata
        portfolio.last_pnl_update = datetime.now().isoformat()
        portfolio.total_portfolio_pnl = total_portfolio_pnl
        
        # Save the updated portfolio to JSON file
        
        portfolio.save_to_file()
        
        
        
        
        return {
            'success': True,
            'total_pnl': total_portfolio_pnl,
            'timestamp': portfolio.last_pnl_update,
            'trades_updated': trades_updated,
            'futures_trades_processed': futures_trades_processed,
            'total_positions': sum(len(trade.entry_prices) + len(trade.exit_prices) 
                                 for trade in portfolio.trades.values() 
                                 if trade.typology and 'future' in [t.lower() for t in trade.typology]),
            'message': f"P&L calculated for {trades_updated} futures trades",
            'futures_tick_data_shape': futures_tick_data.shape if futures_tick_data is not None else None
        }
        
    except Exception as e:
        
        return {'success': False, 'error': str(e)}

def parse_futures_expression(expression: str) -> List[Dict[str, Any]]:
    """
    Parse futures expressions into components.
    
    Examples:
    - "xmz5 comdty-ymz5 comdty" -> Two futures with coefficients +1 and -1
    - "xmz5 comdty - ymz5 comdty" -> Two futures with coefficients +1 and -1 (with spaces)
    - "2*irh5 comdty - irh6 comdty - irz7 comdty" -> Three futures with coefficients +2, -1, -1
    - "xmz5 comdty" -> Single future with coefficient +1
    
    Returns:
        List of dictionaries, each containing:
        - instrument: The individual instrument (e.g., "xmz5 comdty")
        - coefficient: The multiplier for this instrument (+1, -1, +2, etc.)
    """
    try:
        
        
        # Normalize the expression - remove extra spaces and make lowercase for parsing
        expr = expression.strip().lower()
        
        # Pattern to match futures instruments (e.g., "xmz5 comdty", "irh5 comdty")
        # Matches: alphanumeric + whitespace + "comdty" or other suffixes
        instrument_pattern = r'[a-z0-9]+\s+(?:comdty|curncy|index)'
        
        # Find all instruments in the expression
        instruments = re.findall(instrument_pattern, expr)
        
        if not instruments:
            # If no instruments found, treat the whole expression as a single instrument
            
            return [{
                'instrument': expression.strip(),
                'coefficient': 1.0
            }]
        
        
        
        # Parse the expression to extract coefficients
        components = []
        
        # Replace instruments with placeholders and track positions
        temp_expr = expr
        instrument_positions = {}
        
        for i, instrument in enumerate(instruments):
            placeholder = f"__FUT_{i}__"
            instrument_positions[placeholder] = instrument.strip()
            temp_expr = temp_expr.replace(instrument, placeholder, 1)  # Replace only first occurrence
        
        
        
        # Split by + and - while keeping the operators
        parts = re.split(r'(\+|\-)', temp_expr)
        parts = [part.strip() for part in parts if part.strip()]
        
        
        
        # Process each part to extract coefficient and instrument
        current_sign = 1  # Start with positive
        
        for part in parts:
            if part == '+':
                current_sign = 1
                continue
            elif part == '-':
                current_sign = -1
                continue
            
            # Extract coefficient and instrument placeholder
            coefficient = current_sign
            
            # Check for explicit coefficient (e.g., "2*__FUT_0__")
            if '*' in part:
                coeff_part, inst_part = part.split('*', 1)
                try:
                    coefficient = float(coeff_part.strip()) * current_sign
                except ValueError:
                    coefficient = current_sign
                instrument_placeholder = inst_part.strip()
            else:
                instrument_placeholder = part.strip()
            
            # Find the actual instrument
            if instrument_placeholder in instrument_positions:
                instrument = instrument_positions[instrument_placeholder]
                
                components.append({
                    'instrument': instrument,
                    'coefficient': coefficient
                })
                
                
        
        return components
        
    except Exception as e:
        
        return []

def solve_futures_component_prices(components: List[Dict[str, Any]], spread_price: float, 
                                   futures_tick_data: pd.DataFrame) -> Dict[str, float]:
    """
    Solve for individual component prices given a spread price.
    
    For expression like A - B = X, given X (spread_price):
    - Get current price for A from futures_tick_data
    - Solve for B: B = A - X
    
    For expression like 2*A - B - C = X:
    - Get current prices for A and B from futures_tick_data
    - Solve for C: C = 2*A - B - X
    
    Args:
        components: List of component dictionaries from parse_futures_expression
        spread_price: The observed spread price
        futures_tick_data: DataFrame with current prices (px_mid)
        
    Returns:
        Dictionary mapping instrument -> price to use for P&L calculation
    """
    try:
        
        
        # Get current prices for all components from futures_tick_data
        current_prices = {}
        for comp in components:
            instrument = comp['instrument']
            
            if futures_tick_data is None or futures_tick_data.empty:
                
                current_prices[instrument] = spread_price
            elif instrument in futures_tick_data.index:
                px_mid = futures_tick_data.loc[instrument, 'px_mid']
                current_prices[instrument] = px_mid
                
            else:
                
                current_prices[instrument] = spread_price
        
        # Find the component with the largest absolute coefficient to solve for
        max_coeff = 0
        solve_for_instrument = None
        
        for comp in components:
            abs_coeff = abs(comp['coefficient'])
            if abs_coeff > max_coeff:
                max_coeff = abs_coeff
                solve_for_instrument = comp['instrument']
        
        if not solve_for_instrument:
            
            return current_prices
        
        
        
        # Calculate the sum of all other components
        other_sum = 0.0
        for comp in components:
            if comp['instrument'] != solve_for_instrument:
                other_sum += comp['coefficient'] * current_prices[comp['instrument']]
        
        # Solve for the unknown component
        # Formula: coeff_unknown * price_unknown = spread_price - other_sum
        solve_coeff = None
        for comp in components:
            if comp['instrument'] == solve_for_instrument:
                solve_coeff = comp['coefficient']
                break
        
        if solve_coeff == 0:
            
            return current_prices
        
        solved_price = (spread_price - other_sum) / solve_coeff
        
        
        
        
        # Update the prices dictionary
        component_prices = current_prices.copy()
        component_prices[solve_for_instrument] = solved_price
        
        # Verify the calculation
        verification = sum(comp['coefficient'] * component_prices[comp['instrument']] for comp in components)
        
        
        return component_prices
        
    except Exception as e:
        
        # Return current prices as fallback
        return {comp['instrument']: spread_price for comp in components}
