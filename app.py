from flask import Flask, render_template, request, jsonify
import plotly.graph_objs as go
import plotly.utils
import json
import re
import pandas as pd
from datetime import datetime, timedelta
import threading
import logging
from loader import (
    initialize_curves,
    initialize_historical_curves_only,
    add_realtime_bundle,
    is_curves_loaded,
    clear_curves,
    get_cache_stats
)
from swap_functions import get_swap_data, get_status
from regression_functions import prepare_regression_data, perform_regression_analysis, create_regression_charts, format_regression_statistics
from trading_functions import *

app = Flask(__name__)

# Global portfolio instance
portfolio = Portfolio()

# Disable Flask request logging
log = logging.getLogger('werkzeug')


# Clear any existing curves cache on startup to ensure clean state
clear_curves()

# Don't start loading automatically - wait for web page visit
print("🎬 Flask app ready - curve loading will start when web page is accessed")

# Dark theme configuration
DARK_THEME = {
    'plot_bgcolor': '#1e1e1e',  # VS Code dark grey chart background
    'paper_bgcolor': '#1e1e1e',  # VS Code dark grey paper background
    'font_color': '#ffffff',
    'grid_color': '#2a4a5a',  # Much more faint blue-grey grid color
    'line_color': '#00d4ff'
}

def parse_tenor_expression(expression):
    """Parse expressions containing tenor syntax like aud.2y1y-aud.1y1y or aud.130526.1y"""
    # Find all tenor syntax patterns in the expression (updated for new fixed-date format)
    # Supports: aud.1y1y, aud.5y5y.10y10y, aud.5y5y.10y10y.20y10y, aud.130526.1y, aud6s3s.1y1y
    tenor_pattern = r'[a-z0-9]+\.(?:\d{6}\.\d+[ymd]|\d+[ymd]\d+[ymd](?:\.\d+[ymd]\d+[ymd])?(?:\.\d+[ymd]\d+[ymd])?)'
    tenors = re.findall(tenor_pattern, expression.lower())
    
    if not tenors:
        return None, []
    
    # Create a mapping of tenor syntax to placeholder variables
    tenor_map = {}
    for i, tenor in enumerate(set(tenors)):
        tenor_map[tenor] = f'__tenor_{i}__'
    
    # Replace tenor syntax with placeholders in the expression
    parsed_expr = expression.lower()
    for tenor, placeholder in tenor_map.items():
        parsed_expr = parsed_expr.replace(tenor, placeholder)
    
    return parsed_expr, list(tenor_map.keys())

def parse_expression(expression, data_cache):
    """Parse mathematical expressions like 2*A, B-A, 2*B-A-C"""
    expression = expression.strip()
    
    # If it's a simple tenor syntax, return it as is
    if re.match(r'^[a-z]+\.\d+[ymd]\.\d+[ymd]$', expression.lower()):
        return expression
    
    # Replace variable references (A, B, C, etc.) with their values
    def replace_var(match):
        var = match.group(0)
        if var in data_cache:
            return f"data_cache['{var}']"
        else:
            raise ValueError(f"Variable {var} not found")
    
    # Replace variables in the expression
    parsed_expr = re.sub(r'[A-Z]', replace_var, expression)
    
    return parsed_expr

def filter_data_by_range(df, range_filter):
    """Filter dataframe by date range"""
    if range_filter == 'MAX' or df is None or df.empty:
        return df
    
    end_date = df['Date'].max()
    
    if range_filter == '1M':
        start_date = end_date - timedelta(days=30)
    elif range_filter == '3M':
        start_date = end_date - timedelta(days=90)
    elif range_filter == '6M':
        start_date = end_date - timedelta(days=180)
    elif range_filter == '1Y':
        start_date = end_date - timedelta(days=365)
    elif range_filter == '3Y':
        start_date = end_date - timedelta(days=365*3)
    elif range_filter == '5Y':
        start_date = end_date - timedelta(days=365*5)
    elif range_filter == '10Y':
        start_date = end_date - timedelta(days=365*10)
    else:
        return df
    
    return df[df['Date'] >= start_date]

def calculate_total_trade_pnl(trade):
    """Calculate total P&L for a trade - IMPROVED to use existing positions when available"""
    try:
        # Check if curves are available
        from loader import is_curves_loaded
        if not is_curves_loaded():
            print(f"⚠️ Curves not loaded, cannot calculate P&L for trade {trade.trade_id}")
            return None
        
        print(f"🧮 Calculating total P&L for trade: {trade.trade_id}")
        
        # EFFICIENCY IMPROVEMENT: Try to use existing positions first
        if (hasattr(trade, 'positions') and 
            trade.positions and 
            all(hasattr(pos, 'xc_created') and pos.xc_created for pos in trade.positions)):
            
            print(f"✅ Using existing positions with XC swaps already created")
            total_pnl = 0.0
            
            for i, position in enumerate(trade.positions):
                pnl_result = position.calculate_pnl()
                position_pnl = pnl_result['pnl']
                total_pnl += position_pnl
                print(f"   Existing position {i} P&L: ${position_pnl:,.2f}")
            
            print(f"✅ Total trade P&L (using existing positions): ${total_pnl:,.2f}")
            return total_pnl
        
        # EFFICIENCY IMPROVEMENT: If positions exist but XC swaps not created, create them
        elif (hasattr(trade, 'positions') and trade.positions):
            print(f"🔧 Found existing positions but XC swaps not created, creating now...")
            total_pnl = 0.0
            positions_with_xc = 0
            
            for i, position in enumerate(trade.positions):
                if position.create_xc_swaps():
                    pnl_result = position.calculate_pnl()
                    position_pnl = pnl_result['pnl']
                    total_pnl += position_pnl
                    positions_with_xc += 1
                    print(f"   Position {i} P&L (XC created): ${position_pnl:,.2f}")
                else:
                    print(f"   Failed to create XC swaps for existing position {i}")
            
            if positions_with_xc > 0:
                print(f"✅ Total trade P&L (created XC for existing positions): ${total_pnl:,.2f}")
                return total_pnl
        
        # FALLBACK: Create temporary positions if no existing positions
        print(f"🔧 No existing positions found, creating temporary positions...")
        
        # Import required functions
        from trading_functions import XCSwapPosition
        
        total_pnl = 0.0
        position_count = 0
        
        # Calculate P&L for all positions
        for i, (price, size) in enumerate(zip(trade.prices, trade.sizes)):
            if not trade.instrument_details:
                continue
                
            instrument = trade.instrument_details[0]  # Use first instrument
            handle = f"{trade.trade_id}_position_{i}_temp_pnl_calc"
            
            print(f"🔧 Creating temporary position {i}: price={price}, size={size}, instrument={instrument}")
            
            position = XCSwapPosition(
                handle=handle,
                price=price,  # Already in percentage terms
                size=size,
                instrument=instrument
            )
            
            if position.create_xc_swaps():
                pnl_result = position.calculate_pnl()
                position_pnl = pnl_result['pnl']
                total_pnl += position_pnl
                position_count += 1
                print(f"   Temporary position {i} P&L: ${position_pnl:,.2f}")
            else:
                print(f"   Failed to create XC swaps for temporary position {i}")
        
        print(f"✅ Total trade P&L (temporary positions): ${total_pnl:,.2f} ({position_count} positions)")
        return total_pnl
        
    except Exception as e:
        print(f"❌ Error calculating total trade P&L for {trade.trade_id}: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/regression')
def regression():
    return render_template('regression.html')

@app.route('/run_regression', methods=['POST'])
def run_regression():
    try:
        data = request.get_json()
        y_variable = data.get('y_variable')
        x_variables = data.get('x_variables', [])
        range_filter = data.get('range', '6M')
        
        if not y_variable:
            return jsonify({'error': 'Y variable is required'}), 400
        
        if not x_variables or len(x_variables) == 0:
            return jsonify({'error': 'At least one X variable is required'}), 400
        
        # Prepare regression data
        prepared_data = prepare_regression_data(y_variable, x_variables, range_filter)
        if 'error' in prepared_data:
            return jsonify({'error': prepared_data['error']}), 400
        
        # Perform regression analysis
        regression_results = perform_regression_analysis(prepared_data)
        if 'error' in regression_results:
            return jsonify({'error': regression_results['error']}), 400
        
        # Create charts
        charts_raw = create_regression_charts(regression_results, DARK_THEME)
        if 'error' in charts_raw:
            return jsonify({'error': charts_raw['error']}), 400
        
        # Convert charts to JSON strings
        charts = {}
        for chart_name, chart_data in charts_raw.items():
            if isinstance(chart_data, dict) and 'error' in chart_data:
                charts[chart_name] = json.dumps(chart_data)
            elif hasattr(chart_data, 'to_dict'):  # It's a Plotly Figure object
                charts[chart_name] = json.dumps(chart_data, cls=plotly.utils.PlotlyJSONEncoder)
            else:
                charts[chart_name] = chart_data  # Already JSON string
        
        # Format statistics
        formatted_stats = format_regression_statistics(regression_results)
        if 'error' in formatted_stats:
            return jsonify({'error': formatted_stats['error']}), 400
        
        return jsonify({
            'success': True,
            'charts': charts,
            'statistics': formatted_stats
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/curves_status')
def curves_status():
    """Check if curves are loaded"""
    loaded = is_curves_loaded()
    return jsonify({'loaded': loaded})

@app.route('/start_loading', methods=['POST'])
def start_loading():
    """Start historical curve loading in background thread"""
    loaded = is_curves_loaded()
    
    if not loaded:
        # Start loading historical curves only
        def load_thread():
            try:
                result = initialize_historical_curves_only(max_days=200)
            except Exception as e:
                pass
        
        threading.Thread(target=load_thread, daemon=True).start()
        return jsonify({'success': True, 'message': 'Historical curve loading started'})
    else:
        return jsonify({'success': True, 'message': 'Historical curves already loaded'})

# Global variable to track real-time loading status
realtime_loading_status = {'loading': False, 'completed': False, 'error': None}

@app.route('/start_realtime_loading', methods=['POST'])
def start_realtime_loading():
    """Start real-time curve loading in background thread"""
    global realtime_loading_status
    
    # Check if already loading
    if realtime_loading_status['loading']:
        return jsonify({'success': False, 'message': 'Real-time loading already in progress'})
    
    # Reset status
    realtime_loading_status = {'loading': True, 'completed': False, 'error': None}
    
    # Real-time loading can be done regardless of historical loading status
    def realtime_load_thread():
        global realtime_loading_status
        try:
            result = add_realtime_bundle(['aud', 'eur', 'gbp', 'jpy', 'nzd', 'cad'])
            realtime_loading_status = {'loading': False, 'completed': True, 'error': None, 'result': result}
        except Exception as e:
            realtime_loading_status = {'loading': False, 'completed': False, 'error': str(e)}
    
    threading.Thread(target=realtime_load_thread, daemon=True).start()
    return jsonify({'success': True, 'message': 'Real-time curve loading started'})

@app.route('/realtime_loading_status')
def realtime_loading_status_check():
    """Check the status of real-time loading"""
    global realtime_loading_status
    return jsonify(realtime_loading_status)

@app.route('/clear_cache', methods=['POST'])
def clear_cache():
    """Clear all caches"""
    try:
        clear_curves()
        return jsonify({'success': True, 'message': 'Curves cache cleared'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    stats = get_cache_stats()
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'curves_loaded': stats['loaded'],
        'bundle_count': stats.get('bundle_count', 0)
    })


@app.route('/realtime_status')
def realtime_status():
    """Get real-time functionality status"""
    try:
        status = get_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_realtime_rates', methods=['POST'])
def get_realtime_rates():
    """Get real-time rates for specific expressions"""
    try:
        data = request.get_json()
        expressions = data.get('expressions', [])
        
        if not expressions:
            return jsonify({'error': 'No expressions provided'}), 400
        
        rates = {}
        base_rates = {}  # Store base rates for mathematical expressions
        
        # First pass: Get rates for simple expressions (non-mathematical)
        for expr_data in expressions:
            label = expr_data['label']
            expression = expr_data['expression'].strip()
            trade_type = expr_data.get('type', '').lower()  # Get trade type from request
            
            if not expression:
                rates[label] = '--'
                continue
            
            # Check if it's a mathematical expression with standalone variables (A, B, C, etc.)
            # Use word boundaries to avoid matching letters within words like "aud"
            import re
            if re.search(r'\b[A-J]\b', expression.upper()):
                # Skip mathematical expressions for now, handle in second pass
                continue
            
            # Handle futures expressions
            if trade_type == 'future':
                try:
                    from trading_functions import get_futures_details
                    
                    # Get futures details using Bloomberg BDP
                    futures_df = get_futures_details([expression])
                    
                    if futures_df is not None and not futures_df.empty and expression in futures_df.index:
                        # Get the last price from the futures data
                        px_mid = futures_df.loc[expression, 'px_mid']
                        rates[label] = f"{px_mid:.3f}"
                        base_rates[label] = px_mid
                        print(f"✅ Futures rate for {expression}: {px_mid}")
                    else:
                        rates[label] = '--'
                        base_rates[label] = None
                        print(f"❌ No futures data for {expression}")
                    
                except Exception as e:
                    print(f"❌ Error getting futures rate for {expression}: {e}")
                    rates[label] = '--'
                    base_rates[label] = None
                
                continue
            
            # Handle swap expressions only
            else:
                # Check if it's a complex expression (contains arithmetic operators with instruments)
                if any(op in expression for op in ['+', '-', '*', '/']) and re.search(r'[a-z]+\.\d+[ymd]', expression.lower()):
                    # This is a complex expression like "aud.5y5y-eur.5y5y"
                    try:
                        from trading_functions import parse_complex_expression, solve_component_rates
                        
                        # Parse the complex expression
                        components = parse_complex_expression(expression)
                        
                        if not components:
                            rates[label] = '--'
                            continue
                        
                        # Get par rates for all components
                        par_rates = {}
                        for comp in components:
                            instrument = comp['instrument']
                            try:
                                df, error = get_swap_data(instrument)
                                if error or df is None or df.empty:
                                    par_rates[instrument] = 3.0  # Default fallback
                                else:
                                    par_rate = df['Rate'].iloc[-1]
                                    par_rates[instrument] = par_rate
                            except Exception as e:
                                par_rates[instrument] = 3.0  # Default fallback
                        
                        # Calculate the spread value
                        spread_value = sum(comp['coefficient'] * par_rates[comp['instrument']] for comp in components)
                        
                        rates[label] = f"{spread_value:.3f}%"
                        base_rates[label] = spread_value
                        
                    except Exception as e:
                        print(f"Error calculating complex expression for {expression}: {e}")
                        rates[label] = '--'
                        base_rates[label] = None
                    
                    continue
                
                try:
                    # Get the latest rate for this simple swap expression
                    df, error = get_swap_data(expression)
                    if error or df is None or df.empty:
                        rates[label] = '--'
                        base_rates[label] = None
                        continue
                    
                    # Get the most recent rate
                    latest_rate = df['Rate'].iloc[-1]
                    rates[label] = f"{latest_rate:.3f}%"
                    base_rates[label] = latest_rate
                    
                except Exception as e:
                    print(f"Error getting swap rate for {expression}: {e}")
                    rates[label] = '--'
                    base_rates[label] = None
                
                continue
            
        # Second pass: Handle mathematical expressions with variables
        for expr_data in expressions:
            label = expr_data['label']
            expression = expr_data['expression'].strip()
            
            if not expression:
                continue
                
            # Check if it's a mathematical expression with standalone variables
            if re.search(r'\b[A-J]\b', expression.upper()):
                try:
                    # Replace variables with their actual rates
                    calc_expression = expression.upper()
                    
                    # Find all variable references in the expression
                    import re
                    variables_in_expr = re.findall(r'[A-J]', calc_expression)
                    
                    # Check if we have all required variables
                    missing_vars = []
                    for var in variables_in_expr:
                        if var not in base_rates or base_rates[var] is None:
                            missing_vars.append(var)
                    
                    if missing_vars:
                        rates[label] = '--'
                        continue
                    
                    # Replace variables with their numeric values
                    for var in variables_in_expr:
                        if var in base_rates and base_rates[var] is not None:
                            calc_expression = calc_expression.replace(var, str(base_rates[var]))
                    
                    # Evaluate the mathematical expression
                    try:
                        result = eval(calc_expression)
                        rates[label] = f"{result:.3f}%"
                    except:
                        rates[label] = '--'
                        
                except Exception as e:
                    print(f"Error calculating mathematical expression for {label}: {expression} - {e}")
                    rates[label] = '--'
        
        return jsonify({
            'success': True,
            'rates': rates
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update_chart', methods=['POST'])
def update_chart():
    try:
        data = request.get_json()
        expressions = data.get('expressions', [])
        range_filter = data.get('range', 'MAX')
        
        if not expressions:
            return jsonify({'error': 'No expressions provided'}), 400
        
        # Create Plotly figure
        fig = go.Figure()
        
        colors = ['#00d4ff', '#ff6b6b', '#51cf66', '#ffd43b', '#9775fa', '#ff9f43', '#a55eea', '#26de81']
        
        # Cache for storing data by variable name
        data_cache = {}
        
        for i, expr_data in enumerate(expressions):
            label = expr_data['label']
            expression = expr_data['expression']
            axis = expr_data.get('axis', 'left')
            
            try:
                # Check if it's a simple tenor syntax (including template-embedded currency codes like aud6s3s)
                # Supports: aud.10y (simple outright), aud.5y5y (forward), aud.130526.1y (fixed date)
                if (re.match(r'^[a-z0-9]+\.\d+[ymd]$', expression.lower()) or 
                    re.match(r'^[a-z0-9]+\.\d+[ymd]\d+[ymd]$', expression.lower()) or 
                    re.match(r'^[a-z0-9]+\.\d{6}\.\d+[ymd]$', expression.lower())):
                    # Simple tenor syntax
                    df, error = get_swap_data(expression)
                    if error or df is None or df.empty:
                        continue
                    
                    # Filter by range
                    df = filter_data_by_range(df, range_filter)
                    if df is None or df.empty:
                        continue
                    
                    data_cache[label] = df
                    
                    # Only add trace if visible
                    if expr_data.get('visible', True):
                        fig.add_trace(go.Scatter(
                            x=df['Date'],
                            y=df['Rate'],
                            mode='lines',
                            name=f'{label}: {expression.upper()}',
                            line=dict(
                                color=colors[i % len(colors)],
                                width=2
                            ),
                            yaxis='y2' if axis == 'right' else 'y',
                            hovertemplate='<b>Date:</b> %{x}<br><b>Rate:</b> %{y:.3f}%<extra></extra>'
                        ))
                
                else:
                    # Check if it's a tenor syntax expression (e.g., aud.2y.1y-aud.1y.1y)
                    parsed_expr, tenors_needed = parse_tenor_expression(expression)
                    
                    if parsed_expr and tenors_needed:
                        # Direct tenor syntax expression
                        tenor_data = {}
                        
                        # Load data for each tenor
                        for tenor in tenors_needed:
                            df, error = get_swap_data(tenor)
                            if error or df is None or df.empty:
                                continue
                            tenor_data[tenor] = df
                        
                        if not tenor_data:
                            continue
                        
                        # Find common dates across all tenor datasets
                        common_dates = None
                        for tenor, df in tenor_data.items():
                            if common_dates is None:
                                common_dates = set(df['Date'])
                            else:
                                common_dates = common_dates.intersection(set(df['Date']))
                        
                        if not common_dates:
                            continue
                        
                        # Create result dataframe
                        result_dates = sorted(list(common_dates))
                        result_rates = []
                        
                        for date in result_dates:
                            # Get values for this date from all tenors
                            tenor_values = {}
                            for tenor, df in tenor_data.items():
                                rate_row = df[df['Date'] == date]
                                if not rate_row.empty:
                                    tenor_values[tenor] = rate_row['Rate'].iloc[0]
                            
                            # Evaluate the expression
                            try:
                                # Replace tenor syntax with their values in the parsed expression
                                eval_expr = parsed_expr
                                for tenor, value in tenor_values.items():
                                    placeholder = f'__tenor_{tenors_needed.index(tenor)}__'
                                    eval_expr = eval_expr.replace(placeholder, str(value))
                                
                                # Evaluate the mathematical expression
                                result = eval(eval_expr)
                                result_rates.append(result)
                            except:
                                result_rates.append(None)
                        
                        # Create result dataframe
                        result_df = pd.DataFrame({
                            'Date': result_dates,
                            'Rate': result_rates
                        })
                        
                        # Remove rows with None values
                        result_df = result_df.dropna()
                        
                        # Filter by range
                        result_df = filter_data_by_range(result_df, range_filter)
                        
                        if result_df is None or result_df.empty:
                            continue
                        
                        # Store in cache for potential use by other expressions
                        data_cache[label] = result_df
                        
                        # Only add trace if visible
                        if expr_data.get('visible', True):
                            fig.add_trace(go.Scatter(
                                x=result_df['Date'],
                                y=result_df['Rate'],
                                mode='lines',
                                name=f'{label}: {expression.upper()}',
                                line=dict(
                                    color=colors[i % len(colors)],
                                    width=2
                                ),
                                yaxis='y2' if axis == 'right' else 'y',
                                hovertemplate='<b>Date:</b> %{x}<br><b>Rate:</b> %{y:.3f}%<extra></extra>'
                            ))
                    
                    else:
                        # Mathematical expression with variables (A, B, C, etc.)
                        # First, get all the base data needed - case insensitive matching
                        variables_needed = re.findall(r'[A-Za-z]', expression.upper())
                        
                        # Create case-insensitive mapping of variables to data_cache keys
                        var_mapping = {}
                        for var in variables_needed:
                            var_upper = var.upper()
                            # Find matching key in data_cache (case insensitive)
                            for cache_key in data_cache.keys():
                                if cache_key.upper() == var_upper:
                                    var_mapping[var_upper] = cache_key
                                    break
                        
                        # Make sure we have all required variables
                        missing_vars = [var for var in variables_needed if var.upper() not in var_mapping]
                        if missing_vars:
                            continue  # Skip if we don't have required variables
                        
                        # Get a common date range from all variables
                        if not data_cache:
                            continue
                        
                        # Find common dates across all required datasets
                        common_dates = None
                        for var in variables_needed:
                            var_upper = var.upper()
                            if var_upper in var_mapping:
                                cache_key = var_mapping[var_upper]
                                if common_dates is None:
                                    common_dates = set(data_cache[cache_key]['Date'])
                                else:
                                    common_dates = common_dates.intersection(set(data_cache[cache_key]['Date']))
                        
                        if not common_dates:
                            continue
                        
                        # Create result dataframe
                        result_dates = sorted(list(common_dates))
                        result_rates = []
                        
                        for date in result_dates:
                            # Get values for this date from all variables
                            var_values = {}
                            for var in variables_needed:
                                var_upper = var.upper()
                                if var_upper in var_mapping:
                                    cache_key = var_mapping[var_upper]
                                    var_df = data_cache[cache_key]
                                    rate_row = var_df[var_df['Date'] == date]
                                    if not rate_row.empty:
                                        var_values[var] = rate_row['Rate'].iloc[0]
                            
                            # Evaluate the expression
                            try:
                                # Replace variables with their values (case insensitive)
                                eval_expr = expression
                                for var, value in var_values.items():
                                    # Replace both upper and lower case versions
                                    eval_expr = re.sub(re.escape(var), str(value), eval_expr, flags=re.IGNORECASE)
                                
                                # Evaluate the mathematical expression
                                result = eval(eval_expr)
                                result_rates.append(result)
                            except:
                                result_rates.append(None)
                        
                        # Create result dataframe
                        result_df = pd.DataFrame({
                            'Date': result_dates,
                            'Rate': result_rates
                        })
                        
                        # Remove rows with None values
                        result_df = result_df.dropna()
                        
                        # Filter by range
                        result_df = filter_data_by_range(result_df, range_filter)
                        
                        if result_df is None or result_df.empty:
                            continue
                        
                        # Store in cache for potential use by other expressions
                        data_cache[label] = result_df
                        
                        # Only add trace if visible
                        if expr_data.get('visible', True):
                            fig.add_trace(go.Scatter(
                                x=result_df['Date'],
                                y=result_df['Rate'],
                                mode='lines',
                                name=f'{label}: {expression}',
                                line=dict(
                                    color=colors[i % len(colors)],
                                    width=2
                                ),
                                yaxis='y2' if axis == 'right' else 'y',
                                hovertemplate='<b>Date:</b> %{x}<br><b>Rate:</b> %{y:.3f}%<extra></extra>'
                            ))
                    
            except Exception as e:
                print(f"Error processing {label}: {expression} - {str(e)}")
                continue
        
        # Check if we need a right axis
        has_right_axis = any(expr.get('axis') == 'right' for expr in expressions)
        
        # Update layout with dark theme (no title, closer axis labels)
        layout_config = dict(
            xaxis=dict(
                title='Date',
                title_standoff=10,  # Bring title closer to axis
                gridcolor=DARK_THEME['grid_color'],
                color=DARK_THEME['font_color'],
                linecolor=DARK_THEME['grid_color'],  # Add border to x-axis
                linewidth=1,
                mirror=True,  # Add border to top of chart
                tickfont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                ),
                titlefont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                ),
                rangeslider=dict(
                    visible=True,
                    bgcolor=DARK_THEME['plot_bgcolor'],
                    bordercolor=DARK_THEME['grid_color'],
                    borderwidth=1,
                    thickness=0.05  # Make slider thinner
                ),
                type='date'
            ),
            yaxis=dict(
                title='Rate (%)',
                title_standoff=10,  # Bring title closer to axis
                gridcolor=DARK_THEME['grid_color'],
                color=DARK_THEME['font_color'],
                linecolor=DARK_THEME['grid_color'],  # Add border to y-axis
                linewidth=1,
                mirror=True,  # Add border to right side of chart
                tickformat='.3f',  # Display 3 decimal places
                zeroline=False,  # Remove white line at zero
                autorange=True,  # Enable auto-scaling based on visible data
                fixedrange=False,  # Allow y-axis to adjust when x-axis range changes
                tickfont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                ),
                titlefont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                )
            ),
            plot_bgcolor=DARK_THEME['plot_bgcolor'],
            paper_bgcolor=DARK_THEME['paper_bgcolor'],
            font=dict(
                color=DARK_THEME['font_color'],
                family='Avenir, Helvetica Neue, Arial, sans-serif',
                weight=400
            ),
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="v",  # Vertical legend
                yanchor="top",
                y=0.98,  # Position at top of chart area
                xanchor="right",
                x=0.98,  # Position at right of chart area
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                ),
                bgcolor="rgba(30, 30, 30, 0.9)",  # Semi-transparent background
                bordercolor="#333333",
                borderwidth=1
            ),
            margin=dict(l=60, r=80, t=20, b=60)  # Standard margins with legend inside chart area
        )
        
        # Add right y-axis if needed
        if has_right_axis:
            layout_config['yaxis2'] = dict(
                title='Rate (%)',
                title_standoff=10,
                overlaying='y',
                side='right',
                showgrid=False,  # Disable gridlines for right axis
                gridcolor=DARK_THEME['grid_color'],
                color=DARK_THEME['font_color'],
                linecolor=DARK_THEME['grid_color'],  # Add border to right y-axis
                linewidth=1,
                tickformat='.3f',  # Display 3 decimal places
                zeroline=False,  # Remove white line at zero
                tickfont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                ),
                titlefont=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    weight=400
                )
            )
        
        fig.update_layout(**layout_config)
        
        # Convert to JSON
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return jsonify({
            'success': True,
            'chart': graphJSON
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Trading Dashboard Routes

@app.route('/trading')
def trading():
    """Trading dashboard page"""
    return render_template('trading.html')

@app.route('/add_trade', methods=['POST'])
def add_trade():
    """Add a new trade to the portfolio"""
    try:
        data = request.get_json()
        print(f"🔍 Received data: {data}")
        
        # Generate unique trade ID
        trade_id = data.get('name', f"Trade_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Get basic info arrays (multiple rows from basic info table)
        typologies = data.get('typologies', [])
        instruments = data.get('instruments', [])
        
        print(f"🔍 Typologies: {typologies} (type: {type(typologies)})")
        print(f"🔍 Instruments: {instruments} (type: {type(instruments)})")
        
        # Create new trade with lists
        trade = Trade(
            trade_id=trade_id,
            typology=typologies,
            secondary_typology=data.get('secondary_instrument') if 'efp' in typologies else None
        )
        
        trade.instrument_details = instruments
        print(f"🔍 Trade created with typology: {trade.typology}, instruments: {trade.instrument_details}")
        
        # Add positions from arrays - handle both old format (entry_prices/entry_sizes) and new format (prices/sizes)
        entry_prices = data.get('entry_prices', data.get('prices', []))
        entry_sizes = data.get('entry_sizes', data.get('sizes', []))
        
        print(f"🔍 Entry prices: {entry_prices}")
        print(f"🔍 Entry sizes: {entry_sizes}")
        
        for price, size in zip(entry_prices, entry_sizes):
            if price and size:
                trade.prices.append(float(price))
                trade.sizes.append(float(size))
        
        print(f"🔍 Final trade.prices: {trade.prices}")
        print(f"🔍 Final trade.sizes: {trade.sizes}")
        
        # Calculate and store total trade P&L if curves are available
        total_trade_pnl = calculate_total_trade_pnl(trade)
        if total_trade_pnl is not None:
            trade.stored_pnl = total_trade_pnl
            trade.pnl_timestamp = datetime.now().isoformat()
            print(f"💰 Calculated and stored total P&L for new trade: ${total_trade_pnl:,.2f}")
        else:
            trade.stored_pnl = 0.0
            print(f"📊 No P&L calculated for new trade (curves not available)")
        
        # Add to portfolio (this will auto-save to file)
        portfolio.add_trade(trade)
        
        return jsonify({
            'success': True,
            'trade_id': trade_id,
            'message': f'Trade {trade_id} added successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_trades')
def get_trades():
    """Get all trades in the portfolio with stored P&L data"""
    try:
        trades_data = {}
        
        for trade_id, trade in portfolio.trades.items():
            # Use stored P&L instead of calculating every time
            stored_pnl = getattr(trade, 'stored_pnl', 0.0)
            pnl_timestamp = getattr(trade, 'pnl_timestamp', None)
            
            trades_data[trade_id] = {
                'trade_id': trade.trade_id,
                'typology': trade.typology,
                'secondary_typology': trade.secondary_typology,
                'instrument_details': trade.instrument_details,
                'prices': trade.prices,
                'sizes': trade.sizes,
                'weighted_avg_price': trade.get_weighted_average_price(),
                'stored_pnl': stored_pnl,
                'pnl_timestamp': pnl_timestamp,
            }
        
        # Include portfolio-level P&L metadata
        portfolio_metadata = {
            'last_pnl_update': getattr(portfolio, 'last_pnl_update', None),
            'total_portfolio_pnl': getattr(portfolio, 'total_portfolio_pnl', 0.0)
        }
        
        return jsonify({
            'success': True,
            'trades': trades_data,
            'portfolio_metadata': portfolio_metadata
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_trade_details/<trade_id>')
def get_trade_details(trade_id):
    """Get detailed information about a specific trade"""
    try:
        trade_details = portfolio.get_trade_details(trade_id)
        
        if 'error' in trade_details:
            return jsonify({'error': trade_details['error']}), 404
        
        return jsonify({
            'success': True,
            'trade': trade_details
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update_trade', methods=['POST'])
def update_trade():
    """Update trade positions and basic info"""
    try:
        data = request.get_json()
        print(f"🔍 Update trade received data: {data}")
        
        trade_id = data.get('trade_id')
        
        if trade_id not in portfolio.trades:
            return jsonify({'error': f'Trade {trade_id} not found'}), 404
        
        trade = portfolio.trades[trade_id]
        
        # Update basic info if provided
        typologies = data.get('typologies', [])
        instruments = data.get('instruments', [])
        
        if typologies:
            trade.typology = typologies
            print(f"🔍 Updated typology to: {trade.typology}")
        
        if instruments:
            trade.instrument_details = instruments
            print(f"🔍 Updated instruments to: {trade.instrument_details}")
        
        # Update positions - handle both old format (entry_prices/entry_sizes) and new format (prices/sizes)
        entry_prices = data.get('entry_prices', data.get('prices', []))
        entry_sizes = data.get('entry_sizes', data.get('sizes', []))
        
        print(f"🔍 Update - Entry prices: {entry_prices}")
        print(f"🔍 Update - Entry sizes: {entry_sizes}")
        
        trade.prices = [float(p) for p in entry_prices if p]
        trade.sizes = [float(s) for s in entry_sizes if s]
        
        print(f"🔍 Update - Final trade.prices: {trade.prices}")
        print(f"🔍 Update - Final trade.sizes: {trade.sizes}")
        
        # Recalculate and store total trade P&L if curves are available
        total_trade_pnl = calculate_total_trade_pnl(trade)
        if total_trade_pnl is not None:
            trade.stored_pnl = total_trade_pnl
            trade.pnl_timestamp = datetime.now().isoformat()
            print(f"💰 Recalculated and stored total P&L for updated trade: ${total_trade_pnl:,.2f}")
        else:
            # Keep existing stored P&L if curves not available
            print(f"📊 No P&L recalculated for updated trade (curves not available), keeping existing stored P&L")
        
        # Save to file after updating
        portfolio.save_to_file()
        
        return jsonify({
            'success': True,
            'message': f'Trade {trade_id} updated successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete_trade/<trade_id>', methods=['DELETE'])
def delete_trade(trade_id):
    """Delete a trade from the portfolio"""
    try:
        if trade_id not in portfolio.trades:
            return jsonify({'error': f'Trade {trade_id} not found'}), 404
        
        portfolio.remove_trade(trade_id)
        
        return jsonify({
            'success': True,
            'message': f'Trade {trade_id} deleted successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_portfolio_pnl_history')
def get_portfolio_pnl_history():
    """Get historical portfolio PnL data for charting"""
    try:
        # For now, return simulated data
        # In production, you would store historical PnL snapshots
        dates = []
        pnl_values = []
        
        # Generate sample data for the last 30 days
        import numpy as np
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(30):
            date = base_date + timedelta(days=i)
            dates.append(date.isoformat())
            # Simulate PnL progression
            pnl_values.append(np.random.normal(1000 + i * 50, 200))
        
        return jsonify({
            'success': True,
            'dates': dates,
            'pnl_values': pnl_values
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_instrument_price', methods=['POST'])
def get_instrument_price():
    """Get live price for a specific instrument - DEPRECATED: Use /get_realtime_rates instead"""
    try:
        return jsonify({
            'success': False,
            'error': 'This endpoint is deprecated. Use /get_realtime_rates instead.'
        }), 410  # HTTP 410 Gone
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/save_portfolio', methods=['POST'])
def save_portfolio():
    """Manually save portfolio to file"""
    try:
        portfolio.save_to_file()
        return jsonify({
            'success': True,
            'message': f'Portfolio saved successfully ({len(portfolio.trades)} trades)'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/portfolio_status')
def portfolio_status():
    """Get portfolio status and storage information"""
    try:
        import os
        
        # Check if storage file exists
        storage_exists = os.path.exists(portfolio.storage_file)
        
        return jsonify({
            'success': True,
            'trades_count': len(portfolio.trades),
            'storage_file': portfolio.storage_file,
            'storage_exists': storage_exists,
            'storage_dir': portfolio.storage_dir
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/calculate_portfolio_pnl_xc', methods=['POST'])
def calculate_portfolio_pnl_xc():
    """Calculate portfolio P&L using XC positions"""
    try:
        # Calculate XC-based portfolio P&L (curve handle auto-generated)
        pnl_result = portfolio.calculate_portfolio_pnl_xc()
        
        return jsonify({
            'success': True,
            'pnl_data': pnl_result
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/initialize_positions', methods=['POST'])
def initialize_positions():
    """Manually initialize positions for all trades"""
    try:
        successful_trades, total_positions = portfolio.initialize_positions()
        
        return jsonify({
            'success': True,
            'message': f'Positions initialized: {successful_trades}/{len(portfolio.trades)} trades, {total_positions} total positions',
            'successful_trades': successful_trades,
            'total_trades': len(portfolio.trades),
            'total_positions': total_positions
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update_realtime_pnl', methods=['POST'])
def update_realtime_pnl():
    """Update P&L for all trades using the portfolio's update_realtime_pnl method"""
    try:
        print("🔄 Starting real-time P&L update via portfolio method...")
        
        # Call the portfolio's update_realtime_pnl method which handles everything
        pnl_result = portfolio.update_realtime_pnl()
        
        if not pnl_result['success']:
            return jsonify(pnl_result), 500
        
        return jsonify(pnl_result)
        
    except Exception as e:
        print(f"❌ Error in update_realtime_pnl endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/calculate_swap_pnl', methods=['POST'])
def calculate_swap_pnl():
    """Calculate P&L for a single position (swap or futures) - IMPROVED to use existing trade positions when possible"""
    try:
        data = request.get_json()
        print(f"🔍 calculate_swap_pnl received data: {data}")
        
        if not data:
            return jsonify({'error': 'No JSON data received'}), 400
        
        # Extract position details
        trade_id = data.get('trade_id')  # Accept trade_id to reuse existing positions
        position_index = data.get('position_index', 0)  # Which position in the trade
        trade_type = data.get('tradeType', 'swap').lower()
        instrument = data.get('instrument')
        price_raw = data.get('price')
        size_raw = data.get('size')
        
        print(f"🔍 Extracted values: trade_id={trade_id}, position_index={position_index}, trade_type={trade_type}, instrument={instrument}")
        
        # EFFICIENCY IMPROVEMENT: Try to use existing trade position first
        if trade_id and trade_id in portfolio.trades:
            trade = portfolio.trades[trade_id]
            print(f"🔄 Found existing trade: {trade_id}")
            
            # Check if positions are already created
            if (hasattr(trade, 'positions') and 
                len(trade.positions) > position_index):
                
                existing_position = trade.positions[position_index]
                
                # Handle SWAP positions
                if isinstance(existing_position, XCSwapPosition):
                    if hasattr(existing_position, 'xc_created') and existing_position.xc_created:
                        print(f"✅ Reusing existing swap position {position_index} with XC swaps already created")
                        
                        # Calculate P&L using existing XC swaps
                        pnl_result = existing_position.calculate_pnl()
                        
                        # Get component details for response
                        component_details = []
                        for swap_info in existing_position.xc_swaps:
                            component_details.append({
                                'instrument': swap_info['instrument'],
                                'coefficient': swap_info['coefficient'],
                                'notional': swap_info['notional'],
                                'rate': swap_info['rate']
                            })
                        
                        return jsonify({
                            'success': True,
                            'pnl': pnl_result['pnl'],
                            'error': pnl_result['error'],
                            'components': component_details,
                            'component_pnls': pnl_result.get('components', []),
                            'method': 'reused_existing_swap_position',
                            'position_details': {
                                'handle': existing_position.handle,
                                'instrument': existing_position.instrument,
                                'total_components': len(existing_position.xc_swaps)
                            }
                        })
                    else:
                        print(f"🔧 Found existing swap position {position_index} but XC swaps not created, creating now...")
                        if existing_position.create_xc_swaps():
                            pnl_result = existing_position.calculate_pnl()
                            
                            component_details = []
                            for swap_info in existing_position.xc_swaps:
                                component_details.append({
                                    'instrument': swap_info['instrument'],
                                    'coefficient': swap_info['coefficient'],
                                    'notional': swap_info['notional'],
                                    'rate': swap_info['rate']
                                })
                            
                            return jsonify({
                                'success': True,
                                'pnl': pnl_result['pnl'],
                                'error': pnl_result['error'],
                                'components': component_details,
                                'component_pnls': pnl_result.get('components', []),
                                'method': 'created_xc_swaps_for_existing_position',
                                'position_details': {
                                    'handle': existing_position.handle,
                                    'instrument': existing_position.instrument,
                                    'total_components': len(existing_position.xc_swaps)
                                }
                            })
                
                # Handle FUTURES positions
                elif isinstance(existing_position, XCFuturesPosition):
                    print(f"✅ Reusing existing futures position {position_index}")
                    
                    # Get futures tick data
                    from trading_functions import get_futures_details
                    futures_df = get_futures_details([existing_position.instrument])
                    
                    # Calculate P&L using futures tick data
                    pnl_result = existing_position.calculate_pnl(futures_df)
                    
                    return jsonify({
                        'success': True,
                        'pnl': pnl_result['pnl'],
                        'error': pnl_result['error'],
                        'method': 'reused_existing_futures_position',
                        'position_details': {
                            'handle': existing_position.handle,
                            'instrument': existing_position.instrument,
                            'price': existing_position.price,
                            'size': existing_position.size
                        },
                        'futures_details': pnl_result.get('details', {})
                    })
        
        # FALLBACK: Create temporary position if no existing trade/position found
        print(f"🔧 No existing position found, creating temporary position...")
        
        # Convert to numbers with error handling
        try:
            price = float(price_raw) if price_raw is not None else 0
            size = float(size_raw) if size_raw is not None else 0
        except (ValueError, TypeError) as e:
            return jsonify({'error': f'Invalid price or size format: {e}'}), 400
        
        if not instrument or price == 0 or size == 0:
            return jsonify({
                'error': f'Missing required parameters: instrument="{instrument}", price={price}, size={size}'
            }), 400
        
        # Handle FUTURES
        if trade_type == 'future':
            print(f"🔧 Creating temporary futures position...")
            from trading_functions import XCFuturesPosition, get_futures_details
            
            temp_handle = f"temp_futures_pnl_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            
            position = XCFuturesPosition(
                handle=temp_handle,
                price=price,
                size=size,
                instrument=instrument
            )
            
            # Get futures tick data
            futures_df = get_futures_details([instrument])
            
            # Calculate P&L
            pnl_result = position.calculate_pnl(futures_df)
            
            return jsonify({
                'success': True,
                'pnl': pnl_result['pnl'],
                'error': pnl_result['error'],
                'method': 'temporary_futures_position_created',
                'position_details': {
                    'handle': temp_handle,
                    'instrument': instrument,
                    'price': price,
                    'size': size
                },
                'futures_details': pnl_result.get('details', {})
            })
        
        # Handle SWAPS
        else:
            print(f"🔧 Creating temporary swap position...")
            from trading_functions import XCSwapPosition, parse_complex_expression
            
            # Parse the complex expression to understand its structure
            components = parse_complex_expression(instrument)
            
            if not components:
                return jsonify({'error': f'Could not parse instrument expression: {instrument}'}), 400
            
            # Create temporary XC position for P&L calculation
            temp_handle = f"temp_swap_pnl_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            
            position = XCSwapPosition(
                handle=temp_handle,
                price=price,  # Price in percentage terms
                size=size,
                instrument=instrument
            )
            
            # Create XC swaps and calculate P&L
            if position.create_xc_swaps():  
                pnl_result = position.calculate_pnl()
                
                # Get component details for response
                component_details = []
                for swap_info in position.xc_swaps:
                    component_details.append({
                        'instrument': swap_info['instrument'],
                        'coefficient': swap_info['coefficient'],
                        'notional': swap_info['notional'],
                        'rate': swap_info['rate']
                    })
                
                return jsonify({
                    'success': True,
                    'pnl': pnl_result['pnl'],
                    'error': pnl_result['error'],
                    'components': component_details,
                    'component_pnls': pnl_result.get('components', []),
                    'method': 'temporary_swap_position_created',
                    'position_details': {
                        'handle': temp_handle,
                        'instrument': instrument,
                        'total_components': len(position.xc_swaps)
                    }
                })
            else:
                return jsonify({
                    'success': False,
                    'error': f'Failed to create XC swaps for {instrument}'
                }), 500
        
    except Exception as e:
        print(f"❌ Error in calculate_swap_pnl: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
