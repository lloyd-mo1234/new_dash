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
    """Calculate total P&L for a trade using existing positions"""
    try:
        # Check if curves are available
        from loader import is_curves_loaded
        if not is_curves_loaded():
            return None
        
        # Use trade.calculate_pnl() which handles both primary and secondary positions
        pnl_result = trade.calculate_pnl()
        return pnl_result.get('total_pnl', 0.0)
        
    except Exception as e:
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
            
            # Handle EFP expressions (Exchange for Physical - both swap and futures legs)
            if trade_type == 'efp':
                try:
                    # For EFP, the primary expression is the swap leg
                    # Handle it as a swap expression
                    
                    # Check if it's a complex swap expression
                    if any(op in expression for op in ['+', '-', '*', '/']) and re.search(r'[a-z]+\.\d+[ymd]', expression.lower()):
                        # Complex swap expression
                        components = parse_complex_expression(expression)
                        
                        if not components:
                            rates[label] = '--'
                            base_rates[label] = None
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
                    else:
                        # Simple swap expression
                        df, error = get_swap_data(expression)
                        if error or df is None or df.empty:
                            rates[label] = '--'
                            base_rates[label] = None
                        else:
                            latest_rate = df['Rate'].iloc[-1]
                            rates[label] = f"{latest_rate:.3f}%"
                            base_rates[label] = latest_rate
                    
                    
                except Exception as e:
                    rates[label] = '--'
                    base_rates[label] = None
                
                continue
            
            # Handle futures expressions
            elif trade_type == 'future':
                try:
                    # Check if it's a complex futures expression (e.g., "ymz5 comdty - xmz5 comdty")
                    components = parse_futures_expression(expression)
                    
                    if not components:
                        rates[label] = '--'
                        base_rates[label] = None
                        continue
                    
                    # Get unique instrument names from components
                    unique_instruments = list(set([comp['instrument'] for comp in components]))
                    
                    # Get futures details for all unique instruments
                    futures_df = get_futures_details(unique_instruments)
                    
                    if futures_df is None or futures_df.empty:
                        rates[label] = '--'
                        base_rates[label] = None
                        continue
                    
                    # Calculate the result based on expression components
                    result_value = 0.0
                    all_instruments_found = True
                    
                    for comp in components:
                        instrument = comp['instrument']
                        coefficient = comp['coefficient']
                        
                        if instrument in futures_df.index:
                            px_mid = futures_df.loc[instrument, 'px_mid']
                            result_value += coefficient * px_mid
                        else:
                            all_instruments_found = False
                            break
                    
                    if all_instruments_found:
                        rates[label] = f"{result_value:.4f}"
                        base_rates[label] = result_value
                    else:
                        rates[label] = '--'
                        base_rates[label] = None
                    
                except Exception as e:
                    rates[label] = '--'
                    base_rates[label] = None
                
                continue
            
            # Handle swap expressions
            else:
                # Check if it's a complex expression (contains arithmetic operators with instruments)
                if any(op in expression for op in ['+', '-', '*', '/']) and re.search(r'[a-z]+\.\d+[ymd]', expression.lower()):
                    # This is a complex expression like "aud.5y5y-eur.5y5y"
                    try:
                       
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
        
        # Generate unique trade ID
        trade_id = data.get('name', f"Trade_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Get basic info arrays (multiple rows from basic info table)
        typologies = data.get('typologies', [])
        instruments = data.get('instruments', [])
        
        # CRITICAL FIX: Extract group_id from request data
        group_id = data.get('group_id')
        
        # For EFP trades, separate primary and secondary data
        primary_typology = typologies[0] if len(typologies) > 0 else None
        secondary_typology = typologies[1] if len(typologies) > 1 else None
        
        primary_instrument = instruments[0] if len(instruments) > 0 else None
        secondary_instrument = instruments[1] if len(instruments) > 1 else None
        
        # Create new trade with group_id
        trade = Trade(
            trade_id=trade_id,
            typology=primary_typology,
            secondary_typology=secondary_typology,
            group_id=group_id
        )
        
        # Set instrument details - for EFP trades, separate primary and secondary instruments
        is_efp = primary_typology == 'efp' and secondary_typology
        
        if is_efp:
            # For EFP: primary instrument (swap) goes to instrument_details, secondary (futures) goes to instrument_details_secondary
            trade.instrument_details = [primary_instrument] if primary_instrument else []
            trade.instrument_details_secondary = [secondary_instrument] if secondary_instrument else []
        else:
            # For non-EFP trades: all instruments go to instrument_details
            trade.instrument_details = [primary_instrument] if primary_instrument else []
        
        # Add positions from arrays - handle both old format (entry_prices/entry_sizes) and new format (prices/sizes)
        entry_prices = data.get('entry_prices', data.get('prices', []))
        entry_sizes = data.get('entry_sizes', data.get('sizes', []))
        
        # NEW: Get separate secondary arrays for EFP trades
        entry_prices_secondary = data.get('entry_prices_secondary', [])
        entry_sizes_secondary = data.get('entry_sizes_secondary', [])
        
        # CRITICAL FIX: Get insertion date arrays from request
        entry_insertion_dates = data.get('entry_insertion_dates', [])
        entry_insertion_dates_secondary = data.get('entry_insertion_dates_secondary', [])
        
        # 🐛 DEBUG: Log insertion dates received from modal
        
        # For EFP trades, use separate arrays if provided
        is_efp = primary_typology == 'efp' and secondary_typology
        
        if is_efp and (entry_prices_secondary or entry_sizes_secondary):
            # EFP with separate arrays - use them directly
            
            # Primary positions (swap leg)
            for price, size in zip(entry_prices, entry_sizes):
                if price and size:
                    trade.prices.append(float(price))
                    if isinstance(size, list):
                        trade.sizes.append(size)
                    else:
                        trade.sizes.append(float(size))
            
            # Secondary positions (futures leg)
            for price, size in zip(entry_prices_secondary, entry_sizes_secondary):
                if price and size:
                    trade.prices_secondary.append(float(price))
                    if isinstance(size, list):
                        trade.sizes_secondary.append(size)
                    else:
                        trade.sizes_secondary.append(float(size))
            
            # CRITICAL FIX: Set insertion date arrays for EFP trades
            trade.primary_pos_insertion_dt = entry_insertion_dates if entry_insertion_dates else []
            trade.secondary_pos_insertion_dt = entry_insertion_dates_secondary if entry_insertion_dates_secondary else []
            
        else:
            # Non-EFP trade - store all in primary arrays
            for price, size in zip(entry_prices, entry_sizes):
                if price and size:
                    trade.prices.append(float(price))
                    if isinstance(size, list):
                        trade.sizes.append(size)
                    else:
                        trade.sizes.append(float(size))
            
            # CRITICAL FIX: Set primary insertion date array for non-EFP trades
            trade.primary_pos_insertion_dt = entry_insertion_dates if entry_insertion_dates else []
            
        
        # Calculate and store total trade P&L if curves are available
        total_trade_pnl = calculate_total_trade_pnl(trade)
        if total_trade_pnl is not None:
            trade.stored_pnl = total_trade_pnl
            trade.pnl_timestamp = datetime.now().isoformat()
        else:
            trade.stored_pnl = 0.0
        
        # Add to portfolio
        portfolio.add_trade(trade)
        
        # Save to file ONLY if not a temporary trade
        skip_save = data.get('skip_save', False)
        if not skip_save:
            portfolio.save_to_file()
        
        return jsonify({
            'success': True,
            'trade_id': trade_id,
            'message': f'Trade {trade_id} added successfully',
            'temporary': skip_save
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_trades')
def get_trades():
    """Get all trades in the portfolio with stored P&L data"""
    try:
        
        trades_data = {}
        
        for trade_id, trade in portfolio.trades.items():
            # Log each trade with group_id information
            group_id = getattr(trade, 'group_id', None)
            # Use stored P&L instead of calculating every time
            stored_pnl = getattr(trade, 'stored_pnl', 0.0)
            pnl_timestamp = getattr(trade, 'pnl_timestamp', None)
            
            # Calculate 1d PnL using the trade's calculate_1d_pnl method
            one_day_pnl = trade.calculate_1d_pnl()
            
            # Calculate z-scores for swap trades
            z_scores = trade.calculate_z_scores()
            
            trades_data[trade_id] = {
                'trade_id': trade.trade_id,
                'typology': trade.typology,
                'secondary_typology': trade.secondary_typology,
                'group_id': group_id,  # Include group_id in response
                'instrument_details': trade.instrument_details,
                'prices': trade.prices,
                'sizes': trade.sizes,
                'weighted_avg_price': trade.get_weighted_average_price(),
                'stored_pnl': stored_pnl,
                'pnl_timestamp': pnl_timestamp,
                'one_day_pnl': one_day_pnl,  # Include 1d PnL
                'carry': getattr(trade, 'carry', 0.0),  # Include carry
                'z_scores': z_scores,  # Include z-scores (1m, 3m, 6m, 1y)
                # CRITICAL FIX: Include secondary data for EFP trades in backup
                'prices_secondary': getattr(trade, 'prices_secondary', []),
                'sizes_secondary': getattr(trade, 'sizes_secondary', []),
                'instrument_details_secondary': getattr(trade, 'instrument_details_secondary', []),
                'stored_pnl_primary': getattr(trade, 'stored_pnl_primary', None),
                'stored_pnl_secondary': getattr(trade, 'stored_pnl_secondary', None),
                # CRITICAL FIX: Include insertion date arrays in backup
                'primary_pos_insertion_dt': getattr(trade, 'primary_pos_insertion_dt', []),
                'secondary_pos_insertion_dt': getattr(trade, 'secondary_pos_insertion_dt', [])
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
        if trade_id not in portfolio.trades:
            return jsonify({'error': f'Trade {trade_id} not found'}), 404
        
        trade = portfolio.trades[trade_id]
        
        # CRITICAL FIX: Create positions from JSON data if they don't exist
        if not trade.positions and (trade.prices or trade.sizes):
            if not trade.create_positions():
                print("failed to create positions")
        
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
        
        old_trade_id = data.get('trade_id')  # Original trade ID
        new_trade_id = data.get('new_trade_id')  # New trade ID (if renaming)
        
        if old_trade_id not in portfolio.trades:
            return jsonify({'error': f'Trade {old_trade_id} not found'}), 404
        
        trade = portfolio.trades[old_trade_id]
        
        # Handle trade ID change if new_trade_id is provided and different
        if new_trade_id and new_trade_id != old_trade_id:
            # Check if new trade ID already exists
            if new_trade_id in portfolio.trades:
                return jsonify({'error': f'Trade ID {new_trade_id} already exists'}), 400
            
            # Update trade object's trade_id
            trade.trade_id = new_trade_id
            
            # Remove old key from portfolio dict
            del portfolio.trades[old_trade_id]
            
            # Add with new key
            portfolio.trades[new_trade_id] = trade
            
            # Use new trade_id for the rest of the function
            trade_id = new_trade_id
        else:
            trade_id = old_trade_id
        
        # Update basic info if provided
        typologies = data.get('typologies', [])
        instruments = data.get('instruments', [])
        
        # CRITICAL FIX: Extract and update group_id from request data
        if 'group_id' in data:
            trade.group_id = data.get('group_id')
        
        # For EFP trades, separate primary and secondary data
        if typologies:
            trade.typology = typologies[0] if len(typologies) > 0 else None
            trade.secondary_typology = typologies[1] if len(typologies) > 1 else None
        
        if instruments:
            # Set instrument details - for EFP trades, separate primary and secondary instruments
            primary_instrument = instruments[0] if len(instruments) > 0 else None
            secondary_instrument = instruments[1] if len(instruments) > 1 else None
            
            is_efp_update = trade.typology == 'efp' and trade.secondary_typology
            
            if is_efp_update:
                # For EFP: primary instrument (swap) goes to instrument_details, secondary (futures) goes to instrument_details_secondary
                trade.instrument_details = [primary_instrument] if primary_instrument else []
                trade.instrument_details_secondary = [secondary_instrument] if secondary_instrument else []
            else:
                # For non-EFP trades: all instruments go to instrument_details
                trade.instrument_details = [primary_instrument] if primary_instrument else []
        
        # Update positions - handle both old format (entry_prices/entry_sizes) and new format (prices/sizes)
        entry_prices = data.get('entry_prices', data.get('prices', []))
        entry_sizes = data.get('entry_sizes', data.get('sizes', []))
        
        # NEW: Get separate secondary arrays for EFP trades
        entry_prices_secondary = data.get('entry_prices_secondary', [])
        entry_sizes_secondary = data.get('entry_sizes_secondary', [])
        
        # CRITICAL FIX: Get insertion date arrays from request
        entry_insertion_dates = data.get('entry_insertion_dates', [])
        entry_insertion_dates_secondary = data.get('entry_insertion_dates_secondary', [])
        
        # 🐛 DEBUG: Log insertion dates received from modal (update_trade)
        
        # Check if this is an EFP trade that needs separate position handling
        is_efp = trade.typology == 'efp' and trade.secondary_typology
        
        if is_efp and (entry_prices_secondary or entry_sizes_secondary):
            # EFP with separate arrays
            
            # Clear existing arrays
            trade.prices = []
            trade.sizes = []
            trade.prices_secondary = []
            trade.sizes_secondary = []
            
            # CRITICAL FIX: Clear insertion date arrays
            trade.primary_pos_insertion_dt = []
            trade.secondary_pos_insertion_dt = []
            
            # Primary positions (swap leg)
            for price, size in zip(entry_prices, entry_sizes):
                if price and size:
                    trade.prices.append(float(price))
                    if isinstance(size, list):
                        trade.sizes.append(size)
                    else:
                        trade.sizes.append(float(size))
            
            # Secondary positions (futures leg)
            for price, size in zip(entry_prices_secondary, entry_sizes_secondary):
                if price and size:
                    trade.prices_secondary.append(float(price))
                    if isinstance(size, list):
                        trade.sizes_secondary.append(size)
                    else:
                        trade.sizes_secondary.append(float(size))
            
            # CRITICAL FIX: Update insertion date arrays
            if entry_insertion_dates:
                trade.primary_pos_insertion_dt = entry_insertion_dates
            if entry_insertion_dates_secondary:
                trade.secondary_pos_insertion_dt = entry_insertion_dates_secondary
        else:
            # Non-EFP trade - store all in primary arrays
            trade.prices = [float(p) for p in entry_prices if p]
            trade.sizes = []
            for s in entry_sizes:
                if s:
                    if isinstance(s, list):
                        trade.sizes.append(s)
                    else:
                        trade.sizes.append(float(s))
            
            # CRITICAL FIX: Update primary insertion date array for non-EFP trades
            if entry_insertion_dates:
                trade.primary_pos_insertion_dt = entry_insertion_dates
            else:
                # Ensure the array exists but is empty if no dates provided
                trade.primary_pos_insertion_dt = []
        
        # Recalculate and store total trade P&L if curves are available
        total_trade_pnl = calculate_total_trade_pnl(trade)
        if total_trade_pnl is not None:
            trade.stored_pnl = total_trade_pnl
            trade.pnl_timestamp = datetime.now().isoformat()
        
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
        
        # Call the portfolio's update_realtime_pnl method which handles everything
        pnl_result = portfolio.update_realtime_pnl()
        
        if not pnl_result['success']:
            return jsonify(pnl_result), 500
        
        return jsonify(pnl_result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/add_position', methods=['POST'])
def add_position():
    """Add a new position to a trade"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data received'}), 400
        
        # Extract position details
        trade_id = data.get('trade_id')
        trade_type = data.get('tradeType', 'swap').lower()
        position_type = data.get('positionType', 'primary')  # 'primary' or 'secondary' (for EFP)
        instrument = data.get('instrument')
        price_raw = data.get('price')
        size_raw = data.get('size')
        insertion_date = data.get('insertion_date')  # NEW: Get insertion date from request
        
        if not trade_id or trade_id not in portfolio.trades:
            return jsonify({'error': 'Valid trade_id is required'}), 400
        
        trade = portfolio.trades[trade_id]
        
        # Convert to numbers
        try:
            price = float(price_raw) if price_raw is not None else 0
            if isinstance(size_raw, list):
                size = [float(s) for s in size_raw]
            else:
                size = float(size_raw) if size_raw is not None else 0
        except (ValueError, TypeError) as e:
            return jsonify({'error': f'Invalid price or size format: {e}'}), 400
        
        # Validate - allow zeros for initialization
        size_is_valid = False
        if isinstance(size, list):
            size_is_valid = len(size) > 0
        else:
            size_is_valid = True  # Single size is always valid (including 0)
        
        # Allow empty instrument for placeholder positions (will be filled in later when user enters trade type and instrument)
        # if not instrument:
        #     return jsonify({'error': 'Missing required parameters'}), 400
        
        # Determine which positions array to use
        # Allow secondary positions for any trade that requests it
        if position_type == 'secondary':
            # Ensure positions_secondary exists
            if not hasattr(trade, 'positions_secondary'):
                trade.positions_secondary = []
            positions_array = trade.positions_secondary
            handle_prefix = f"{trade_id}_futures_position"
        else:
            # Ensure positions exists
            if not hasattr(trade, 'positions'):
                trade.positions = []
            positions_array = trade.positions
            handle_prefix = f"{trade_id}_position"
        
        # Create unique handle
        position_handle = f"{handle_prefix}_{len(positions_array)}"
        
        # Handle FUTURES
        if trade_type == 'future' or (trade_type == 'efp' and position_type == 'secondary'):
            position = XCFuturesPosition(
                handle=position_handle,
                price=price,
                size=size,
                instrument=instrument,
                insertion_date=insertion_date  # NEW: Pass insertion date to position
            )
            
            # Build the futures expression
            if not position.build_futures_expression():
                return jsonify({'error': f'Failed to build futures expression for {instrument}'}), 500
            
            # Add position to trade
            positions_array.append(position)
            
            # Update trade arrays to keep in sync
            if trade_type == 'efp' and position_type == 'secondary':
                trade.prices_secondary.append(price)
                trade.sizes_secondary.append(size)
                # NEW: Add insertion date to secondary array
                if not hasattr(trade, 'secondary_pos_insertion_dt'):
                    trade.secondary_pos_insertion_dt = []
                trade.secondary_pos_insertion_dt.append(insertion_date)
            else:
                trade.prices.append(price)
                trade.sizes.append(size)
                # NEW: Add insertion date to primary array
                if not hasattr(trade, 'primary_pos_insertion_dt'):
                    trade.primary_pos_insertion_dt = []
                trade.primary_pos_insertion_dt.append(insertion_date)
            
            # CRITICAL FIX: Recalculate and update stored PnL after adding position
            total_trade_pnl = calculate_total_trade_pnl(trade)
            if total_trade_pnl is not None:
                trade.stored_pnl = total_trade_pnl
                trade.pnl_timestamp = datetime.now().isoformat()
            
            # Calculate individual position PnL for display in modal
            position_pnl = None
            if trade_type == 'future' or (trade_type == 'efp' and position_type == 'secondary'):
                # Get futures details
                unique_instruments = list(set([comp['instrument'] for comp in position.components]))
                print(f"\n🔍 Fetching Bloomberg data for: {unique_instruments}")
                futures_df = get_futures_details(unique_instruments)
                print(f"📊 Bloomberg dataframe received:")
                if futures_df is not None and not futures_df.empty:
                    print(futures_df)
                    print(f"\nDataframe shape: {futures_df.shape}")
                    print(f"Columns: {futures_df.columns.tolist()}")
                    print(f"Index: {futures_df.index.tolist()}")
                else:
                    print("⚠️ No data received or empty dataframe")
                pnl_result = position.calculate_pnl(futures_df)
                position_pnl = pnl_result.get('pnl')
                print(f"💰 Calculated position PnL: {position_pnl}")
            
            return jsonify({
                'success': True,
                'position_index': len(positions_array) - 1,
                'position_handle': position_handle,
                'message': 'Position added successfully',
                'updated_pnl': total_trade_pnl,  # Total trade PnL
                'position_pnl': position_pnl  # Individual position PnL
            })
        
        # Handle SWAPS
        else:
            position = XCSwapPosition(
                handle=position_handle,
                price=price,
                size=size,
                instrument=instrument,
                insertion_date=insertion_date  # NEW: Pass insertion date to position
            )
            
            # Create XC swaps
            if not position.create_xc_swaps():
                return jsonify({'error': f'Failed to create XC swaps for {instrument}'}), 500
            
            # Add position to trade
            positions_array.append(position)
            
            # Update trade arrays to keep in sync
            trade.prices.append(price)
            trade.sizes.append(size)
            # NEW: Add insertion date to primary array
            if not hasattr(trade, 'primary_pos_insertion_dt'):
                trade.primary_pos_insertion_dt = []
            trade.primary_pos_insertion_dt.append(insertion_date)
            
            # CRITICAL FIX: Recalculate and update stored PnL after adding position
            total_trade_pnl = calculate_total_trade_pnl(trade)
            if total_trade_pnl is not None:
                trade.stored_pnl = total_trade_pnl
                trade.pnl_timestamp = datetime.now().isoformat()
            
            # Calculate individual position PnL for display in modal
            pnl_result = position.calculate_pnl()
            position_pnl = pnl_result.get('pnl')
            
            return jsonify({
                'success': True,
                'position_index': len(positions_array) - 1,
                'position_handle': position_handle,
                'message': 'Position added successfully',
                'updated_pnl': total_trade_pnl,  # Total trade PnL
                'position_pnl': position_pnl  # Individual position PnL
            })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/edit_position', methods=['POST'])
def edit_position():
    """Edit an existing position's price and/or size"""
    try:
        
        data = request.get_json()
        
        # 🐛 EXPLICIT DEBUG: Log the exact insertion_date parameter
        insertion_date_from_request = data.get('insertion_date')
        
        if not data:
            return jsonify({'error': 'No JSON data received'}), 400
        
        # Extract parameters with detailed logging
        trade_id = data.get('trade_id')
        position_index = data.get('position_index', 0)
        position_type = data.get('positionType', 'primary')  # 'primary' or 'secondary'
        new_price = data.get('price')
        new_size = data.get('size')
        new_insertion_date = data.get('insertion_date')  # NEW: Get insertion date from request
        
        
        if not trade_id or trade_id not in portfolio.trades:
            return jsonify({'error': 'Valid trade_id is required'}), 400
        
        trade = portfolio.trades[trade_id]
        
        # CRITICAL FIX: Create positions from JSON data if they don't exist
        if position_type == 'secondary':
            if not hasattr(trade, 'positions_secondary'):
                trade.positions_secondary = []
            
            if not trade.positions_secondary and (trade.prices_secondary or trade.sizes_secondary):
                if not trade.create_positions():
                    return jsonify({'error': 'Failed to create positions from stored data'}), 500
                    
        else:
            if not hasattr(trade, 'positions'):
                trade.positions = []
            
            if not trade.positions and (trade.prices or trade.sizes):
                if not trade.create_positions():
                    return jsonify({'error': 'Failed to create positions from stored data'}), 500
        
        # Get the position with detailed logging
        if position_type == 'secondary':
            
            if position_index >= len(trade.positions_secondary):
                return jsonify({'error': f'Position {position_index} not found in secondary positions'}), 404
            
            position = trade.positions_secondary[position_index]
        else:
            
            if position_index >= len(trade.positions):
                return jsonify({'error': f'Position {position_index} not found'}), 404
            
            position = trade.positions[position_index]
        
        # Update position with new values if provided
        if new_price is not None:
            old_price = position.price
            position.price = float(new_price)
        
        if new_size is not None:
            old_size = position.size
            if isinstance(new_size, list):
                position.size = [float(s) for s in new_size]
            else:
                position.size = float(new_size)
        
        # NEW: Update insertion date if provided
        if new_insertion_date is not None:
            old_insertion_date = getattr(position, 'insertion_date', None)
            position.insertion_date = new_insertion_date
            
            # Also update the trade's insertion date arrays (ensure arrays exist and are long enough)
            if position_type == 'secondary':
                # Ensure secondary insertion date array exists
                if not hasattr(trade, 'secondary_pos_insertion_dt'):
                    trade.secondary_pos_insertion_dt = []
                
                # Extend array if needed to accommodate the position index
                while len(trade.secondary_pos_insertion_dt) <= position_index:
                    trade.secondary_pos_insertion_dt.append(None)
                
                # Update the insertion date
                trade.secondary_pos_insertion_dt[position_index] = new_insertion_date
            else:
                # Ensure primary insertion date array exists
                if not hasattr(trade, 'primary_pos_insertion_dt'):
                    trade.primary_pos_insertion_dt = []
                
                # Extend array if needed to accommodate the position index
                while len(trade.primary_pos_insertion_dt) <= position_index:
                    trade.primary_pos_insertion_dt.append(None)
                
                # Update the insertion date
                trade.primary_pos_insertion_dt[position_index] = new_insertion_date
        
        # Recreate XC structures with new values
        if isinstance(position, XCSwapPosition):
            position.xc_created = False
            position.xc_swaps = []
            
            # Actually recreate the swaps with new values
            if not position.create_xc_swaps():
                return jsonify({'error': 'Failed to recreate XC swaps with new values'}), 500
        
        # Recreate futures expression with new values
        elif isinstance(position, XCFuturesPosition):
            position.futures_built = False
            position.component_rates = {}  # Clear cached component rates to force recalculation
            
            # Actually rebuild the futures expression with new values
            if not position.build_futures_expression():
                return jsonify({'error': 'Failed to rebuild futures expression with new values'}), 500
        
        else:
            print("")
        
        print("")
        print("")
        
        return jsonify({
            'success': True,
            'message': f'Position {position_index} updated successfully',
            'position_index': position_index,
            'position_type': position_type,
            'position_class': type(position).__name__
        })
        
    except Exception as e:
        print("")
        print("")
        import traceback
        print("")
        return jsonify({'error': str(e)}), 500


@app.route('/calculate_position_pnl', methods=['POST'])
def calculate_position_pnl():
    """Calculate P&L for an existing position"""
    try:
        
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data received'}), 400
        
        # Extract parameters with detailed logging
        trade_id = data.get('trade_id')
        position_index = data.get('position_index', 0)
        position_type = data.get('positionType', 'primary')  # 'primary' or 'secondary'
        
        
        if not trade_id or trade_id not in portfolio.trades:
            return jsonify({'error': 'Valid trade_id is required'}), 400
        
        trade = portfolio.trades[trade_id]
        
        # CRITICAL FIX: Create positions from JSON data if they don't exist
        positions_array = trade.positions_secondary if position_type == 'secondary' else trade.positions
        
        
        if not positions_array and (trade.prices or trade.sizes):
            if not trade.create_positions():
                return jsonify({'error': 'Failed to create positions from stored data'}), 500
            else:
                # Update positions_array reference after creation
                positions_array = trade.positions_secondary if position_type == 'secondary' else trade.positions
        
        # Get the position with detailed validation
        
        if position_index >= len(positions_array):
            return jsonify({'error': f'Position {position_index} not found'}), 404
            
        position = positions_array[position_index]
        
        # Calculate P&L based on position type with comprehensive debugging
        if isinstance(position, XCFuturesPosition):
            
            # Get futures details
            unique_instruments = list(set([comp['instrument'] for comp in position.components]))
            
            futures_df = get_futures_details(unique_instruments)
            if futures_df is not None and not futures_df.empty:
                for inst in unique_instruments:
                    if inst in futures_df.index:
                        px_mid = futures_df.loc[inst, 'px_mid']
            
            pnl_result = position.calculate_pnl(futures_df)
            
            return jsonify({
                'success': True,
                'pnl': pnl_result['pnl'],
                'error': pnl_result['error'],
                'position_details': {
                    'handle': position.handle,
                    'instrument': position.instrument,
                    'price': position.price,
                    'size': position.size,
                    'position_type': 'XCFuturesPosition',
                    'components_count': len(position.components)
                },
                'futures_data_info': {
                    'data_available': futures_df is not None and not futures_df.empty,
                    'instruments_requested': unique_instruments,
                    'instruments_found': futures_df.index.tolist() if futures_df is not None and not futures_df.empty else []
                }
            })
        
        elif isinstance(position, XCSwapPosition):
            
            
            # Calculate P&L
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
                'position_details': {
                    'handle': position.handle,
                    'instrument': position.instrument,
                    'total_components': len(position.xc_swaps),
                    'position_type': 'XCSwapPosition',
                    'price': position.price,
                    'size': position.size
                }
            })
        
        else:
            return jsonify({'error': 'Unknown position type'}), 500
        
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e)}), 500

@app.route('/restore_portfolio', methods=['POST'])
def restore_portfolio():
    """Restore portfolio from a backup (cancel unsaved changes)"""
    try:
        data = request.get_json()
        
        if not data or 'trades' not in data:
            return jsonify({'error': 'No backup data provided'}), 400
        
        backup_trades = data.get('trades', {})
        
        
        # Clear current portfolio
        portfolio.trades.clear()
        
        # Restore each trade from backup
        for trade_id, trade_data in backup_trades.items():
            trade = Trade(
                trade_id=trade_data['trade_id'],
                typology=trade_data.get('typology'),
                secondary_typology=trade_data.get('secondary_typology')
            )
            
            # Restore trade properties
            trade.instrument_details = trade_data.get('instrument_details', [])
            trade.instrument_details_secondary = trade_data.get('instrument_details_secondary', [])
            trade.prices = trade_data.get('prices', [])
            trade.sizes = trade_data.get('sizes', [])
            trade.prices_secondary = trade_data.get('prices_secondary', [])
            trade.sizes_secondary = trade_data.get('sizes_secondary', [])
            trade.stored_pnl = trade_data.get('stored_pnl', 0.0)
            trade.stored_pnl_primary = trade_data.get('stored_pnl_primary', 0.0)
            trade.stored_pnl_secondary = trade_data.get('stored_pnl_secondary', 0.0)
            trade.pnl_timestamp = trade_data.get('pnl_timestamp')
            
            # CRITICAL FIX: Restore group_id field
            trade.group_id = trade_data.get('group_id')
            
            # CRITICAL FIX: Restore insertion date arrays
            trade.primary_pos_insertion_dt = trade_data.get('primary_pos_insertion_dt', [])
            trade.secondary_pos_insertion_dt = trade_data.get('secondary_pos_insertion_dt', [])
            
            # CRITICAL FIX: Restore pnl_array for 1d PnL calculation
            trade.pnl_array = trade_data.get('pnl_array', [])
            trade.pnl_array_primary = trade_data.get('pnl_array_primary', [])
            trade.pnl_array_secondary = trade_data.get('pnl_array_secondary', [])
            
            # Add to portfolio
            portfolio.trades[trade_id] = trade
        
        # CRITICAL FIX: If curves are available, reinitialize positions to recreate XC objects
        # This ensures pnl_array is populated and 1d PnL calculation works
        if is_curves_loaded():
            print('✅ Curves available after restore - reinitializing positions...')
            successful_trades, total_positions = portfolio.initialize_positions()
            print(f'✅ Reinitialized {total_positions} positions for {successful_trades} trades')
        else:
            print('⚠️ No curves available after restore - using stored P&L values only')
        
        # Save restored portfolio to file
        portfolio.save_to_file()
        
        
        return jsonify({
            'success': True,
            'message': f'Portfolio restored with {len(portfolio.trades)} trades'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_trade_pnl_array/<trade_id>')
def get_trade_pnl_array(trade_id):
    """Get P&L time series array for a specific trade"""
    try:
        if trade_id not in portfolio.trades:
            return jsonify({'error': f'Trade {trade_id} not found'}), 404
        
        trade = portfolio.trades[trade_id]
        
        # Get the P&L array from the trade
        pnl_array = getattr(trade, 'pnl_array', [])
        pnl_array_primary = getattr(trade, 'pnl_array_primary', [])
        pnl_array_secondary = getattr(trade, 'pnl_array_secondary', [])
        
        if not pnl_array and not pnl_array_primary and not pnl_array_secondary:
            return jsonify({
                'success': False,
                'error': 'No P&L array data available for this trade. Try updating real-time P&L first.'
            }), 404
        
        return jsonify({
            'success': True,
            'pnl_array': pnl_array,
            'pnl_array_primary': pnl_array_primary,
            'pnl_array_secondary': pnl_array_secondary,
            'trade_id': trade_id
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_group_pnl_array/<group_id>')
def get_group_pnl_array(group_id):
    """Get combined P&L time series array for all trades in a group"""
    try:
        print(f"\n🔍 === GROUP PNL ARRAY REQUEST ===")
        print(f"Requested group_id: '{group_id}'")
        
        # Find all trades with this group_id
        group_trades = [trade for trade in portfolio.trades.values() 
                       if getattr(trade, 'group_id', None) == group_id]
        
        print(f"✅ Found {len(group_trades)} trades in group")
        
        if not group_trades:
            return jsonify({
                'success': False,
                'error': f'No trades found in group {group_id}'
            }), 404
        
        # Helper function to convert datetime to date
        def to_date(date_obj):
            """Convert datetime.datetime or datetime.date to datetime.date"""
            if isinstance(date_obj, datetime):
                return date_obj.date()
            return date_obj
        
        # Collect all dates and aggregate PnL by date (use datetime.date as keys)
        date_pnl_map = {}  # {date: total_pnl}
        date_primary_map = {}  # {date: primary_pnl}
        date_secondary_map = {}  # {date: secondary_pnl}
        
        for trade in group_trades:
            print(f"\n  Processing trade: {trade.trade_id}")
            
            # Get PnL arrays from trade
            pnl_array = getattr(trade, 'pnl_array', [])
            pnl_array_primary = getattr(trade, 'pnl_array_primary', [])
            pnl_array_secondary = getattr(trade, 'pnl_array_secondary', [])
            
            print(f"    Total entries: {len(pnl_array)}, Primary: {len(pnl_array_primary)}, Secondary: {len(pnl_array_secondary)}")
            
            # Aggregate total PnL - convert all dates to date objects
            for date_obj, pnl_value in pnl_array:
                date_key = to_date(date_obj)
                if date_key not in date_pnl_map:
                    date_pnl_map[date_key] = 0.0
                date_pnl_map[date_key] += pnl_value
            
            # Aggregate primary PnL - convert all dates to date objects
            for date_obj, pnl_value in pnl_array_primary:
                date_key = to_date(date_obj)
                if date_key not in date_primary_map:
                    date_primary_map[date_key] = 0.0
                date_primary_map[date_key] += pnl_value
            
            # Aggregate secondary PnL - convert all dates to date objects
            for date_obj, pnl_value in pnl_array_secondary:
                date_key = to_date(date_obj)
                if date_key not in date_secondary_map:
                    date_secondary_map[date_key] = 0.0
                date_secondary_map[date_key] += pnl_value
        
        # Check if we have any data
        if not date_pnl_map and not date_primary_map and not date_secondary_map:
            return jsonify({
                'success': False,
                'error': f'No P&L array data available for trades in group {group_id}. Try updating real-time P&L first.'
            }), 404
        
        # Convert to sorted arrays with ISO date strings
        combined_pnl_array = []
        combined_primary_array = []
        combined_secondary_array = []
        
        # Use union of all dates (all are now datetime.date objects, so sorting works)
        all_dates = set(date_pnl_map.keys()) | set(date_primary_map.keys()) | set(date_secondary_map.keys())
        
        for date_obj in sorted(all_dates):
            # Convert date to ISO string (YYYY-MM-DD format)
            date_str = date_obj.strftime('%Y-%m-%d')
            
            # Add to arrays
            combined_pnl_array.append([date_str, date_pnl_map.get(date_obj, 0.0)])
            combined_primary_array.append([date_str, date_primary_map.get(date_obj, 0.0)])
            combined_secondary_array.append([date_str, date_secondary_map.get(date_obj, 0.0)])
        
        print(f"\n✅ Returning combined arrays:")
        print(f"   Total array: {len(combined_pnl_array)} dates")
        print(f"   Primary array: {len(combined_primary_array)} dates")
        print(f"   Secondary array: {len(combined_secondary_array)} dates")
        if combined_pnl_array:
            print(f"   Sample data: {combined_pnl_array[:2]}")
        
        return jsonify({
            'success': True,
            'pnl_array': combined_pnl_array,
            'pnl_array_primary': combined_primary_array,
            'pnl_array_secondary': combined_secondary_array,
            'group_id': group_id,
            'num_trades': len(group_trades),
            'trade_ids': [trade.trade_id for trade in group_trades]
        })
        
    except Exception as e:
        print(f"\n❌ ERROR in get_group_pnl_array: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
