import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
from scipy import stats
import plotly.graph_objs as go
import plotly.utils
import json
from datetime import datetime, timedelta
from enhanced_simplified_swap_functions import get_enhanced_swap_data
from performance_optimizations import (
    memory_cache_with_lru, disk_cache, ChartDataOptimizer
)

def get_common_dates(dataframes):
    """Find common dates across multiple dataframes"""
    if not dataframes:
        return []
    
    common_dates = set(dataframes[0]['Date'])
    for df in dataframes[1:]:
        common_dates = common_dates.intersection(set(df['Date']))
    
    return sorted(list(common_dates))

def filter_data_by_range_regression(df, range_filter):
    """Filter dataframe by date range for regression analysis"""
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

@memory_cache_with_lru(maxsize=50)  # Cache up to 50 regression data preparations
def prepare_regression_data(y_variable, x_variables, range_filter='MAX'):
    """
    Prepare data for regression analysis (with caching)
    
    Args:
        y_variable: String with tenor syntax for dependent variable
        x_variables: List of strings with tenor syntax for independent variables
        range_filter: Date range filter
    
    Returns:
        dict: Contains prepared data and any errors
    """
    try:
        # Get data for all variables
        all_dataframes = []
        variable_names = ['Y'] + [f'X{i+1}' for i in range(len(x_variables))]
        
        # Get Y variable data
        y_df, y_error = get_enhanced_swap_data(y_variable)
        if y_error or y_df is None or y_df.empty:
            return {'error': f'Error loading Y variable ({y_variable}): {y_error}'}
        all_dataframes.append(y_df)
        
        # Get X variables data
        x_dataframes = []
        for i, x_var in enumerate(x_variables):
            if x_var.strip():  # Only process non-empty variables
                x_df, x_error = get_enhanced_swap_data(x_var)
                if x_error or x_df is None or x_df.empty:
                    return {'error': f'Error loading X{i+1} variable ({x_var}): {x_error}'}
                all_dataframes.append(x_df)
                x_dataframes.append(x_df)
        
        if not x_dataframes:
            return {'error': 'At least one X variable is required'}
        
        # Find common dates
        common_dates = get_common_dates(all_dataframes)
        if not common_dates:
            return {'error': 'No common dates found across all variables'}
        
        # Create combined dataframe with common dates
        result_data = []
        for date in common_dates:
            row = {'Date': date}
            
            # Get Y value
            y_row = y_df[y_df['Date'] == date]
            if not y_row.empty:
                row['Y'] = y_row['Rate'].iloc[0]
            else:
                continue
            
            # Get X values
            valid_row = True
            for i, x_df in enumerate(x_dataframes):
                x_row = x_df[x_df['Date'] == date]
                if not x_row.empty:
                    row[f'X{i+1}'] = x_row['Rate'].iloc[0]
                else:
                    valid_row = False
                    break
            
            if valid_row:
                result_data.append(row)
        
        if not result_data:
            return {'error': 'No valid data points found'}
        
        # Convert to DataFrame
        combined_df = pd.DataFrame(result_data)
        
        # Apply date range filter
        combined_df = filter_data_by_range_regression(combined_df, range_filter)
        
        if combined_df.empty:
            return {'error': 'No data available for selected date range'}
        
        return {
            'data': combined_df,
            'variable_names': variable_names,
            'y_variable': y_variable,
            'x_variables': [x for x in x_variables if x.strip()],
            'date_range': (combined_df['Date'].min(), combined_df['Date'].max()),
            'n_observations': len(combined_df)
        }
        
    except Exception as e:
        return {'error': f'Error preparing regression data: {str(e)}'}

def perform_regression_analysis(prepared_data):
    """
    Perform regression analysis on prepared data
    
    Args:
        prepared_data: Dictionary from prepare_regression_data
    
    Returns:
        dict: Regression results and statistics
    """
    try:
        if 'error' in prepared_data:
            return prepared_data
        
        df = prepared_data['data']
        
        # Prepare X and y arrays
        y = df['Y'].values
        X_cols = [col for col in df.columns if col.startswith('X')]
        X = df[X_cols].values
        
        # Fit regression model
        model = LinearRegression()
        model.fit(X, y)
        
        # Make predictions
        y_pred = model.predict(X)
        residuals = y - y_pred
        
        # Calculate statistics
        n = len(y)
        k = X.shape[1]  # number of predictors
        
        # R-squared
        r2 = r2_score(y, y_pred)
        
        # Adjusted R-squared
        adj_r2 = 1 - (1 - r2) * (n - 1) / (n - k - 1)
        
        # Standard error of regression
        mse = mean_squared_error(y, y_pred)
        std_error = np.sqrt(mse)
        
        # F-statistic
        ss_res = np.sum(residuals ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        f_stat = ((ss_tot - ss_res) / k) / (ss_res / (n - k - 1))
        f_p_value = 1 - stats.f.cdf(f_stat, k, n - k - 1)
        
        # T-statistics for coefficients
        # Calculate standard errors for coefficients
        X_with_intercept = np.column_stack([np.ones(n), X])
        try:
            cov_matrix = np.linalg.inv(X_with_intercept.T @ X_with_intercept) * mse
            std_errors = np.sqrt(np.diag(cov_matrix))
            
            # Coefficients with intercept
            coefficients = np.concatenate([[model.intercept_], model.coef_])
            t_stats = coefficients / std_errors
            t_p_values = 2 * (1 - stats.t.cdf(np.abs(t_stats), n - k - 1))
        except:
            # Fallback if matrix inversion fails
            std_errors = np.full(k + 1, np.nan)
            t_stats = np.full(k + 1, np.nan)
            t_p_values = np.full(k + 1, np.nan)
        
        # Prepare results
        results = {
            'model': model,
            'predictions': y_pred,
            'residuals': residuals,
            'dates': df['Date'].values,
            'y_actual': y,
            'X_data': X,
            'statistics': {
                'r_squared': r2,
                'adj_r_squared': adj_r2,
                'std_error': std_error,
                'f_statistic': f_stat,
                'f_p_value': f_p_value,
                'n_observations': n,
                'n_predictors': k
            },
            'coefficients': {
                'intercept': model.intercept_,
                'slopes': model.coef_,
                'std_errors': std_errors,
                't_statistics': t_stats,
                'p_values': t_p_values
            },
            'variable_info': {
                'y_variable': prepared_data['y_variable'],
                'x_variables': prepared_data['x_variables'],
                'variable_names': prepared_data['variable_names']
            }
        }
        
        return results
        
    except Exception as e:
        return {'error': f'Error performing regression analysis: {str(e)}'}

def create_regression_charts(regression_results, dark_theme):
    """
    Create all regression charts
    
    Args:
        regression_results: Results from perform_regression_analysis
        dark_theme: Dark theme configuration
    
    Returns:
        dict: Contains all chart JSONs
    """
    try:
        if 'error' in regression_results:
            return regression_results
        
        charts = {}
        
        # 1. Scatter plot with line of best fit
        charts['scatter'] = create_scatter_plot(regression_results, dark_theme)
        
        # 2. Residuals time series
        charts['residuals'] = create_residuals_chart(regression_results, dark_theme)
        
        # 3. Time series of all variables
        charts['timeseries'] = create_variables_timeseries(regression_results, dark_theme)
        
        return charts
        
    except Exception as e:
        return {'error': f'Error creating regression charts: {str(e)}'}

def create_scatter_plot(regression_results, dark_theme):
    """Create scatter plot of Y vs first X variable with line of best fit and time-based color buckets"""
    try:
        # Get data
        y_actual = regression_results['y_actual']
        X_data = regression_results['X_data']
        y_pred = regression_results['predictions']
        dates = regression_results['dates']
        
        # Get variable names for titles and axis labels
        y_variable = regression_results['variable_info']['y_variable']
        x_variables = regression_results['variable_info']['x_variables']
        x_first_variable = x_variables[0] if len(x_variables) > 0 else 'X1'
        
        # Use first X variable for scatter plot
        x_first = X_data[:, 0] if X_data.shape[1] > 0 else None
        if x_first is None:
            return json.dumps({'error': 'No X variable data available'})
        
        # Convert to lists if they're numpy arrays
        if hasattr(y_actual, 'tolist'):
            y_actual = y_actual.tolist()
        if hasattr(x_first, 'tolist'):
            x_first = x_first.tolist()
        if hasattr(y_pred, 'tolist'):
            y_pred = y_pred.tolist()
        if hasattr(dates, 'tolist'):
            dates = dates.tolist()
        
        if len(y_actual) == 0 or len(x_first) == 0:
            return json.dumps({'error': 'No data available for scatter plot'})
        
        fig = go.Figure()
        
        # Define 4 time buckets with different colors and date range labels
        n_points = len(y_actual)
        bucket_size = max(1, n_points // 4)
        colors = ['#00d4ff', '#51cf66', '#ffd43b', '#ff6b6b']  # Blue, Green, Yellow, Red
        
        # Create date range labels for buckets
        bucket_names = []
        for i in range(4):
            start_idx = i * bucket_size
            if i == 3:  # Last bucket gets remaining points
                end_idx = n_points
            else:
                end_idx = (i + 1) * bucket_size
            
            if start_idx < n_points:
                start_date = dates[start_idx]
                end_date = dates[min(end_idx - 1, n_points - 1)]
                
                # Format dates as Jul 25 - Aug 25
                from datetime import datetime
                import pandas as pd
                
                def convert_to_datetime(date_val):
                    """Convert various date formats to datetime object"""
                    if hasattr(date_val, 'strftime'):
                        return date_val
                    elif isinstance(date_val, (int, float)):
                        # Handle timestamp or Excel serial date
                        try:
                            # Try as pandas timestamp (nanoseconds since epoch)
                            return pd.to_datetime(date_val)
                        except:
                            try:
                                # Try as Excel serial date
                                return pd.to_datetime(date_val, unit='D', origin='1899-12-30')
                            except:
                                try:
                                    # Try as Unix timestamp
                                    return pd.to_datetime(date_val, unit='s')
                                except:
                                    return None
                    else:
                        # Handle string dates
                        try:
                            return pd.to_datetime(str(date_val))
                        except:
                            return None
                
                start_dt = convert_to_datetime(start_date)
                end_dt = convert_to_datetime(end_date)
                
                if start_dt and end_dt:
                    start_str = start_dt.strftime('%b %y')
                    end_str = end_dt.strftime('%b %y')
                else:
                    start_str = f'Period {i+1} Start'
                    end_str = f'Period {i+1} End'
                
                if start_str == end_str:
                    bucket_names.append(start_str)
                else:
                    bucket_names.append(f'{start_str} - {end_str}')
        
        # Create 4 time-based buckets for scatter points
        for i in range(4):
            start_idx = i * bucket_size
            if i == 3:  # Last bucket gets remaining points
                end_idx = n_points
            else:
                end_idx = (i + 1) * bucket_size
            
            if start_idx < n_points:
                bucket_x = x_first[start_idx:end_idx]
                bucket_y = y_actual[start_idx:end_idx]
                
                fig.add_trace(go.Scatter(
                    x=bucket_x,
                    y=bucket_y,
                    mode='markers',
                    name=bucket_names[i] if i < len(bucket_names) else f'Period {i+1}',
                    marker=dict(
                        color=colors[i],
                        size=8,
                        opacity=0.8
                    ),
                    hovertemplate='<b>X:</b> %{x:.3f}%<br><b>Y:</b> %{y:.3f}%<extra></extra>'
                ))
        
        # Add line of best fit using linear regression
        # Sort by X values for proper line drawing
        sorted_indices = sorted(range(len(x_first)), key=lambda i: x_first[i])
        x_sorted = [x_first[i] for i in sorted_indices]
        y_pred_sorted = [y_pred[i] for i in sorted_indices]
        
        fig.add_trace(go.Scatter(
            x=x_sorted,
            y=y_pred_sorted,
            mode='lines',
            name='Line of Best Fit',
            line=dict(
                color='#8b949e',
                width=3,
                dash='solid'
            ),
            hovertemplate='<b>Best Fit:</b> %{y:.3f}%<extra></extra>'
        ))
        
        # Highlight latest point
        if len(x_first) > 0 and len(y_actual) > 0:
            fig.add_trace(go.Scatter(
                x=[x_first[-1]],
                y=[y_actual[-1]],
                mode='markers',
                name='Latest Point',
                marker=dict(
                    color='white',
                    size=12,
                    line=dict(color='#ff6b6b', width=2)
                ),
                hovertemplate='<b>Latest:</b> X=%{x:.3f}%, Y=%{y:.3f}%<extra></extra>'
            ))
        
        # Create chart title and axis labels
        chart_title = f'{y_variable.upper()} vs {x_first_variable.upper()}'
        
        # Update layout with title, axis labels, and legend in bottom right
        fig.update_layout(
            title=dict(
                text=chart_title,
                x=0,  # Left align
                xanchor='left',
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=14,  # Same size as input box text (0.91rem ≈ 14px)
                    color=dark_theme['font_color']
                )
            ),
            xaxis_title=dict(
                text=x_first_variable.upper(),
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=10,  # 2/3 of input box size
                    color=dark_theme['font_color']
                )
            ),
            yaxis_title=dict(
                text=y_variable.upper(),
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=10,  # 2/3 of input box size
                    color=dark_theme['font_color']
                )
            ),
            plot_bgcolor=dark_theme['plot_bgcolor'],
            paper_bgcolor=dark_theme['paper_bgcolor'],
            font=dict(color=dark_theme['font_color'], family='Avenir, Helvetica Neue, Arial, sans-serif'),
            showlegend=True,
            legend=dict(
                orientation='v',
                x=0.98,  # Bottom right corner
                xanchor='right',
                y=0.02,
                yanchor='bottom',
                font=dict(family='Avenir, Helvetica Neue, Arial, sans-serif', size=9),
                bgcolor="rgba(30, 30, 30, 0.9)",
                bordercolor="#333333",
                borderwidth=1
            ),
            hovermode='closest',
            margin=dict(l=60, r=60, t=40, b=60)
        )
        
        fig.update_xaxes(
            gridcolor=dark_theme['grid_color'], 
            color=dark_theme['font_color'],
            linecolor=dark_theme['grid_color'],
            linewidth=1,
            mirror=True,
            title_standoff=10
        )
        fig.update_yaxes(
            gridcolor=dark_theme['grid_color'], 
            color=dark_theme['font_color'],
            linecolor=dark_theme['grid_color'],
            linewidth=1,
            mirror=True,
            title_standoff=10
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
    except Exception as e:
        print(f"DEBUG: Error in create_scatter_plot: {str(e)}")
        return json.dumps({'error': f'Error creating scatter plot: {str(e)}'})

def create_residuals_chart(regression_results, dark_theme):
    """Create residuals vs first X variable chart with time-based color buckets"""
    try:
        X_data = regression_results['X_data']
        residuals = regression_results['residuals']
        dates = regression_results['dates']
        
        # Get variable names for titles and axis labels
        x_variables = regression_results['variable_info']['x_variables']
        x_first_variable = x_variables[0] if len(x_variables) > 0 else 'X1'
        
        # Use first X variable
        x_first = X_data[:, 0] if X_data.shape[1] > 0 else None
        if x_first is None:
            return json.dumps({'error': 'No X variable data available'})
        
        # Convert to lists if they're numpy arrays
        if hasattr(x_first, 'tolist'):
            x_first = x_first.tolist()
        if hasattr(residuals, 'tolist'):
            residuals = residuals.tolist()
        if hasattr(dates, 'tolist'):
            dates = dates.tolist()
        
        fig = go.Figure()
        
        # Define 4 time buckets with same colors as scatter plot
        n_points = len(residuals)
        bucket_size = max(1, n_points // 4)
        colors = ['#00d4ff', '#51cf66', '#ffd43b', '#ff6b6b']  # Blue, Green, Yellow, Red
        
        # Create date range labels for buckets (same logic as scatter plot)
        bucket_names = []
        for i in range(4):
            start_idx = i * bucket_size
            if i == 3:  # Last bucket gets remaining points
                end_idx = n_points
            else:
                end_idx = (i + 1) * bucket_size
            
            if start_idx < n_points:
                start_date = dates[start_idx]
                end_date = dates[min(end_idx - 1, n_points - 1)]
                
                # Format dates as Jul 25 - Aug 25
                from datetime import datetime
                import pandas as pd
                
                def convert_to_datetime(date_val):
                    """Convert various date formats to datetime object"""
                    if hasattr(date_val, 'strftime'):
                        return date_val
                    elif isinstance(date_val, (int, float)):
                        # Handle timestamp or Excel serial date
                        try:
                            # Try as pandas timestamp (nanoseconds since epoch)
                            return pd.to_datetime(date_val)
                        except:
                            try:
                                # Try as Excel serial date
                                return pd.to_datetime(date_val, unit='D', origin='1899-12-30')
                            except:
                                try:
                                    # Try as Unix timestamp
                                    return pd.to_datetime(date_val, unit='s')
                                except:
                                    return None
                    else:
                        # Handle string dates
                        try:
                            return pd.to_datetime(str(date_val))
                        except:
                            return None
                
                start_dt = convert_to_datetime(start_date)
                end_dt = convert_to_datetime(end_date)
                
                if start_dt and end_dt:
                    start_str = start_dt.strftime('%b %y')
                    end_str = end_dt.strftime('%b %y')
                else:
                    start_str = f'Period {i+1} Start'
                    end_str = f'Period {i+1} End'
                
                if start_str == end_str:
                    bucket_names.append(start_str)
                else:
                    bucket_names.append(f'{start_str} - {end_str}')
        
        # Create 4 time-based buckets for residual points
        for i in range(4):
            start_idx = i * bucket_size
            if i == 3:  # Last bucket gets remaining points
                end_idx = n_points
            else:
                end_idx = (i + 1) * bucket_size
            
            if start_idx < n_points:
                bucket_x = x_first[start_idx:end_idx]
                bucket_residuals = residuals[start_idx:end_idx]
                
                fig.add_trace(go.Scatter(
                    x=bucket_x,
                    y=bucket_residuals,
                    mode='markers',
                    name=bucket_names[i] if i < len(bucket_names) else f'Period {i+1}',
                    marker=dict(
                        color=colors[i],
                        size=6,
                        opacity=0.7
                    ),
                    hovertemplate='<b>X:</b> %{x:.3f}%<br><b>Residual:</b> %{y:.3f}%<extra></extra>'
                ))
        
        # Zero line
        fig.add_hline(
            y=0,
            line_dash="dash",
            line_color="#ff6b6b",
            line_width=2
        )
        
        # Highlight latest residual point
        if len(x_first) > 0 and len(residuals) > 0:
            fig.add_trace(go.Scatter(
                x=[x_first[-1]],
                y=[residuals[-1]],
                mode='markers',
                name='Latest Point',
                marker=dict(
                    color='white',
                    size=12,
                    line=dict(color='#ff6b6b', width=2)
                ),
                hovertemplate='<b>Latest:</b> X=%{x:.3f}%, Residual=%{y:.3f}%<extra></extra>'
            ))
        
        # Update layout with title, axis labels, and legend in bottom right
        fig.update_layout(
            title=dict(
                text='Residual vs Model',
                x=0,  # Left align
                xanchor='left',
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=14,  # Same size as input box text (0.91rem ≈ 14px)
                    color=dark_theme['font_color']
                )
            ),
            xaxis_title=dict(
                text=x_first_variable.upper(),
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=10,  # 2/3 of input box size
                    color=dark_theme['font_color']
                )
            ),
            yaxis_title=dict(
                text='RESIDUAL',
                font=dict(
                    family='Avenir, Helvetica Neue, Arial, sans-serif',
                    size=10,  # 2/3 of input box size
                    color=dark_theme['font_color']
                )
            ),
            plot_bgcolor=dark_theme['plot_bgcolor'],
            paper_bgcolor=dark_theme['paper_bgcolor'],
            font=dict(color=dark_theme['font_color'], family='Avenir, Helvetica Neue, Arial, sans-serif'),
            showlegend=True,
            legend=dict(
                orientation='v',
                x=0.98,  # Bottom right corner
                xanchor='right',
                y=0.02,
                yanchor='bottom',
                font=dict(family='Avenir, Helvetica Neue, Arial, sans-serif', size=9),
                bgcolor="rgba(30, 30, 30, 0.9)",
                bordercolor="#333333",
                borderwidth=1
            ),
            hovermode='closest',
            margin=dict(l=60, r=60, t=40, b=60)
        )
        
        fig.update_xaxes(
            gridcolor=dark_theme['grid_color'], 
            color=dark_theme['font_color'],
            linecolor=dark_theme['grid_color'],
            linewidth=1,
            mirror=True,
            title_standoff=10
        )
        fig.update_yaxes(
            gridcolor=dark_theme['grid_color'], 
            color=dark_theme['font_color'],
            linecolor=dark_theme['grid_color'],
            linewidth=1,
            mirror=True,
            title_standoff=10
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
    except Exception as e:
        return json.dumps({'error': f'Error creating residuals chart: {str(e)}'})

def create_variables_timeseries(regression_results, dark_theme):
    """Create time series chart of all variables for the same date range as regression"""
    try:
        # Get the regression date range
        regression_dates = regression_results['dates']
        if hasattr(regression_dates, 'tolist'):
            regression_dates = regression_dates.tolist()
        
        start_date = min(regression_dates)
        end_date = max(regression_dates)
        
        # Get the original data for all variables
        y_variable = regression_results['variable_info']['y_variable']
        x_variables = regression_results['variable_info']['x_variables']
        
        fig = go.Figure()
        colors = ['#00d4ff', '#ff6b6b', '#51cf66', '#ffd43b']
        
        # Get data for each variable
        all_variables = [y_variable] + x_variables
        variable_labels = ['Y'] + [f'X{i+1}' for i in range(len(x_variables))]
        
        for i, (var_syntax, label) in enumerate(zip(all_variables, variable_labels)):
            df, error = get_enhanced_swap_data(var_syntax)
            if not error and df is not None and not df.empty:
                # Filter to same date range as regression
                filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
                
                if not filtered_df.empty:
                    fig.add_trace(go.Scatter(
                        x=filtered_df['Date'],
                        y=filtered_df['Rate'],
                        mode='lines',
                        name=f'{label}: {var_syntax.upper()}',
                        line=dict(
                            color=colors[i % len(colors)],
                            width=2
                        ),
                        hovertemplate='<b>Date:</b> %{x}<br><b>Rate:</b> %{y:.3f}%<extra></extra>'
                    ))
        
        # Update layout with legend below
        fig.update_layout(
            title='',  # Remove title
            xaxis_title='',  # Remove x-axis title
            yaxis_title='',  # Remove y-axis title
            plot_bgcolor=dark_theme['plot_bgcolor'],
            paper_bgcolor=dark_theme['paper_bgcolor'],
            font=dict(color=dark_theme['font_color'], family='Inter, sans-serif'),
            showlegend=True,
            legend=dict(
                orientation='h',
                x=0.5,
                xanchor='center',
                y=-0.15,
                font=dict(family='Inter, sans-serif', size=9)
            ),
            hovermode='x unified',
            margin=dict(l=40, r=40, t=20, b=80)
        )
        
        fig.update_xaxes(gridcolor=dark_theme['grid_color'], color=dark_theme['font_color'])
        fig.update_yaxes(gridcolor=dark_theme['grid_color'], color=dark_theme['font_color'])
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
    except Exception as e:
        return json.dumps({'error': f'Error creating variables time series: {str(e)}'})

def format_regression_statistics(regression_results):
    """Format regression statistics for display"""
    try:
        if 'error' in regression_results:
            return regression_results
        
        stats = regression_results['statistics']
        coeffs = regression_results['coefficients']
        var_info = regression_results['variable_info']
        
        # Format main statistics
        formatted_stats = {
            'r_squared': f"{stats['r_squared']:.4f}",
            'adj_r_squared': f"{stats['adj_r_squared']:.4f}",
            'std_error': f"{stats['std_error']:.4f}",
            'f_statistic': f"{stats['f_statistic']:.4f}",
            'f_p_value': f"{stats['f_p_value']:.4f}",
            'n_observations': stats['n_observations'],
            'n_predictors': stats['n_predictors']
        }
        
        # Format coefficients table
        coeff_table = []
        
        # Intercept
        coeff_table.append({
            'variable': 'Intercept',
            'coefficient': f"{coeffs['intercept']:.6f}",
            'std_error': f"{coeffs['std_errors'][0]:.6f}" if not np.isnan(coeffs['std_errors'][0]) else 'N/A',
            't_statistic': f"{coeffs['t_statistics'][0]:.4f}" if not np.isnan(coeffs['t_statistics'][0]) else 'N/A',
            'p_value': f"{coeffs['p_values'][0]:.4f}" if not np.isnan(coeffs['p_values'][0]) else 'N/A'
        })
        
        # Slopes
        for i, x_var in enumerate(var_info['x_variables']):
            coeff_table.append({
                'variable': f'X{i+1} ({x_var})',
                'coefficient': f"{coeffs['slopes'][i]:.6f}",
                'std_error': f"{coeffs['std_errors'][i+1]:.6f}" if not np.isnan(coeffs['std_errors'][i+1]) else 'N/A',
                't_statistic': f"{coeffs['t_statistics'][i+1]:.4f}" if not np.isnan(coeffs['t_statistics'][i+1]) else 'N/A',
                'p_value': f"{coeffs['p_values'][i+1]:.4f}" if not np.isnan(coeffs['p_values'][i+1]) else 'N/A'
            })
        
        return {
            'statistics': formatted_stats,
            'coefficients': coeff_table,
            'variable_info': var_info
        }
        
    except Exception as e:
        return {'error': f'Error formatting statistics: {str(e)}'}
