"""
Bloomberg API test using xbbg - Function signature inspection
"""

def inspect_bdp_function():
    """Inspect the blp.bdp function signature and documentation"""
    try:
        import xbbg.blp as blp
        import inspect
        
        print("✅ xbbg imported successfully")
        print("=" * 60)
        print("INSPECTING blp.bdp FUNCTION")
        print("=" * 60)
        
        # Get function signature
        try:
            sig = inspect.signature(blp.bdp)
            print(f"📋 Function Signature: blp.bdp{sig}")
            
            # Get parameter details
            print("\n📋 Parameters:")
            for param_name, param in sig.parameters.items():
                print(f"   {param_name}:")
                print(f"     Type: {param.annotation if param.annotation != inspect.Parameter.empty else 'Not specified'}")
                print(f"     Default: {param.default if param.default != inspect.Parameter.empty else 'Required'}")
                print(f"     Kind: {param.kind}")
                
        except Exception as e:
            print(f"❌ Could not get function signature: {e}")
        
        # Get docstring
        try:
            doc = blp.bdp.__doc__
            if doc:
                print(f"\n📖 Documentation:")
                print(doc)
            else:
                print("\n⚠️ No documentation available")
        except Exception as e:
            print(f"❌ Could not get documentation: {e}")
            
        # Get source code if possible
        try:
            source = inspect.getsource(blp.bdp)
            print(f"\n💻 Source Code (first 20 lines):")
            lines = source.split('\n')[:20]
            for i, line in enumerate(lines, 1):
                print(f"   {i:2d}: {line}")
            if len(source.split('\n')) > 20:
                print(f"   ... ({len(source.split('\n')) - 20} more lines)")
        except Exception as e:
            print(f"⚠️ Could not get source code: {e}")
            
        return True
        
    except ImportError as e:
        print(f"❌ xbbg import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Inspection error: {e}")
        return False

def test_bdp_parameter_formats():
    """Test different parameter formats for blp.bdp"""
    try:
        import xbbg.blp as blp
        
        print("\n" + "=" * 60)
        print("TESTING DIFFERENT PARAMETER FORMATS")
        print("=" * 60)
        
        # Test data
        test_securities = ['YMZ5 Comdty']
        test_fields = ['FUT_TICK_SIZE', 'FUT_TICK_VAL', 'LAST_PRICE']
        
        print(f"📊 Test Data:")
        print(f"   Securities: {test_securities}")
        print(f"   Fields: {test_fields}")
        
        # Test Format 1: Keyword arguments
        print(f"\n🔧 Test 1: blp.bdp(securities=list, fields=list)")
        try:
            df1 = blp.bdp(securities=test_securities, fields=test_fields)
            print(f"✅ Format 1 SUCCESS - Shape: {df1.shape if df1 is not None else 'None'}")
            if df1 is not None and not df1.empty:
                print(f"   Columns: {list(df1.columns)}")
                print(f"   Index: {list(df1.index)}")
        except Exception as e:
            print(f"❌ Format 1 FAILED: {e}")
        
        # Test Format 2: Positional arguments
        print(f"\n🔧 Test 2: blp.bdp(list, list)")
        try:
            df2 = blp.bdp(test_securities, test_fields)
            print(f"✅ Format 2 SUCCESS - Shape: {df2.shape if df2 is not None else 'None'}")
            if df2 is not None and not df2.empty:
                print(f"   Columns: {list(df2.columns)}")
                print(f"   Index: {list(df2.index)}")
        except Exception as e:
            print(f"❌ Format 2 FAILED: {e}")
        
        # Test Format 3: Different parameter names
        print(f"\n🔧 Test 3: blp.bdp(tickers=list, flds=list)")
        try:
            df3 = blp.bdp(tickers=test_securities, flds=test_fields)
            print(f"✅ Format 3 SUCCESS - Shape: {df3.shape if df3 is not None else 'None'}")
            if df3 is not None and not df3.empty:
                print(f"   Columns: {list(df3.columns)}")
                print(f"   Index: {list(df3.index)}")
        except Exception as e:
            print(f"❌ Format 3 FAILED: {e}")
        
        # Test Format 4: Single strings instead of lists
        print(f"\n🔧 Test 4: blp.bdp(string, string)")
        try:
            df4 = blp.bdp(test_securities[0], test_fields[0])
            print(f"✅ Format 4 SUCCESS - Shape: {df4.shape if df4 is not None else 'None'}")
            if df4 is not None and not df4.empty:
                print(f"   Columns: {list(df4.columns)}")
                print(f"   Index: {list(df4.index)}")
        except Exception as e:
            print(f"❌ Format 4 FAILED: {e}")
        
        # Test Format 5: Mixed parameters
        print(f"\n🔧 Test 5: blp.bdp(string, list)")
        try:
            df5 = blp.bdp(test_securities[0], test_fields)
            print(f"✅ Format 5 SUCCESS - Shape: {df5.shape if df5 is not None else 'None'}")
            if df5 is not None and not df5.empty:
                print(f"   Columns: {list(df5.columns)}")
                print(f"   Index: {list(df5.index)}")
        except Exception as e:
            print(f"❌ Format 5 FAILED: {e}")
            
        return True
        
    except ImportError as e:
        print(f"❌ xbbg import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Testing error: {e}")
        return False

def show_successful_example():
    """Show a working example if any format succeeded"""
    try:
        import xbbg.blp as blp
        
        print("\n" + "=" * 60)
        print("WORKING EXAMPLE")
        print("=" * 60)
        
        # Try the most common format first
        test_securities = ['YMZ5 Comdty']
        test_fields = ['FUT_TICK_SIZE', 'FUT_TICK_VAL', 'LAST_PRICE']
        
        df = blp.bdp(test_securities, test_fields)
        
        if df is not None and not df.empty:
            print(f"✅ Working format: blp.bdp({test_securities}, {test_fields})")
            print(f"\n📊 Result:")
            print(df)
            
            # Show how to access the data
            print(f"\n🔍 Data Access Examples:")
            for instrument in df.index:
                print(f"   Instrument: {instrument}")
                for col in df.columns:
                    value = df.loc[instrument, col]
                    print(f"     {col}: {value}")
            
            return df
        else:
            print("⚠️ No successful format found")
            return None
            
    except Exception as e:
        print(f"❌ Example failed: {e}")
        return None

def show_help():
    """Show help for blp.bdp function"""
    try:
        import xbbg.blp as blp
        
        print("\n" + "=" * 60)
        print("HELP DOCUMENTATION")
        print("=" * 60)
        
        help(blp.bdp)
        
    except Exception as e:
        print(f"❌ Help failed: {e}")

def inspect_bdh_function():
    """Inspect the blp.bdh function signature and documentation"""
    try:
        import xbbg.blp as blp
        import inspect
        
        print("\n" + "=" * 60)
        print("INSPECTING blp.bdh FUNCTION (Historical Data)")
        print("=" * 60)
        
        # Get function signature
        try:
            sig = inspect.signature(blp.bdh)
            print(f"📋 Function Signature: blp.bdh{sig}")
            
            # Get parameter details
            print("\n📋 Parameters:")
            for param_name, param in sig.parameters.items():
                print(f"   {param_name}:")
                print(f"     Type: {param.annotation if param.annotation != inspect.Parameter.empty else 'Not specified'}")
                print(f"     Default: {param.default if param.default != inspect.Parameter.empty else 'Required'}")
                print(f"     Kind: {param.kind}")
                
        except Exception as e:
            print(f"❌ Could not get function signature: {e}")
        
        # Get docstring
        try:
            doc = blp.bdh.__doc__
            if doc:
                print(f"\n📖 Documentation:")
                print(doc)
            else:
                print("\n⚠️ No documentation available")
        except Exception as e:
            print(f"❌ Could not get documentation: {e}")
            
        return True
        
    except ImportError as e:
        print(f"❌ xbbg import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Inspection error: {e}")
        return False

def test_bdh_for_pnl():
    """Test BDH to pull historical LAST_PRICE data for PnL calculations"""
    try:
        import xbbg.blp as blp
        from datetime import datetime, timedelta
        
        print("\n" + "=" * 60)
        print("TESTING BDH FOR PnL CALCULATIONS")
        print("=" * 60)
        
        # Define test parameters
        test_security = 'YMZ5 Comdty'
        field = 'LAST_PRICE'
        
        # Define date range (last 10 business days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=14)  # Get ~10 business days
        
        print(f"📊 Test Parameters:")
        print(f"   Security: {test_security}")
        print(f"   Field: {field}")
        print(f"   Start Date: {start_date.strftime('%Y-%m-%d')}")
        print(f"   End Date: {end_date.strftime('%Y-%m-%d')}")
        
        # Test Format 1: With start and end dates
        print(f"\n🔧 Test 1: blp.bdh(tickers, flds, start_date, end_date)")
        try:
            df1 = blp.bdh(
                tickers=test_security,
                flds=field,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            print(f"✅ Format 1 SUCCESS - Shape: {df1.shape if df1 is not None else 'None'}")
            if df1 is not None and not df1.empty:
                print(f"   Columns: {list(df1.columns)}")
                print(f"   Index: {df1.index[:5].tolist() if len(df1) > 5 else df1.index.tolist()}")
                print(f"   First few rows:")
                print(df1.head())
                return df1
        except Exception as e:
            print(f"❌ Format 1 FAILED: {e}")
        
        # Test Format 2: Different date format
        print(f"\n🔧 Test 2: blp.bdh with datetime objects")
        try:
            df2 = blp.bdh(
                tickers=test_security,
                flds=field,
                start_date=start_date,
                end_date=end_date
            )
            print(f"✅ Format 2 SUCCESS - Shape: {df2.shape if df2 is not None else 'None'}")
            if df2 is not None and not df2.empty:
                print(f"   First few rows:")
                print(df2.head())
                return df2
        except Exception as e:
            print(f"❌ Format 2 FAILED: {e}")
        
        # Test Format 3: Positional arguments
        print(f"\n🔧 Test 3: blp.bdh(ticker, field, start, end)")
        try:
            df3 = blp.bdh(
                test_security,
                field,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            print(f"✅ Format 3 SUCCESS - Shape: {df3.shape if df3 is not None else 'None'}")
            if df3 is not None and not df3.empty:
                print(f"   First few rows:")
                print(df3.head())
                return df3
        except Exception as e:
            print(f"❌ Format 3 FAILED: {e}")
        
        return None
        
    except ImportError as e:
        print(f"❌ xbbg import failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Testing error: {e}")
        return None

def calculate_pnl_example(df):
    """Calculate PnL from historical price data"""
    try:
        print("\n" + "=" * 60)
        print("PnL CALCULATION EXAMPLE")
        print("=" * 60)
        
        if df is None or df.empty:
            print("❌ No data available for PnL calculation")
            return
        
        print(f"📊 Historical Price Data:")
        print(df)
        
        # Calculate daily returns
        print(f"\n📈 Daily Price Changes:")
        df_copy = df.copy()
        
        # Get the price column name (might be multi-level)
        if isinstance(df.columns, pd.MultiIndex):
            price_col = df.columns[0]
        else:
            price_col = df.columns[0] if len(df.columns) > 0 else 'LAST_PRICE'
        
        df_copy['Daily_Change'] = df_copy[price_col].diff()
        df_copy['Daily_Return_%'] = df_copy[price_col].pct_change() * 100
        df_copy['Cumulative_PnL'] = df_copy['Daily_Change'].cumsum()
        
        print(df_copy)
        
        # Summary statistics
        print(f"\n📊 Summary Statistics:")
        print(f"   Total Price Change: {df_copy['Daily_Change'].sum():.4f}")
        print(f"   Average Daily Change: {df_copy['Daily_Change'].mean():.4f}")
        print(f"   Max Daily Gain: {df_copy['Daily_Change'].max():.4f}")
        print(f"   Max Daily Loss: {df_copy['Daily_Change'].min():.4f}")
        print(f"   Total Return %: {((df[price_col].iloc[-1] / df[price_col].iloc[0]) - 1) * 100:.2f}%")
        
        return df_copy
        
    except Exception as e:
        print(f"❌ PnL calculation error: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_datetime_types_in_bdh():
    """Test whether BDH returns datetime.datetime or datetime.date objects"""
    try:
        import xbbg.blp as blp
        from datetime import datetime, timedelta, date
        import pandas as pd
        
        print("\n" + "=" * 60)
        print("TESTING DATETIME TYPES IN BDH RESULT")
        print("=" * 60)
        
        # Define test parameters
        test_security = 'YMZ5 Comdty'
        field = 'LAST_PRICE'
        
        # Define date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"📊 Test Parameters:")
        print(f"   Security: {test_security}")
        print(f"   Field: {field}")
        print(f"   Start Date: {start_date.strftime('%Y-%m-%d')}")
        print(f"   End Date: {end_date.strftime('%Y-%m-%d')}")
        
        # Get historical data
        print(f"\n🔧 Fetching historical data...")
        try:
            df = blp.bdh(
                tickers=test_security,
                flds=field,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if df is None or df.empty:
                print("❌ No data returned")
                return None
            
            print(f"✅ Data fetched - Shape: {df.shape}")
            
            # Check the index type
            print(f"\n🔍 DATETIME TYPE ANALYSIS:")
            print(f"   DataFrame Index Type: {type(df.index)}")
            print(f"   DataFrame Index dtype: {df.index.dtype}")
            
            # Check first few index values
            print(f"\n   First 5 index values:")
            for i, idx_val in enumerate(df.index[:5]):
                print(f"     [{i}] Value: {idx_val}")
                print(f"         Type: {type(idx_val)}")
                print(f"         Is datetime.datetime? {isinstance(idx_val, datetime)}")
                print(f"         Is datetime.date? {isinstance(idx_val, date)}")
                print(f"         Is pd.Timestamp? {isinstance(idx_val, pd.Timestamp)}")
                
                # If it's a Timestamp, check if it has time component
                if isinstance(idx_val, pd.Timestamp):
                    print(f"         Has time component? {idx_val.hour != 0 or idx_val.minute != 0 or idx_val.second != 0}")
                    print(f"         Time component: {idx_val.time()}")
                    print(f"         Date only: {idx_val.date()}")
                print()
            
            # Test conversion scenarios
            print(f"🔧 CONVERSION TESTS:")
            
            # Test 1: Can we convert to datetime.date?
            try:
                first_date = df.index[0].date() if hasattr(df.index[0], 'date') else df.index[0]
                print(f"   ✅ Can convert to date(): {first_date} (type: {type(first_date)})")
            except Exception as e:
                print(f"   ❌ Cannot convert to date(): {e}")
            
            # Test 2: Can we create datetime from it?
            try:
                if isinstance(df.index[0], pd.Timestamp):
                    first_datetime = df.index[0].to_pydatetime()
                    print(f"   ✅ Can convert to datetime: {first_datetime} (type: {type(first_datetime)})")
                else:
                    print(f"   ⚠️ Not a Timestamp, cannot test to_pydatetime()")
            except Exception as e:
                print(f"   ❌ Cannot convert to datetime: {e}")
            
            # Test 3: Comparison with date string
            try:
                test_date_str = "2025-08-17"
                test_date = datetime.strptime(test_date_str, "%Y-%m-%d").date()
                print(f"\n   🧪 Testing comparison with date string '{test_date_str}':")
                
                # Try comparing with first index value
                first_idx_as_date = df.index[0].date() if hasattr(df.index[0], 'date') else df.index[0]
                comparison_result = first_idx_as_date >= test_date
                print(f"      {first_idx_as_date} >= {test_date} = {comparison_result}")
                
            except Exception as e:
                print(f"   ❌ Comparison test failed: {e}")
            
            # Show the actual dataframe
            print(f"\n📊 Sample Data:")
            print(df.head(10))
            
            return df
            
        except Exception as e:
            print(f"❌ Data fetch failed: {e}")
            import traceback
            traceback.print_exc()
            return None
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()
        return None

def show_bdh_help():
    """Show help for blp.bdh function"""
    try:
        import xbbg.blp as blp
        
        print("\n" + "=" * 60)
        print("BDH HELP DOCUMENTATION")
        print("=" * 60)
        
        help(blp.bdh)
        
    except Exception as e:
        print(f"❌ Help failed: {e}")

if __name__ == "__main__":
    import pandas as pd
    
    print("=" * 70)
    print("Bloomberg xbbg Function Analysis")
    print("=" * 70)
    
    # Test BDP (Reference Data)
    print("\n### PART 1: BDP (Reference Data) ###")
    inspect_bdp_function()
    test_bdp_parameter_formats()
    show_successful_example()
    
    # Test BDH (Historical Data) - For PnL calculations
    print("\n\n### PART 2: BDH (Historical Data) - For PnL ###")
    inspect_bdh_function()
    historical_df = test_bdh_for_pnl()
    
    if historical_df is not None:
        calculate_pnl_example(historical_df)
    
    # Test datetime types in BDH result
    print("\n\n### PART 3: DATETIME TYPE TESTING ###")
    test_datetime_types_in_bdh()
    
    show_bdh_help()
    
    print("\n" + "=" * 70)
    print("Analysis Complete!")
    print("=" * 70)
