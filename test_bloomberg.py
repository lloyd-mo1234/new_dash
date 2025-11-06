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

if __name__ == "__main__":
    print("=" * 70)
    print("Bloomberg xbbg BDP Function Analysis")
    print("=" * 70)
    
    # Run all tests
    inspect_bdp_function()
    test_bdp_parameter_formats()
    show_successful_example()
    show_help()
    
    print("\n" + "=" * 70)
    print("Analysis Complete!")
    print("=" * 70)
