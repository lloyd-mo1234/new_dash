"""
XC Package Functions Reference
=============================

This file contains the syntax and documentation for key XC package functions
used in the trading application for swap creation and P&L calculation.

Import: from cba.analytics import xcurves as xc
Last updated: 2025-10-30
"""

# xc.StandardSwap Function
"""
xc.StandardSwap - Creates a standard interest rate swap object

Syntax:
xc.StandardSwap(
    product_handle,      # string, required - Handle to reference the swap object
    template_name,       # string, required - Template to create swap from (e.g., 'AUD_BBSW_3M')
    settlement_date,     # string, optional - Settlement date (defaults to curve reference date)
    start_date,          # string, required - Swap's start date
    end_date,            # string, required - Swap's unadjusted end date
    notional,            # float, required - Swap's notional (positive to receive fixed rate)
    rate,                # float, required - Swap rate/spread
    term_spread=None,    # string, optional - Discount curve or CSA
    discount_curve=None, # float, optional - FX rate for xccy basis swaps
    fx_rate=None,        # float, optional - Margin on leg2/term leg
    roll_date=None       # string, optional - Swap's roll date
)

Returns: Swap object handle for valuation
"""

# xc.PresentValue Function
"""
xc.PresentValue - Calculates the present value (P&L) of a financial instrument

Syntax:
xc.PresentValue(
    curve_handle,        # string, required - Handle of the created curve block/bundle
    product_handle       # string, required - Handle of the created product
)

Returns: Present value of the given product (float)
"""

# Example Usage:
"""
# Import the module
from cba.analytics import xcurves as xc

# Create a 5-year AUD swap
swap_handle = 'swap_001'
xc.StandardSwap(
    product_handle=swap_handle,
    template_name='AUD_BBSW_3M',
    start_date='2024-01-01',
    end_date='2029-01-01',
    notional=25000000.0,    # 25 million as float
    rate=0.0314             # 3.14% as decimal
)

# Calculate P&L (requires curve bundle handle)
curve_handle = 'main_curves'  # From xcurves memory
pnl = xc.PresentValue(curve_handle, swap_handle)
print(f"P&L: ${pnl:,.2f}")
"""

# Additional Notes:
"""
- Rates should be converted from percentage to decimal (3.14% -> 0.0314)
- Notional should be a float (25000000.0 for 25 million)
- Start/end dates are derived from instrument syntax (e.g., aud.5y5y)
- P&L calculation requires access to curve bundles loaded in xcurves memory
- Product handles must be unique strings to reference created swaps
- Template names depend on currency and index (e.g., 'AUD_BBSW_3M', 'USD_LIBOR_3M')
"""
