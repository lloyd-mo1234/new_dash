import cba.analytics.xcurves as xc
import xbbg.blp as blp
import numpy as np
import os
import glob
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))
import date_fn

formatted_date = "251002"

print(os.path.join("..", "..", "usd_curves", formatted_date + "_usd_curve.json"))

usd_curve = xc.Deserialise(os.path.join("..", "..", "usd_curves", formatted_date + "_usd_curve.json"), "usd.sofr.primary", True)
