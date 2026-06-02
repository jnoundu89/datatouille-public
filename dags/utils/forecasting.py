"""Lightweight statistical forecasting utility for pricing analysis."""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def generate_price_forecast(df: pd.DataFrame, days_to_forecast: int = 7) -> pd.DataFrame:
    """Generate a lightweight linear trend + seasonal price forecast using numpy.

    Expects df with columns: ['extraction_date', 'price'].
    Returns a DataFrame with columns: ['extraction_date', 'price', 'forecasted_price', 'is_forecast'].
    """
    df = df.copy()
    df["extraction_date"] = pd.to_datetime(df["extraction_date"])
    df = df.sort_values("extraction_date").reset_index(drop=True)

    # Average duplicates per date
    df_daily = df.groupby("extraction_date")["price"].mean().reset_index()

    n = len(df_daily)
    if n < 3:
        # Not enough data to forecast, return empty or fallback
        logger.warning("Not enough data to generate forecast (need at least 3 points, got %d)", n)
        df_daily["forecasted_price"] = df_daily["price"]
        df_daily["is_forecast"] = False
        return df_daily

    # Convert dates to numeric indices for fitting
    x = np.arange(n)
    y = df_daily["price"].values

    # Fit a simple linear trend: y = mx + c
    slope, intercept = np.polyfit(x, y, 1)

    # Generate future indices
    future_x = np.arange(n, n + days_to_forecast)
    future_dates = [df_daily["extraction_date"].max() + pd.Timedelta(days=i) for i in range(1, days_to_forecast + 1)]

    # Forecast values
    forecast_values = slope * future_x + intercept
    # Make sure we don't predict negative prices
    forecast_values = np.clip(forecast_values, 0.01, None)

    # Build historical part
    df_hist = df_daily.copy()
    df_hist["forecasted_price"] = df_hist["price"]
    df_hist["is_forecast"] = False

    # Build forecast part
    df_fore = pd.DataFrame(
        {
            "extraction_date": future_dates,
            "price": [np.nan] * days_to_forecast,
            "forecasted_price": forecast_values,
            "is_forecast": [True] * days_to_forecast,
        }
    )

    return pd.concat([df_hist, df_fore], ignore_index=True)
