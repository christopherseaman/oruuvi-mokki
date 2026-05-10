# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy", "pandas", "scipy"]
# ///
"""Unsupervised look at residualized pressure noise.

No labels assumed. Pipeline:
  1. detrend = raw - rolling_mean(30min, trailing)   # high-pass filter
  2. for each window w in {30s, 1min, 3min, 5min, 10min}:
       sd_res = rolling_std(detrend, w)              # residual noise envelope
  3. examine the resulting distributions:
       - is it bimodal? (Sarle's bimodality coefficient)
       - what's the time-series structure?
  4. let any clusters declare themselves.
"""
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import skew, kurtosis, gaussian_kde

db = sqlite3.connect("/home/christopher/projects/oruuvi-mokki/data/ruuvi.db")
df = pd.read_sql_query("SELECT epoch_ms, pressure_hpa FROM readings ORDER BY id", db)
df["t"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True).dt.tz_convert("America/Los_Angeles")
df = df.set_index("t")

# detrend with a slow trailing mean — removes weather + slow drift
df["trend_30min"] = df["pressure_hpa"].rolling("30min").mean()
df["resid_pa"]   = (df["pressure_hpa"] - df["trend_30min"]) * 100

WINDOWS = ["30s", "1min", "3min", "5min", "10min"]
for w in WINDOWS:
    df[f"sd_res_{w}"] = df["resid_pa"].rolling(w).std()

# drop warm-up where 30min trend isn't valid yet, and the BLE-handling spike
WARMUP_END = df.index[0] + pd.Timedelta("45min")
mask = df.index >= WARMUP_END
df = df[mask]

# Sarle's bimodality coefficient: BC = (g1^2 + 1) / (g2 + 3*(n-1)^2/((n-2)(n-3)))
# BC > 5/9 (~0.555) suggests bimodality (sometimes); shape of histogram is the real test.
def bimodality_coefficient(x):
    x = np.asarray(x); x = x[~np.isnan(x)]
    if len(x) < 50: return float("nan")
    g1 = skew(x); g2 = kurtosis(x, fisher=True)  # excess
    n = len(x)
    return (g1**2 + 1) / (g2 + 3 * (n-1)**2 / ((n-2)*(n-3)))

print(f"{'window':<8} {'n':>7} {'mean':>6} {'sd':>6} {'p10':>6} {'p50':>6} {'p90':>6} {'BC':>6}")
for w in WINDOWS:
    x = df[f"sd_res_{w}"].dropna().values
    if len(x) == 0: continue
    bc = bimodality_coefficient(x)
    print(f"{w:<8} {len(x):>7} {x.mean():>6.2f} {x.std():>6.2f} "
          f"{np.percentile(x,10):>6.2f} {np.percentile(x,50):>6.2f} {np.percentile(x,90):>6.2f} {bc:>6.3f}")

# 2-row figure: timeseries (top) + histogram with KDE overlay (bottom)
fig, axes = plt.subplots(2, len(WINDOWS), figsize=(18, 7))
for col, w in enumerate(WINDOWS):
    s = df[f"sd_res_{w}"].dropna()
    # timeseries
    ax = axes[0, col]
    ax.plot(s.index, s.values, lw=0.4, color="#2bb09f")
    ax.set_title(f"{w} window")
    ax.grid(alpha=0.3)
    if col == 0: ax.set_ylabel("sd of residual (Pa)")
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=df.index.tz))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=20, ha="right", fontsize=7)

    # histogram + KDE
    ax = axes[1, col]
    bins = np.linspace(0, np.percentile(s.values, 99.5), 60)
    ax.hist(s.values, bins=bins, density=True, alpha=0.5, color="#1d4ed8")
    if len(s) > 100:
        xs = np.linspace(0, bins[-1], 400)
        kde = gaussian_kde(s.values, bw_method=0.15)
        ax.plot(xs, kde(xs), color="#dc2626", lw=1.5, label=f"KDE  BC={bimodality_coefficient(s.values):.3f}")
        ax.legend(fontsize=8)
    ax.set_xlabel("sd of residual (Pa)")
    if col == 0: ax.set_ylabel("density")
    ax.grid(alpha=0.3)

plt.tight_layout()
out = "/home/christopher/projects/oruuvi-mokki/.temp/unsupervised.png"
plt.savefig(out, dpi=120)
print("\nwrote", out)
