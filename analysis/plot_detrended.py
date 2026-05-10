# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy", "pandas"]
# ///
"""Two-panel: detrended residual + 30s rolling stddev, max resolution.

Window: 04:00 - 16:00 PDT 2026-05-09 (12 h covering the user's fan annotations).
Detrending: trailing 30-min rolling mean.
"""
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DB = "/home/christopher/projects/oruuvi-mokki/data/ruuvi.db"
TZ = "America/Los_Angeles"
WIN_LO = pd.Timestamp("2026-05-09 04:00", tz=TZ)
WIN_HI = pd.Timestamp("2026-05-09 16:00", tz=TZ)

db = sqlite3.connect(DB)
df = pd.read_sql_query("SELECT epoch_ms, pressure_hpa FROM readings ORDER BY id", db)
df["t"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True).dt.tz_convert(TZ)
df = df.set_index("t")

# Detrend with a TRAILING 30-min rolling mean — slow enough to keep weather,
# fast enough that any sustained fan-induced offset doesn't fully leak in.
# Compute on full dataset so the trend has 30min of pre-context at WIN_LO.
df["trend_pa"]   = df["pressure_hpa"].rolling("30min").mean() * 100
df["raw_pa"]     = df["pressure_hpa"] * 100
df["resid_pa"]   = df["raw_pa"] - df["trend_pa"]
df["sd_30s_pa"]  = df["resid_pa"].rolling("30s").std()

slc = df.loc[WIN_LO:WIN_HI]
print(f"window: {WIN_LO} to {WIN_HI}")
print(f"rows in window: {len(slc):,}")
print(f"resid: min={slc['resid_pa'].min():.2f}  max={slc['resid_pa'].max():.2f}  mean={slc['resid_pa'].mean():.3f}")
print(f"sd30s: min={slc['sd_30s_pa'].min():.2f}  max={slc['sd_30s_pa'].max():.2f}  mean={slc['sd_30s_pa'].mean():.2f}")

fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True,
                         gridspec_kw={"hspace": 0.08})

# Top: residual at max resolution (every sample, no decimation)
ax = axes[0]
ax.plot(slc.index, slc["resid_pa"], lw=0.4, color="#2bb09f")
ax.axhline(0, color="black", lw=0.5)
ax.set_ylabel("pressure residual (Pa)\n[raw − 30-min trailing mean]")
ax.set_title(f"Detrended pressure & 30s rolling-stddev of residual  |  {WIN_LO:%Y-%m-%d %H:%M} – {WIN_HI:%H:%M %Z}  |  {len(slc):,} samples")
ax.grid(alpha=0.3)

# Bottom: 30s stddev of the residual
ax = axes[1]
ax.plot(slc.index, slc["sd_30s_pa"], lw=0.5, color="#dc2626")
ax.set_ylabel("30s stddev of residual (Pa)")
ax.set_xlabel("local time (Pacific)")
ax.grid(alpha=0.3)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=slc.index.tz))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=9)

# Mark the user's annotation timestamps for reference
for ax in axes:
    ax.axvline(pd.Timestamp("2026-05-09 04:15", tz=TZ), color="#2563eb", lw=0.8, ls="--", alpha=0.6)
    ax.axvline(pd.Timestamp("2026-05-09 13:15", tz=TZ), color="#2563eb", lw=0.8, ls="--", alpha=0.6)
axes[0].text(pd.Timestamp("2026-05-09 04:15", tz=TZ), axes[0].get_ylim()[1]*0.9,
             "  04:15 anno: fan on", color="#2563eb", fontsize=8)
axes[0].text(pd.Timestamp("2026-05-09 13:15", tz=TZ), axes[0].get_ylim()[1]*0.9,
             "  13:15 anno: fan off", color="#2563eb", fontsize=8)

plt.tight_layout()
out = "/home/christopher/projects/oruuvi-mokki/.temp/detrended_30s_share.png"
plt.savefig(out, dpi=140)
print("wrote", out)
