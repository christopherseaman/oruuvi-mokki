# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy", "pandas"]
# ///
"""Two additional last-2h plots:
  07_pressure_app_style.png   pressure with area-fill + min/max band per pixel, mimics Ruuvi Station
  08_temp_humid_detector.png  temp & humidity as the airflow detector signal
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DB = "/home/christopher/projects/oruuvi-mokki/data/ruuvi.db"
TZ = "America/Los_Angeles"
OUT = Path("/home/christopher/projects/oruuvi-mokki/analysis/2026-05-10_last_2h")
OUT.mkdir(exist_ok=True)
ANNO_FAN_ON = pd.Timestamp("2026-05-10 14:32", tz=TZ)

db = sqlite3.connect(DB)
max_ms = db.execute("SELECT MAX(epoch_ms) FROM readings").fetchone()[0]
lo_ms  = max_ms - 2 * 3600 * 1000
df = pd.read_sql_query(
    "SELECT epoch_ms, pressure_hpa, temperature_c, humidity_pct, measurement_sequence FROM readings WHERE epoch_ms >= ? ORDER BY id",
    db, params=(lo_ms,)
)
df["t"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True).dt.tz_convert(TZ)
df = df.set_index("t")

# ==================== 07: Ruuvi-app-style pressure ====================
# Bin by ~6s (matches what the app appears to do at this window width)
# Render min/max as a filled band + median as a line, with area fill from y_min.
bin_seconds = 6
df["binkey"] = (df["epoch_ms"] // (bin_seconds * 1000)) * bin_seconds * 1000
agg = df.groupby("binkey").agg(
    p_min=("pressure_hpa", "min"),
    p_max=("pressure_hpa", "max"),
    p_med=("pressure_hpa", "median"),
    n=("pressure_hpa", "size"),
).reset_index()
agg["t"] = pd.to_datetime(agg["binkey"], unit="ms", utc=True).dt.tz_convert(TZ)

y_floor = float(np.floor(df["pressure_hpa"].min() * 10) / 10) - 0.2
y_ceil  = float(np.ceil(df["pressure_hpa"].max()  * 10) / 10) + 0.1

fig, ax = plt.subplots(1, 1, figsize=(16, 5))
ax.fill_between(agg["t"], agg["p_min"], agg["p_max"],
                color="#2bb09f", alpha=0.55, lw=0, label="min↔max per 6s")
ax.plot(agg["t"], agg["p_med"], color="#0f766e", lw=1.0, label="median per 6s")
ax.fill_between(agg["t"], y_floor, agg["p_med"], color="#2bb09f", alpha=0.15, lw=0)
ax.set_ylim(y_floor, y_ceil)
ax.set_ylabel("pressure (hPa)")
ax.set_xlabel("local time (Pacific)")
ax.set_title(f"Ruuvi-Station-style pressure rendering — min/max band over 6s bins, fill toward y-axis floor\n"
             f"Min: {df['pressure_hpa'].min():.2f}  Max: {df['pressure_hpa'].max():.2f}  "
             f"Avg: {df['pressure_hpa'].mean():.2f}  Latest: {df['pressure_hpa'].iloc[-1]:.2f}")
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.7, label="14:32 fan-on")
ax.legend(loc="upper right", fontsize=8)
ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45]))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=TZ))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)
ax.grid(alpha=0.3)

plt.tight_layout()
plt.savefig(OUT / "07_pressure_app_style.png", dpi=140)
plt.close()

# ==================== 08: temp/humidity as airflow detector ====================
df["dT"]  = df["temperature_c"].diff()
df["dH"]  = df["humidity_pct"].diff()
df["T_smooth_3m"] = df["temperature_c"].rolling("3min").mean()
df["H_smooth_3m"] = df["humidity_pct"].rolling("3min").mean()
df["dTdt_3min"] = df["T_smooth_3m"].diff().rolling("1min").mean() * 60  # °C/min
df["dHdt_3min"] = df["H_smooth_3m"].diff().rolling("1min").mean() * 60  # %RH/min

fig, axes = plt.subplots(3, 1, figsize=(16, 10), sharex=True, gridspec_kw={"hspace": 0.12})

ax = axes[0]
ax.plot(df.index, df["temperature_c"], lw=0.4, color="#ea580c", alpha=0.5, label="raw temperature")
ax.plot(df.index, df["T_smooth_3m"],   lw=1.5, color="#7c2d12", label="3-min smoothed")
ax.set_ylabel("temperature (°C)")
ax.set_title("Temperature — visible step UP at 14:32 (fan blows warmer-than-ambient air on sensor)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(alpha=0.3)
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)

ax = axes[1]
ax.plot(df.index, df["humidity_pct"], lw=0.4, color="#1d4ed8", alpha=0.5, label="raw humidity")
ax.plot(df.index, df["H_smooth_3m"],  lw=1.5, color="#1e3a8a", label="3-min smoothed")
ax.set_ylabel("humidity (%RH)")
ax.set_title("Humidity — inverse response to temperature (RH ↓ as T ↑, then rises as moisture catches up)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(alpha=0.3)
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)

ax = axes[2]
ax.plot(df.index, df["dTdt_3min"], lw=1.0, color="#ea580c", label="dT/dt  (°C / min)")
ax_h = ax.twinx()
ax_h.plot(df.index, df["dHdt_3min"], lw=1.0, color="#1d4ed8", label="dH/dt  (%RH / min)")
ax.axhline(0, color="black", lw=0.5)
ax.set_ylabel("dT/dt (°C/min)", color="#ea580c")
ax_h.set_ylabel("dH/dt (%RH/min)", color="#1d4ed8")
ax.set_xlabel("local time (Pacific)")
ax.set_title("Rate-of-change of smoothed temp & humidity — the airflow detector candidate")
ax.grid(alpha=0.3)
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)

for ax_ in axes:
    ax_.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45]))
    ax_.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=TZ))
    plt.setp(ax_.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)

plt.tight_layout()
plt.savefig(OUT / "08_temp_humid_detector.png", dpi=140)
plt.close()

print("wrote:")
for p in sorted(OUT.glob("0[78]_*.png")):
    print(f"  {p.relative_to(OUT.parent)}  ({p.stat().st_size//1024} KB)")
