# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy", "pandas", "scipy"]
# ///
"""Fine-grain analysis of the last 2 hours of Ruuvi data.

Outputs into analysis/2026-05-10_last_2h/:
  01_raw_and_windows.png      raw pressure + rolling stddev at 15s/30s/1m/3m/5m/10m
  02_detrended_30s.png        detrended residual + 30s stddev (max resolution)
  03_distributions.png        residual-stddev distributions per window + BC
  04_sensor_diagnostics.png   accel magnitude, temp/humidity, RSSI
  05_pipeline_health.png      inter-arrival times, dedup ratio, pressure quantization
  06_first_diff.png           pressure first-difference (high-pass) + run-length
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import skew, kurtosis, gaussian_kde

DB = "/home/christopher/projects/oruuvi-mokki/data/ruuvi.db"
TZ = "America/Los_Angeles"
OUT = Path("/home/christopher/projects/oruuvi-mokki/analysis/2026-05-10_last_2h")
OUT.mkdir(exist_ok=True)

db = sqlite3.connect(DB)
# Use latest sample as anchor; last 2 hours from there
max_ms = db.execute("SELECT MAX(epoch_ms) FROM readings").fetchone()[0]
lo_ms  = max_ms - 2 * 3600 * 1000

df = pd.read_sql_query(
    "SELECT * FROM readings WHERE epoch_ms >= ? ORDER BY id",
    db, params=(lo_ms,)
)
df["t"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True).dt.tz_convert(TZ)
df = df.set_index("t")

# fan-on annotation vertical line
ANNO_FAN_ON = pd.Timestamp("2026-05-10 14:32", tz=TZ)

print(f"window: {df.index[0]}  ->  {df.index[-1]}   ({len(df):,} raw rows)")
print(f"unique measurements: {df['measurement_sequence'].nunique():,}")
print(f"average rows per unique measurement: {len(df) / df['measurement_sequence'].nunique():.2f}")

# Pre-compute rolling residual + stddev
df["raw_pa"]    = df["pressure_hpa"] * 100
df["trend_pa"]  = df["raw_pa"].rolling("30min").mean()
df["resid_pa"]  = df["raw_pa"] - df["trend_pa"]
for w in ["15s", "30s", "1min", "3min", "5min", "10min"]:
    df[f"sd_p_{w}"]   = df["pressure_hpa"].rolling(w).std() * 100  # raw
    df[f"sd_res_{w}"] = df["resid_pa"].rolling(w).std()             # detrended

def style_time(ax):
    ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=TZ))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)
    ax.grid(alpha=0.3)
    ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)

# ==================== 01: raw + rolling stddev at all windows ====================
fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True, gridspec_kw={"hspace": 0.1})
axes[0].plot(df.index, df["pressure_hpa"], lw=0.4, color="#2bb09f")
axes[0].set_ylabel("pressure (hPa)")
axes[0].set_title(f"Last 2 h raw pressure ({len(df):,} samples)   |   blue dashed = 14:32 fan-on")
style_time(axes[0])

colors = {"15s": "#dc2626", "30s": "#ea580c", "1min": "#7c3aed",
          "3min": "#1d4ed8", "5min": "#0891b2", "10min": "#d97706"}
for w, c in colors.items():
    axes[1].plot(df.index, df[f"sd_p_{w}"], lw=0.6, color=c, label=w, alpha=0.85)
axes[1].set_ylabel("rolling stddev of raw pressure (Pa)")
axes[1].set_xlabel("local time (Pacific)")
axes[1].legend(loc="upper left", ncol=6, fontsize=8)
style_time(axes[1])

plt.tight_layout()
plt.savefig(OUT / "01_raw_and_windows.png", dpi=140)
plt.close()

# ==================== 02: detrended residual + 30s stddev ====================
fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True, gridspec_kw={"hspace": 0.1})
axes[0].plot(df.index, df["resid_pa"], lw=0.4, color="#2bb09f")
axes[0].axhline(0, color="black", lw=0.5)
axes[0].set_ylabel("residual (Pa)\n[raw − 30-min trailing mean]")
axes[0].set_title("Detrended pressure (max resolution) + 30s stddev of residual")
style_time(axes[0])

axes[1].plot(df.index, df["sd_res_30s"], lw=0.6, color="#dc2626")
axes[1].set_ylabel("30s stddev of residual (Pa)")
axes[1].set_xlabel("local time (Pacific)")
style_time(axes[1])

plt.tight_layout()
plt.savefig(OUT / "02_detrended_30s.png", dpi=140)
plt.close()

# ==================== 03: distributions per window ====================
def bc(x):
    x = np.asarray(x); x = x[~np.isnan(x)]
    if len(x) < 50: return float("nan")
    g1 = skew(x); g2 = kurtosis(x, fisher=True); n = len(x)
    return (g1**2 + 1) / (g2 + 3 * (n-1)**2 / ((n-2)*(n-3)))

WINDOWS = ["15s", "30s", "1min", "3min", "5min", "10min"]
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
for ax, w in zip(axes.flat, WINDOWS):
    s = df[f"sd_res_{w}"].dropna().values
    if len(s) < 30:
        ax.set_title(f"{w} — too few samples")
        continue
    bins = np.linspace(0, np.percentile(s, 99.5), 60)
    ax.hist(s, bins=bins, density=True, alpha=0.55, color="#1d4ed8")
    if len(s) > 100:
        xs = np.linspace(0, bins[-1], 400)
        kde = gaussian_kde(s, bw_method=0.18)
        ax.plot(xs, kde(xs), color="#dc2626", lw=1.5)
    ax.set_title(f"{w} window — μ={s.mean():.2f} σ={s.std():.2f}  BC={bc(s):.3f}")
    ax.set_xlabel("residual stddev (Pa)")
    ax.set_ylabel("density")
    ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(OUT / "03_distributions.png", dpi=140)
plt.close()

# ==================== 04: sensor diagnostics ====================
df["accel_mag_mg"] = np.sqrt(df["acceleration_x_mg"]**2 + df["acceleration_y_mg"]**2 + df["acceleration_z_mg"]**2)
fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True, gridspec_kw={"hspace": 0.12})

axes[0].plot(df.index, df["accel_mag_mg"], lw=0.5, color="#059669")
axes[0].set_ylabel("accel magnitude (mg)")
axes[0].set_title("Accelerometer magnitude (≈1000 mg = gravity-only, no movement)")
style_time(axes[0])

axes[1].plot(df.index, df["temperature_c"], lw=0.6, color="#ea580c", label="temperature_c")
ax_t2 = axes[1].twinx()
ax_t2.plot(df.index, df["humidity_pct"], lw=0.6, color="#1d4ed8", label="humidity_pct")
axes[1].set_ylabel("temperature (°C)", color="#ea580c")
ax_t2.set_ylabel("humidity (%RH)", color="#1d4ed8")
axes[1].set_title("Temperature & humidity")
style_time(axes[1])

axes[2].plot(df.index, df["rssi_dbm"], lw=0.4, color="#7c3aed")
axes[2].set_ylabel("RSSI (dBm)")
axes[2].set_title("RSSI — receive strength")
style_time(axes[2])

axes[3].plot(df.index, df["battery_mv"], lw=0.6, color="#dc2626", label="battery_mv")
ax_b2 = axes[3].twinx()
ax_b2.plot(df.index, df["movement_counter"], lw=0.6, color="#0891b2", label="movement_counter")
axes[3].set_ylabel("battery (mV)", color="#dc2626")
ax_b2.set_ylabel("movement counter", color="#0891b2")
axes[3].set_title("Battery & movement counter")
axes[3].set_xlabel("local time (Pacific)")
style_time(axes[3])

plt.tight_layout()
plt.savefig(OUT / "04_sensor_diagnostics.png", dpi=140)
plt.close()

# ==================== 05: pipeline health ====================
# Inter-arrival times of consecutive rows (raw)
inter_arrival_ms = np.diff(df["epoch_ms"].values)
# Inter-arrival between UNIQUE measurements (deduped on measurement_sequence)
unique = df.drop_duplicates(subset=["measurement_sequence"]).sort_values("epoch_ms")
unique_dt_ms = np.diff(unique["epoch_ms"].values)

fig, axes = plt.subplots(2, 2, figsize=(15, 8))

ax = axes[0, 0]
ax.hist(inter_arrival_ms, bins=np.linspace(0, 3000, 80), color="#1d4ed8", alpha=0.7)
ax.set_title(f"Raw inter-arrival times (n={len(inter_arrival_ms):,})  median={np.median(inter_arrival_ms):.0f} ms")
ax.set_xlabel("ms between consecutive raw rows")
ax.set_ylabel("count")
ax.grid(alpha=0.3)

ax = axes[0, 1]
ax.hist(unique_dt_ms, bins=np.linspace(0, 6000, 80), color="#059669", alpha=0.7)
ax.set_title(f"Unique-measurement spacing (n={len(unique_dt_ms):,})  median={np.median(unique_dt_ms):.0f} ms")
ax.set_xlabel("ms between consecutive unique measurements")
ax.set_ylabel("count")
ax.grid(alpha=0.3)

ax = axes[1, 0]
# Pressure value histogram — shows 1 Pa quantization
ax.hist(df["pressure_hpa"].values, bins=np.linspace(df["pressure_hpa"].min()-0.01, df["pressure_hpa"].max()+0.01, 200),
        color="#dc2626", alpha=0.7)
ax.set_title(f"Raw pressure value histogram — note 0.01 hPa (=1 Pa) quantization")
ax.set_xlabel("pressure (hPa)")
ax.set_ylabel("count")
ax.grid(alpha=0.3)

ax = axes[1, 1]
# Rows-per-unique-measurement ratio over time (5-min bins)
df["bin"] = (df["epoch_ms"] // 300000)
dup = df.groupby("bin").agg(rows=("epoch_ms","size"),
                             uniq=("measurement_sequence", "nunique"),
                             t=("epoch_ms", "min")).reset_index(drop=True)
dup["ratio"] = dup["rows"] / dup["uniq"]
dup["bin_t"] = pd.to_datetime(dup["t"], unit="ms", utc=True).dt.tz_convert(TZ)
ax.plot(dup["bin_t"], dup["ratio"], marker="o", lw=1, color="#7c3aed")
ax.set_title("Retransmission factor per 5-min bin (rows / unique measurements)")
ax.set_xlabel("local time (Pacific)")
ax.set_ylabel("rows per measurement")
ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=TZ))
ax.grid(alpha=0.3)
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)

plt.tight_layout()
plt.savefig(OUT / "05_pipeline_health.png", dpi=140)
plt.close()

# ==================== 06: first-difference + run-length on unique measurements ====================
u = df.drop_duplicates(subset=["measurement_sequence"]).sort_values("epoch_ms").reset_index(drop=False)
u["diff_pa"]   = u["pressure_hpa"].diff() * 100
# Run length = number of consecutive identical pressure values
u["change"]    = (u["pressure_hpa"] != u["pressure_hpa"].shift()).astype(int)
u["run_id"]    = u["change"].cumsum()
run_lengths    = u.groupby("run_id").size().values

fig, axes = plt.subplots(2, 2, figsize=(15, 8))

ax = axes[0, 0]
ax.hist(u["diff_pa"].dropna().values, bins=np.arange(-15, 16, 1),
        color="#1d4ed8", alpha=0.7)
ax.set_title(f"First-difference of unique pressures (Pa)  σ={u['diff_pa'].std():.2f}")
ax.set_xlabel("Δ pressure (Pa) between consecutive unique measurements")
ax.set_ylabel("count")
ax.grid(alpha=0.3)
ax.axvline(0, color="black", lw=0.5)

ax = axes[0, 1]
ax.hist(np.abs(u["diff_pa"].dropna().values), bins=np.arange(0, 16, 1),
        color="#dc2626", alpha=0.7)
ax.set_title(f"|First-difference|  mean={np.abs(u['diff_pa']).mean():.2f} Pa  median={np.median(np.abs(u['diff_pa'].dropna())):.2f}")
ax.set_xlabel("|Δ pressure| (Pa)")
ax.set_ylabel("count")
ax.grid(alpha=0.3)

ax = axes[1, 0]
ax.hist(run_lengths, bins=np.arange(1, run_lengths.max()+2) - 0.5,
        color="#059669", alpha=0.7)
ax.set_title(f"Run length of identical pressure values (n_runs={len(run_lengths):,})  mean={run_lengths.mean():.2f}")
ax.set_xlabel("consecutive unique measurements at same pressure value")
ax.set_ylabel("count")
ax.set_yscale("log")
ax.grid(alpha=0.3)

ax = axes[1, 1]
# RMS of first-difference in rolling 1-min window (alternative airflow metric)
u_idx = u.set_index(pd.to_datetime(u["epoch_ms"], unit="ms", utc=True).dt.tz_convert(TZ))
u_idx["sq"] = u_idx["diff_pa"] ** 2
u_idx["rms_1m"] = np.sqrt(u_idx["sq"].rolling("1min").mean())
u_idx["rms_5m"] = np.sqrt(u_idx["sq"].rolling("5min").mean())
ax.plot(u_idx.index, u_idx["rms_1m"], lw=0.7, color="#dc2626", label="RMS Δ 1-min")
ax.plot(u_idx.index, u_idx["rms_5m"], lw=1.0, color="#1d4ed8", label="RMS Δ 5-min")
ax.set_title("RMS of first-difference (high-pass alternative to stddev)")
ax.set_xlabel("local time (Pacific)")
ax.set_ylabel("RMS |Δ pressure| (Pa)")
ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=TZ))
ax.axvline(ANNO_FAN_ON, color="#2563eb", lw=0.8, ls="--", alpha=0.6)
ax.legend(fontsize=8)
ax.grid(alpha=0.3)
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)

plt.tight_layout()
plt.savefig(OUT / "06_first_diff.png", dpi=140)
plt.close()

print("\nwrote:")
for p in sorted(OUT.glob("*.png")):
    print(f"  {p.relative_to(OUT.parent)}  ({p.stat().st_size//1024} KB)")
