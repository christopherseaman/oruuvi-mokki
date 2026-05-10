# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy", "pandas"]
# ///
"""Separability with TIGHT, TEMPORALLY-ADJACENT references.

Anchor on the visible fuzz->smooth flip in the screenshot (~13:50 PDT 05/09).
ON  = 12:30 - 13:30 PDT 05/09  (clearly fuzzy)
OFF = 14:00 - 14:35 PDT 05/09  (clearly smooth)
30-90 minutes apart; weather drift held constant.
"""
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

db = sqlite3.connect("/home/christopher/projects/oruuvi-mokki/data/ruuvi.db")
df = pd.read_sql_query("SELECT epoch_ms, pressure_hpa FROM readings ORDER BY id", db)
df["t"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True).dt.tz_convert("America/Los_Angeles")
df = df.set_index("t")

WINDOWS = ["15s", "30s", "1min", "3min", "5min", "10min"]
for w in WINDOWS:
    df[f"sd_{w}"] = df["pressure_hpa"].rolling(w).std() * 100  # Pa

ON_LO  = pd.Timestamp("2026-05-09 12:30", tz="America/Los_Angeles")
ON_HI  = pd.Timestamp("2026-05-09 13:30", tz="America/Los_Angeles")
OFF_LO = pd.Timestamp("2026-05-09 14:00", tz="America/Los_Angeles")
OFF_HI = pd.Timestamp("2026-05-09 14:35", tz="America/Los_Angeles")

on_mask  = (df.index >= ON_LO)  & (df.index <= ON_HI)
off_mask = (df.index >= OFF_LO) & (df.index <= OFF_HI)
print(f"on samples : {on_mask.sum()}    off samples : {off_mask.sum()}")
print(f"on  range  : {ON_LO}  to  {ON_HI}")
print(f"off range  : {OFF_LO}  to  {OFF_HI}")
print(f"gap between regimes: {(OFF_LO - ON_HI).total_seconds()/60:.0f} min")
print()
print(f"{'window':<8} {'on_mean':>8} {'on_sd':>7} {'off_mean':>9} {'off_sd':>7} {'d_prime':>9} {'overlap':>8}")

results = []
for label in WINDOWS:
    col = f"sd_{label}"
    on  = df.loc[on_mask,  col].dropna().values
    off = df.loc[off_mask, col].dropna().values
    if len(on) < 30 or len(off) < 30:
        continue
    mu_on, sd_on   = on.mean(),  on.std(ddof=1)
    mu_off, sd_off = off.mean(), off.std(ddof=1)
    n_on, n_off = len(on), len(off)
    pooled = np.sqrt(((n_on-1)*sd_on**2 + (n_off-1)*sd_off**2) / (n_on + n_off - 2))
    d_prime = (mu_on - mu_off) / pooled if pooled > 0 else float("nan")

    lo = min(on.min(), off.min()); hi = max(on.max(), off.max())
    bins = np.linspace(lo, hi, 60)
    h_on,  _ = np.histogram(on,  bins=bins, density=True)
    h_off, _ = np.histogram(off, bins=bins, density=True)
    h_on  = h_on  / h_on.sum();  h_off = h_off / h_off.sum()
    overlap = np.sum(np.sqrt(h_on * h_off))

    results.append((label, mu_on, sd_on, mu_off, sd_off, d_prime, overlap, on, off))
    print(f"{label:<8} {mu_on:>8.2f} {sd_on:>7.2f} {mu_off:>9.2f} {sd_off:>7.2f} {d_prime:>9.2f} {overlap:>8.3f}")

fig, axes = plt.subplots(2, 3, figsize=(15, 8))
for ax, (label, mu_on, sd_on, mu_off, sd_off, dp, ov, on, off) in zip(axes.flat, results):
    bins = np.linspace(0, max(on.max(), off.max()), 60)
    ax.hist(off, bins=bins, density=True, alpha=0.55, color="#1d4ed8", label=f"OFF  μ={mu_off:.2f}")
    ax.hist(on,  bins=bins, density=True, alpha=0.55, color="#dc2626", label=f"ON   μ={mu_on:.2f}")
    ax.axvline(mu_off, color="#1d4ed8", lw=1.5, ls="--")
    ax.axvline(mu_on,  color="#dc2626", lw=1.5, ls="--")
    ax.set_title(f"{label} window — d′ = {dp:+.2f}, overlap = {ov:.2f}")
    ax.set_xlabel("rolling stddev (Pa)")
    ax.set_ylabel("density")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)

plt.tight_layout()
out = "/home/christopher/projects/oruuvi-mokki/.temp/separability2.png"
plt.savefig(out, dpi=120)
print("\nwrote", out)
