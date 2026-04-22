"""Generate presentation-ready plots directly from the local CSV files."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "Data"
OUTPUT_DIR = ROOT_DIR / "plots"


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    plot_solar_radiation()
    plot_energy_combined()
    plot_energy_consumption_focused()
    plot_wind_history()

    print(f"Saved plots to: {OUTPUT_DIR}")


def plot_solar_radiation() -> None:
    df = pd.read_csv(DATA_DIR / "sun_combined.csv")
    df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
    df = df.sort_values("datum")

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(df["datum"], df["open_meteo_radiation"], label="Open-Meteo", linewidth=2)
    ax.plot(df["datum"], df["kmi_radiation_avg"], label="KMI", linewidth=2)
    ax.plot(df["datum"], df["kaggle_radiation_avg"], label="Kaggle", linewidth=2)
    ax.set_title("Solar Radiation Comparison")
    ax.set_xlabel("Date")
    ax.set_ylabel("Radiation")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "01_solar_radiation.png", dpi=180)
    plt.close(fig)


def _load_consumption_df() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "consumptie.csv")
    df["tijd"] = pd.to_datetime(df["tijd"], errors="coerce")
    df = df.sort_values("tijd")

    numeric_cols = [
        "Energie vlaanderen zon",
        "Energie vlaanderen wind",
        "Elia totaal",
        "kaggle prive",
        "kaggle openbaar",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def plot_energy_combined() -> None:
    df = _load_consumption_df()
    prod_mask = df["Energie vlaanderen zon"].notna() | df["Energie vlaanderen wind"].notna()
    df_prod = df.loc[prod_mask].copy()
    if df_prod.empty:
        return

    df_prod["Energie vlaanderen totaal"] = (
        df_prod["Energie vlaanderen zon"].fillna(0) + df_prod["Energie vlaanderen wind"].fillna(0)
    )

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(
        df_prod["tijd"],
        df_prod["Energie vlaanderen totaal"],
        label="Vlaanderen Total (Solar + Wind)",
        linewidth=2.2,
    )
    if df_prod["Elia totaal"].notna().any():
        ax.plot(df_prod["tijd"], df_prod["Elia totaal"], label="ELIA Total", linewidth=2, alpha=0.85)
    ax.set_title("Total Energy Production: Vlaanderen vs ELIA")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Value")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "02_energy_combined.png", dpi=180)
    plt.close(fig)


def plot_energy_consumption_focused() -> None:
    df = _load_consumption_df()
    # Keep only public consumption as requested.
    df["kaggle openbaar"] = df["kaggle openbaar"].mask(df["kaggle openbaar"] <= 0)

    # Hard-focus presentation window requested by user.
    date_mask = (df["tijd"] >= pd.Timestamp("2022-01-01")) & (df["tijd"] <= pd.Timestamp("2022-06-30 23:59:59"))
    df_focus = df.loc[date_mask, ["tijd", "kaggle openbaar"]].copy()
    # Remove trailing null block after the last valid value.
    last_valid_idx = df_focus["kaggle openbaar"].last_valid_index()
    if last_valid_idx is not None:
        df_focus = df_focus.loc[:last_valid_idx]
    df_focus = df_focus.dropna(subset=["kaggle openbaar"])
    if df_focus.empty:
        return

    series_public = df_focus["kaggle openbaar"]

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(df_focus["tijd"], series_public, label="Kaggle Public", linewidth=2)
    ax.set_title("Public Consumption (2022-01 to 2022-06)")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Consumption")

    # Focus on the main data block and suppress scale distortion from outliers.
    series = series_public.dropna()
    if not series.empty:
        low = series.quantile(0.01)
        high = series.quantile(0.99)
        if pd.notna(low) and pd.notna(high) and high > low:
            pad = (high - low) * 0.1
            ax.set_ylim(low - pad, high + pad)

    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "03_energy_consumption_focused.png", dpi=180)
    plt.close(fig)


def plot_wind_history() -> None:
    df = pd.read_csv(DATA_DIR / "v_wind_alles_compleet.csv", na_values=["NULL", "null", ""])
    df["tijdstip"] = pd.to_datetime(df["tijdstip"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.sort_values("tijdstip")

    wind_cols = [
        "wind_ecmwf_2026",
        "wind_kmi_2002",
        "wind_ukkel_2024",
        "wind_antwerpen_archive",
    ]
    for col in wind_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    hist_mask = df["wind_kmi_2002"].notna() | df["wind_ukkel_2024"].notna()
    df_hist = df.loc[hist_mask].copy()
    df_kmi = df_hist[df_hist["tijdstip"] <= pd.Timestamp("2016-12-31 23:59:59")].copy()

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=False)

    ax_kmi = axes[0]
    kmi_df = df_kmi[["tijdstip", "wind_kmi_2002"]].dropna()
    if not kmi_df.empty:
        ax_kmi.plot(kmi_df["tijdstip"], kmi_df["wind_kmi_2002"], label="wind_kmi_2002", linewidth=1.4)
        ax_kmi.legend()
    ax_kmi.set_title("Wind History - Historical KMI (to 2016)")
    ax_kmi.set_xlabel("Timestamp")
    ax_kmi.set_ylabel("Wind")
    ax_kmi.grid(True, alpha=0.3)

    ax_ukkel = axes[1]
    ukkel_df = df_hist[["tijdstip", "wind_ukkel_2024"]].dropna()
    if not ukkel_df.empty:
        ax_ukkel.plot(ukkel_df["tijdstip"], ukkel_df["wind_ukkel_2024"], label="wind_ukkel_2024", linewidth=1.4)
        ax_ukkel.legend()
    ax_ukkel.set_title("Wind History - Historical Ukkel")
    ax_ukkel.set_xlabel("Timestamp")
    ax_ukkel.set_ylabel("Wind")
    ax_ukkel.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "04_wind_history.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    main()
