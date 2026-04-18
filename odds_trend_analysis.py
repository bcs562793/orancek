"""
Odds Trend Analysis - Maç öncesi oran değişiminin sonuçlara etkisi
==================================================================
odds_snapshots tablosundaki market oran değişimlerini analiz eder,
sonuç dosyasıyla birleştirerek trend-sonuç ilişkisini raporlar.
"""

import os
import sys
import json
import argparse
import warnings
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.gridspec import GridSpec
import seaborn as sns
from scipy import stats

warnings.filterwarnings("ignore")

# ── Supabase bağlantısı ────────────────────────────────────────────────────────
try:
    from supabase import create_client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

# ── Sabitler ──────────────────────────────────────────────────────────────────
ODDS_COLS = ["ms1", "ms2", "iy1", "iy2", "iyms21", "iyms12", "iyms11", "iyms22"]
LABEL_MAP = {
    "ms1": "MS1 (Ev Sahibi Kazanır)",
    "ms2": "MS2 (Deplasman Kazanır)",
    "iy1": "İY1 (Ev Sahibi İY Kazanır)",
    "iy2": "İY2 (Deplasman İY Kazanır)",
    "iyms21": "İY2/MS1",
    "iyms12": "İY1/MS2",
    "iyms11": "İY1/MS1",
    "iyms22": "İY2/MS2",
}
OUTPUT_DIR = Path("output")


# ══════════════════════════════════════════════════════════════════════════════
# 1. VERİ OKUMA
# ══════════════════════════════════════════════════════════════════════════════

def load_snapshots_from_supabase(url: str, key: str) -> pd.DataFrame:
    """Supabase'den odds_snapshots tablosunu çeker."""
    client = create_client(url, key)
    rows, page_size, start = [], 1000, 0
    while True:
        resp = (
            client.table("odds_snapshots")
            .select("*")
            .range(start, start + page_size - 1)
            .execute()
        )
        chunk = resp.data
        if not chunk:
            break
        rows.extend(chunk)
        start += page_size
        print(f"  Çekildi: {len(rows):,} satır", end="\r")
    print()
    return pd.DataFrame(rows)


def load_snapshots_from_sql(path: str) -> pd.DataFrame:
    """SQL dump dosyasından INSERT satırlarını parse eder."""
    import re
    records = []
    col_pattern = re.compile(
        r"INSERT INTO.*?odds_snapshots.*?VALUES\s*\((.+?)\);", re.IGNORECASE | re.DOTALL
    )
    with open(path, encoding="utf-8", errors="ignore") as f:
        content = f.read()
    for match in col_pattern.finditer(content):
        raw = match.group(1)
        # Basit CSV tokenizer (NULL ve string literal destekli)
        tokens = []
        current, in_str = "", False
        for ch in raw:
            if ch == "'" and not in_str:
                in_str = True
            elif ch == "'" and in_str:
                in_str = False
            elif ch == "," and not in_str:
                tokens.append(current.strip())
                current = ""
                continue
            else:
                current += ch
        tokens.append(current.strip())
        if len(tokens) >= 10:
            records.append(tokens)
    if not records:
        raise ValueError("SQL dump içinde INSERT satırı bulunamadı.")
    # Sütun sırası: id, fixture_id, snapshot_time, markets, markets_change,
    #               nesine_name, match_method, ms1..iyms22, ev_ft_sum, dep_ft_sum
    cols = [
        "id", "fixture_id", "snapshot_time", "markets", "markets_change",
        "nesine_name", "match_method",
        "ms1", "ms2", "iy1", "iy2", "iyms21", "iyms12", "iyms11", "iyms22",
        "ev_ft_sum", "dep_ft_sum",
    ]
    df = pd.DataFrame(records, columns=cols[: len(records[0])])
    return df


def load_results(path: str) -> pd.DataFrame:
    """
    RTF/TSV/CSV sonuç dosyasını okur.
    Beklenen sütunlar: fixture_id, Home, Away, Ft, Ht
    """
    p = Path(path)
    if p.suffix.lower() == ".rtf":
        # extract-text çıktısı tab-separated
        import subprocess
        result = subprocess.run(
            ["extract-text", str(p)], capture_output=True, text=True
        )
        from io import StringIO
        df = pd.read_csv(StringIO(result.stdout), sep="\t")
    elif p.suffix.lower() in (".csv",):
        df = pd.read_csv(path)
    else:
        df = pd.read_csv(path, sep="\t")

    # fixture_id temizle
    df.columns = [c.strip() for c in df.columns]
    df["fixture_id"] = (
        df["fixture_id"].astype(str).str.replace("'", "").str.strip()
    )
    df = df[df["fixture_id"].str.match(r"^\d+$")]
    df["fixture_id"] = df["fixture_id"].astype(int)

    # Skor parse et
    def parse_score(s, idx):
        try:
            parts = str(s).replace(" ", "").split("-")
            return int(parts[idx])
        except Exception:
            return np.nan

    for raw_col, new_col, idx in [
        ("Ft", "ft_home", 0), ("Ft", "ft_away", 1),
        ("Ht", "ht_home", 0), ("Ht", "ht_away", 1),
    ]:
        if raw_col in df.columns:
            df[new_col] = df[raw_col].apply(lambda x: parse_score(x, idx))

    # MS sonucu: 1=Ev, X=Beraberlik, 2=Dep
    def ft_result(row):
        try:
            h, a = int(row["ft_home"]), int(row["ft_away"])
            return "1" if h > a else ("X" if h == a else "2")
        except Exception:
            return np.nan

    def ht_result(row):
        try:
            h, a = int(row["ht_home"]), int(row["ht_away"])
            return "1" if h > a else ("X" if h == a else "2")
        except Exception:
            return np.nan

    df["ft_result"] = df.apply(ft_result, axis=1)
    df["ht_result"] = df.apply(ht_result, axis=1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. SNAPSHOT'LARI HAZIRLAMA
# ══════════════════════════════════════════════════════════════════════════════

def prepare_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fixture_id"] = pd.to_numeric(df["fixture_id"], errors="coerce")
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["fixture_id", "snapshot_time"])
    df["fixture_id"] = df["fixture_id"].astype(int)
    for col in ODDS_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["fixture_id", "snapshot_time"])


# ══════════════════════════════════════════════════════════════════════════════
# 3. TREND METRİKLERİ HESAPLAMA
# ══════════════════════════════════════════════════════════════════════════════

def compute_trend_metrics(snapshots: pd.DataFrame) -> pd.DataFrame:
    """
    Her fixture için her oran kolonu üzerinde:
      - first / last değer
      - mutlak ve yüzde değişim (Δ)
      - lineer regresyon eğimi (slope) — snapshot sayısı ≥ 3 gerekir
      - volatilite (std)
      - snapshot sayısı
    """
    rows = []
    for fid, grp in snapshots.groupby("fixture_id"):
        grp = grp.sort_values("snapshot_time")
        n = len(grp)
        record = {"fixture_id": fid, "n_snapshots": n}
        for col in ODDS_COLS:
            if col not in grp.columns:
                continue
            series = grp[col].dropna()
            if len(series) < 2:
                record.update({
                    f"{col}_first": np.nan, f"{col}_last": np.nan,
                    f"{col}_delta": np.nan, f"{col}_delta_pct": np.nan,
                    f"{col}_slope": np.nan, f"{col}_vol": np.nan,
                })
                continue
            first, last = series.iloc[0], series.iloc[-1]
            delta = last - first
            delta_pct = (delta / first * 100) if first else np.nan
            vol = series.std()
            # Slope: regresyon sıra indeksi üzerinden
            x = np.arange(len(series))
            slope = np.polyfit(x, series.values, 1)[0] if len(series) >= 3 else np.nan
            record.update({
                f"{col}_first": first, f"{col}_last": last,
                f"{col}_delta": delta, f"{col}_delta_pct": delta_pct,
                f"{col}_slope": slope, f"{col}_vol": vol,
            })
        rows.append(record)
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# 4. BİRLEŞTİRME
# ══════════════════════════════════════════════════════════════════════════════

def merge_data(trends: pd.DataFrame, results: pd.DataFrame) -> pd.DataFrame:
    merged = trends.merge(
        results[["fixture_id", "Home", "Away",
                 "ft_home", "ft_away", "ht_home", "ht_away",
                 "ft_result", "ht_result"]],
        on="fixture_id", how="inner",
    )
    print(f"  Eşleşen maç sayısı: {len(merged):,}")
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# 5. ANALİZ FONKSİYONLARI
# ══════════════════════════════════════════════════════════════════════════════

def trend_label(delta_pct: float, threshold: float = 3.0) -> str:
    if pd.isna(delta_pct):
        return "Veri Yok"
    if delta_pct < -threshold:
        return "Düşüş (▼)"
    if delta_pct > threshold:
        return "Yükseliş (▲)"
    return "Sabit (→)"


def add_trend_labels(df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
    df = df.copy()
    for col in ODDS_COLS:
        pct_col = f"{col}_delta_pct"
        if pct_col in df.columns:
            df[f"{col}_trend"] = df[pct_col].apply(
                lambda x: trend_label(x, threshold)
            )
    return df


def result_distribution_by_trend(
    df: pd.DataFrame, odds_col: str, result_col: str = "ft_result"
) -> pd.DataFrame:
    trend_col = f"{odds_col}_trend"
    if trend_col not in df.columns or result_col not in df.columns:
        return pd.DataFrame()
    ct = pd.crosstab(df[trend_col], df[result_col], normalize="index") * 100
    ct.index.name = f"{odds_col} Trendi"
    ct.columns.name = "Maç Sonucu (%)"
    return ct.round(1)


def mean_odds_by_result(df: pd.DataFrame, odds_col: str, result_col: str = "ft_result") -> pd.DataFrame:
    last_col = f"{odds_col}_last"
    delta_col = f"{odds_col}_delta_pct"
    if last_col not in df.columns:
        return pd.DataFrame()
    return (
        df.groupby(result_col)[[last_col, delta_col]]
        .agg(["mean", "median", "count"])
        .round(3)
    )


def significance_test(df: pd.DataFrame, odds_col: str, result_col: str = "ft_result"):
    """Kruskal-Wallis testi: oran değişimi gruplar arasında farklı mı?"""
    delta_col = f"{odds_col}_delta_pct"
    if delta_col not in df.columns:
        return None
    groups = [
        grp[delta_col].dropna().values
        for _, grp in df.groupby(result_col)
        if delta_col in grp.columns
    ]
    groups = [g for g in groups if len(g) >= 5]
    if len(groups) < 2:
        return None
    stat, p = stats.kruskal(*groups)
    return {"statistic": round(stat, 3), "p_value": round(p, 4), "col": odds_col}


# ══════════════════════════════════════════════════════════════════════════════
# 6. GÖRSELLEŞTİRME
# ══════════════════════════════════════════════════════════════════════════════

RESULT_COLORS = {"1": "#2196F3", "X": "#9C27B0", "2": "#F44336"}
TREND_COLORS  = {"Düşüş (▼)": "#F44336", "Sabit (→)": "#9E9E9E", "Yükseliş (▲)": "#4CAF50", "Veri Yok": "#EEEEEE"}


def plot_trend_distribution(df: pd.DataFrame, out_dir: Path):
    fig, axes = plt.subplots(2, 4, figsize=(20, 9))
    fig.suptitle("Oran Trendi Dağılımı (Maç Başlamadan Önce)", fontsize=15, fontweight="bold")
    for ax, col in zip(axes.flatten(), ODDS_COLS):
        trend_col = f"{col}_trend"
        if trend_col not in df.columns:
            ax.set_visible(False)
            continue
        counts = df[trend_col].value_counts()
        colors = [TREND_COLORS.get(k, "#888") for k in counts.index]
        counts.plot.bar(ax=ax, color=colors, edgecolor="white")
        ax.set_title(LABEL_MAP.get(col, col), fontsize=9, fontweight="bold")
        ax.set_xlabel("")
        ax.set_ylabel("Maç Sayısı")
        ax.tick_params(axis="x", rotation=20, labelsize=8)
        for p in ax.patches:
            ax.annotate(f"{int(p.get_height())}",
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha="center", va="bottom", fontsize=7)
    plt.tight_layout()
    fig.savefig(out_dir / "01_trend_distribution.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  ✓ 01_trend_distribution.png")


def plot_result_by_trend(df: pd.DataFrame, odds_col: str, result_col: str, out_dir: Path, fname_prefix: str = ""):
    ct = result_distribution_by_trend(df, odds_col, result_col)
    if ct.empty:
        return
    present_results = [r for r in ["1", "X", "2"] if r in ct.columns]
    ct = ct[present_results]
    colors = [RESULT_COLORS.get(r, "#888") for r in present_results]
    fig, ax = plt.subplots(figsize=(9, 5))
    ct.plot(kind="bar", ax=ax, color=colors, edgecolor="white", width=0.7)
    ax.set_title(
        f"{LABEL_MAP.get(odds_col, odds_col)} Trendi → {result_col.upper()} Sonuç Dağılımı",
        fontsize=11, fontweight="bold"
    )
    ax.set_ylabel("Yüzde (%)")
    ax.set_xlabel("")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.legend(title="Sonuç", labels=present_results)
    ax.tick_params(axis="x", rotation=20, labelsize=9)
    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f%%", label_type="edge", fontsize=7, padding=2)
    plt.tight_layout()
    safe_col = odds_col.replace("/", "_")
    fig.savefig(out_dir / f"{fname_prefix}{safe_col}_{result_col}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_delta_violin(df: pd.DataFrame, out_dir: Path):
    """Oran % değişiminin sonuç gruplarına göre violin plot."""
    fig, axes = plt.subplots(2, 4, figsize=(20, 9))
    fig.suptitle("Oran % Değişimi Dağılımı — MS Sonucuna Göre", fontsize=14, fontweight="bold")
    for ax, col in zip(axes.flatten(), ODDS_COLS):
        delta_col = f"{col}_delta_pct"
        if delta_col not in df.columns:
            ax.set_visible(False)
            continue
        plot_df = df[["ft_result", delta_col]].dropna()
        plot_df = plot_df[plot_df["ft_result"].isin(["1", "X", "2"])]
        if plot_df.empty:
            ax.set_visible(False)
            continue
        order = ["1", "X", "2"]
        palette = {k: RESULT_COLORS[k] for k in order}
        sns.violinplot(data=plot_df, x="ft_result", y=delta_col,
                       order=order, palette=palette, ax=ax, cut=0, inner="quartile")
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_title(LABEL_MAP.get(col, col), fontsize=9, fontweight="bold")
        ax.set_xlabel("MS Sonucu")
        ax.set_ylabel("% Değişim")
    plt.tight_layout()
    fig.savefig(out_dir / "02_delta_violin.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  ✓ 02_delta_violin.png")


def plot_slope_heatmap(df: pd.DataFrame, out_dir: Path):
    """Oran eğimleri korelasyon ısı haritası."""
    slope_cols = [f"{c}_slope" for c in ODDS_COLS if f"{c}_slope" in df.columns]
    if not slope_cols:
        return
    corr = df[slope_cols].corr()
    corr.index = [LABEL_MAP.get(c.replace("_slope", ""), c) for c in slope_cols]
    corr.columns = corr.index
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", center=0, ax=ax,
                linewidths=0.5, annot_kws={"size": 8})
    ax.set_title("Oran Eğimleri Arası Korelasyon", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(out_dir / "03_slope_correlation.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  ✓ 03_slope_correlation.png")


def plot_odds_movement_sample(snapshots: pd.DataFrame, results: pd.DataFrame,
                               out_dir: Path, n_samples: int = 12):
    """Örnek maçlar için oran hareket grafikleri."""
    common = set(snapshots["fixture_id"].unique()) & set(results["fixture_id"].unique())
    sample_ids = list(common)[:n_samples]
    if not sample_ids:
        return
    fig, axes = plt.subplots(3, 4, figsize=(20, 12))
    fig.suptitle("Örnek Maçlarda Oran Hareketi (ms1 / ms2)", fontsize=14, fontweight="bold")
    for ax, fid in zip(axes.flatten(), sample_ids):
        grp = snapshots[snapshots["fixture_id"] == fid].sort_values("snapshot_time")
        res_row = results[results["fixture_id"] == fid].iloc[0]
        label = f"{res_row.get('Home','?')} vs {res_row.get('Away','?')}\nMS:{res_row.get('ft_result','?')}"
        if "ms1" in grp.columns:
            ax.plot(range(len(grp)), grp["ms1"].values, label="MS1", color=RESULT_COLORS["1"], linewidth=1.5)
        if "ms2" in grp.columns:
            ax.plot(range(len(grp)), grp["ms2"].values, label="MS2", color=RESULT_COLORS["2"], linewidth=1.5)
        ax.set_title(label, fontsize=8)
        ax.set_xlabel("Snapshot Sırası")
        ax.set_ylabel("Oran")
        ax.legend(fontsize=7)
        ax.grid(alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_dir / "04_sample_movements.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  ✓ 04_sample_movements.png")


# ══════════════════════════════════════════════════════════════════════════════
# 7. RAPOR OLUŞTURMA
# ══════════════════════════════════════════════════════════════════════════════

def generate_report(df: pd.DataFrame, sig_tests: list, out_dir: Path):
    lines = [
        "# Odds Trend Analiz Raporu",
        "",
        f"**Eşleşen Maç Sayısı:** {len(df):,}",
        "",
        "---",
        "",
        "## 1. İstatistiksel Anlamlılık (Kruskal-Wallis)",
        "",
        "| Market | H İstatistiği | p-değeri | Anlamlı (α=0.05) |",
        "|--------|--------------|---------|-----------------|",
    ]
    for t in sig_tests:
        if t is None:
            continue
        sig = "✅ Evet" if t["p_value"] < 0.05 else "❌ Hayır"
        lines.append(f"| {LABEL_MAP.get(t['col'], t['col'])} | {t['statistic']} | {t['p_value']} | {sig} |")

    lines += ["", "---", "", "## 2. Trend → MS Sonuç Tabloları", ""]
    for col in ODDS_COLS:
        ct = result_distribution_by_trend(df, col, "ft_result")
        if ct.empty:
            continue
        lines.append(f"### {LABEL_MAP.get(col, col)}")
        lines.append("")
        lines.append(ct.to_markdown())
        lines.append("")

    lines += ["", "---", "", "## 3. Trend → İY Sonuç Tabloları", ""]
    for col in ["ms1", "ms2", "iy1", "iy2"]:
        ct = result_distribution_by_trend(df, col, "ht_result")
        if ct.empty:
            continue
        lines.append(f"### {LABEL_MAP.get(col, col)}")
        lines.append("")
        lines.append(ct.to_markdown())
        lines.append("")

    lines += [
        "", "---", "",
        "## 4. Ortalama Oran Değişimi (MS Sonucuna Göre)", "",
    ]
    for col in ["ms1", "ms2"]:
        tbl = mean_odds_by_result(df, col, "ft_result")
        if tbl.empty:
            continue
        lines.append(f"### {LABEL_MAP.get(col, col)}")
        lines.append("")
        lines.append(tbl.to_markdown())
        lines.append("")

    report_path = out_dir / "REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  ✓ REPORT.md ({report_path})")


# ══════════════════════════════════════════════════════════════════════════════
# 8. ANA AKIŞ
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Odds Trend Analysis")
    parser.add_argument("--results", required=True, help="Sonuç dosyası (RTF/CSV/TSV)")
    parser.add_argument("--sql", default=None, help="SQL dump dosyası (.sql)")
    parser.add_argument("--supabase-url", default=None, help="Supabase URL (env: SUPABASE_URL)")
    parser.add_argument("--supabase-key", default=None, help="Supabase anon key (env: SUPABASE_KEY)")
    parser.add_argument("--threshold", type=float, default=3.0,
                        help="Trend eşiği: oran % değişimi bu değeri aşarsa trend sayılır (default: 3)")
    parser.add_argument("--out", default="output", help="Çıktı dizini (default: output)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Snapshot verisi yükle ──────────────────────────────────────────────
    supa_url = args.supabase_url or os.getenv("SUPABASE_URL")
    supa_key = args.supabase_key or os.getenv("SUPABASE_KEY")

    if supa_url and supa_key and SUPABASE_AVAILABLE:
        print("📡 Supabase'den veri çekiliyor...")
        snapshots_raw = load_snapshots_from_supabase(supa_url, supa_key)
    elif args.sql:
        print(f"📂 SQL dump okunuyor: {args.sql}")
        snapshots_raw = load_snapshots_from_sql(args.sql)
    else:
        parser.error(
            "Snapshot kaynağı gerekli: --sql <dosya> veya "
            "--supabase-url + --supabase-key (ya da env değişkenleri SUPABASE_URL, SUPABASE_KEY)"
        )
        return

    print(f"  Snapshot satır sayısı: {len(snapshots_raw):,}")

    # ── Sonuç dosyası yükle ───────────────────────────────────────────────
    print(f"📂 Sonuç dosyası okunuyor: {args.results}")
    results = load_results(args.results)
    print(f"  Maç sonucu sayısı: {len(results):,}")

    # ── Hazırlık ve trend hesaplama ───────────────────────────────────────
    print("⚙️  Snapshot'lar hazırlanıyor...")
    snapshots = prepare_snapshots(snapshots_raw)

    print("📊 Trend metrikleri hesaplanıyor...")
    trends = compute_trend_metrics(snapshots)

    print("🔗 Veriler birleştiriliyor...")
    merged = merge_data(trends, results)
    merged = add_trend_labels(merged, threshold=args.threshold)

    # Ham birleşik veri kaydet
    merged.to_csv(out_dir / "merged_data.csv", index=False)
    print(f"  ✓ merged_data.csv ({len(merged):,} satır)")

    # ── İstatistiksel testler ─────────────────────────────────────────────
    print("🧪 Anlamlılık testleri yapılıyor...")
    sig_tests = [significance_test(merged, col) for col in ODDS_COLS]

    # ── Grafikler ─────────────────────────────────────────────────────────
    print("🎨 Grafikler oluşturuluyor...")
    plot_trend_distribution(merged, out_dir)
    plot_delta_violin(merged, out_dir)
    plot_slope_heatmap(merged, out_dir)
    plot_odds_movement_sample(snapshots, results, out_dir)

    for col in ODDS_COLS:
        plot_result_by_trend(merged, col, "ft_result", out_dir, "05_ms_")
        plot_result_by_trend(merged, col, "ht_result", out_dir, "06_iy_")

    # ── Rapor ─────────────────────────────────────────────────────────────
    print("📝 Rapor yazılıyor...")
    generate_report(merged, sig_tests, out_dir)

    print("\n✅ Analiz tamamlandı!")
    print(f"   Çıktılar: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
