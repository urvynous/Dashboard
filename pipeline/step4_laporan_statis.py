"""
LANGKAH 4 -- LAPORAN STATIS: grafik PNG untuk laporan/slide.

Menerapkan teknik Pertemuan 9 satu per satu:
  01_overplotting.png      scatter sampel vs heatmap seluruh data
  02_deret_waktu.png       omzet harian + rata-rata bergerak 7 hari + anotasi promo
  03_boxplot_kirim.png     box plot dari kuantil aproksimasi (plt.bxp)
  04_sebelum_sesudah.png   pie 6 irisan vs batang horizontal terurut
  05_dashboard_satu_halaman.png  dashboard ringkas untuk dilampirkan ke laporan

Semua grafik kecuali sampel scatter dibuat dari TABEL GOLD.

Jalankan mandiri:  python pipeline/step4_laporan_statis.py
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LogNorm  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

BIRU, MERAH, ABU, TEKS2 = "#257de0", "#ec4647", "#b8b8b8", "#666666"
plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False,
                     "axes.titleweight": "bold", "axes.titlesize": 11,
                     "axes.labelcolor": "#333333", "font.size": 9})


def rupiah_ringkas(x):
    if x >= 1e12:
        return f"Rp{x / 1e12:,.2f} T"
    return f"Rp{x / 1e9:,.1f} M"


def gold(nama):
    return pd.read_parquet(os.path.join(config.GOLD_DIR, f"{nama}.parquet"))


def _simpan(fig, nama):
    jalur = os.path.join(config.FIG_DIR, nama)
    fig.savefig(jalur, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"[laporan] {jalur}")


def _sumber(fig):
    fig.text(0.01, -0.02, "Sumber: tabel gold pipeline BD-P09 (data sintetis), Okt 2025 - Sep 2026",
             fontsize=7, color=TEKS2)


def jalankan(spark=None):
    os.makedirs(config.FIG_DIR, exist_ok=True)
    cube = gold("gold_cube")
    harian = gold("gold_harian")
    heat = gold("gold_heatmap_kirim_rating")
    qk = gold("gold_kuantil_kirim")

    # ---------------------------------------------------------------- 01
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8))
    if spark is not None:
        from pyspark.sql import functions as F
        sampel = (spark.read.parquet(config.SILVER_DIR)
                  .filter(F.col("rating").isNotNull() & F.col("hari_kirim").isNotNull())
                  .sample(0.01, seed=1).select("hari_kirim", "rating").toPandas())
        ax[0].scatter(sampel.hari_kirim, sampel.rating, s=2, alpha=0.3, color=BIRU)
        ax[0].set_title(f"Sebelum: scatter {len(sampel):,} titik (sampel 1%)")
    ax[0].set_xlabel("Lama kirim (hari)"); ax[0].set_ylabel("Rating")
    h = heat[heat.provinsi == "NASIONAL"].pivot(index="rating", columns="bin_kirim", values="jumlah")
    m = ax[1].pcolormesh(h.columns, h.index, h.values, norm=LogNorm(), cmap="Blues", shading="nearest")
    fig.colorbar(m, ax=ax[1], label="jumlah pesanan (skala log)")
    ax[1].set_title("Sesudah: heatmap seluruh data -- rating turun saat kirim > 4 hari")
    ax[1].set_xlabel("Lama kirim (hari)")
    _sumber(fig); plt.tight_layout(); _simpan(fig, "01_overplotting.png")

    # ---------------------------------------------------------------- 02
    nas = harian[harian.provinsi == "NASIONAL"].sort_values("tanggal").copy()
    nas["tanggal"] = pd.to_datetime(nas["tanggal"])
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(nas.tanggal, nas.omzet / 1e9, color=ABU, lw=0.8, label="Harian")
    ax.plot(nas.tanggal, nas.omzet_ma7 / 1e9, color=BIRU, lw=2, label="Rata-rata bergerak 7 hari")
    for tgl in config.HARI_PROMO_BESAR:
        y = nas.loc[nas.tanggal == tgl, "omzet"].iloc[0] / 1e9
        ax.annotate(pd.Timestamp(tgl).strftime("%d.%m"), (pd.Timestamp(tgl), y), xytext=(8, -4),
                    textcoords="offset points", color=MERAH, fontweight="bold")
    a, b = (pd.Timestamp(d) for d in config.RAMADAN)
    ax.axvspan(a, b, color=BIRU, alpha=0.07)
    ax.text(a + (b - a) / 2, ax.get_ylim()[1] * 0.93, "Ramadan", ha="center", color=TEKS2)
    ax.set_ylabel("Omzet (miliar Rp)")
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, frameon=False)
    ax.grid(axis="y", alpha=0.3)
    ax.set_title("Omzet tumbuh sepanjang tahun, dengan lonjakan tajam pada 11.11 dan 12.12", pad=26)
    _sumber(fig); plt.tight_layout(); _simpan(fig, "02_deret_waktu.png")

    # ---------------------------------------------------------------- 03
    q = qk[qk.provinsi != "NASIONAL"].sort_values("rata2")
    stat = [{"label": r.provinsi, "whislo": r.p05, "q1": r.q1, "med": r.median, "q3": r.q3,
             "whishi": r.p95, "mean": r.rata2, "fliers": []} for r in q.itertuples()]
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    gaya = dict(showmeans=True, patch_artist=True, medianprops=dict(color="#191919"),
                meanprops=dict(marker="D", markerfacecolor="white", markeredgecolor="#191919", markersize=5))
    try:                                    # matplotlib >= 3.10
        bp = ax.bxp(stat, orientation="horizontal", **gaya)
    except TypeError:                       # matplotlib lama
        bp = ax.bxp(stat, vert=False, **gaya)
    for kotak, r in zip(bp["boxes"], q.itertuples()):
        kotak.set_facecolor("#f9c9c9" if r.rata2 > config.AMBANG_KIRIM_LAMBAT else "#cfe2f8")
    ax.axvline(config.AMBANG_KIRIM_LAMBAT, ls="--", color=MERAH, lw=1)
    ax.set_ylim(0.4, len(q) + 1.0)
    ax.text(config.AMBANG_KIRIM_LAMBAT, len(q) + 0.6, " target 3 hari", color=MERAH, fontsize=8)
    ax.set_xlabel("Lama pengiriman (hari) -- kumis = persentil 5 dan 95, belah ketupat = rata-rata")
    ax.set_title("Luar Jawa-Bali menunggu lebih lama DAN lebih tidak pasti")
    _sumber(fig); plt.tight_layout(); _simpan(fig, "03_boxplot_kirim.png")

    # ---------------------------------------------------------------- 04
    per_kat = cube[(cube.gid == 6)].sort_values("omzet")   # gid 6 = bulan & provinsi semua
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8))
    ax[0].pie(per_kat.omzet, labels=per_kat.kategori, autopct="%1.1f%%", startangle=90,
              colors=plt.cm.tab10.colors[:6], textprops={"fontsize": 7})
    ax[0].set_title("Sebelum: pie 6 irisan, sulit dibandingkan")
    warna = [MERAH if k == per_kat.kategori.iloc[-1] else ABU for k in per_kat.kategori]
    ax[1].barh(per_kat.kategori, per_kat.omzet / 1e9, color=warna, height=0.6)
    for i, v in enumerate(per_kat.omzet / 1e9):
        ax[1].text(v, i, f" {v:,.0f}", va="center", fontsize=8)
    ax[1].set_xlabel("Omzet (miliar Rp)")
    ax[1].set_title(f"Sesudah: {per_kat.kategori.iloc[-1]} menyumbang omzet terbesar")
    _sumber(fig); plt.tight_layout(); _simpan(fig, "04_sebelum_sesudah.png")

    # ---------------------------------------------------------------- 05
    total = cube[cube.gid == 7].iloc[0]
    bulanan = cube[cube.gid == 3].sort_values("bulan")            # per bulan saja
    per_prov = cube[cube.gid == 5].copy()                           # per provinsi saja
    per_prov["rata2_kirim"] = per_prov.sum_kirim / per_prov.n_kirim
    per_prov = per_prov.sort_values("omzet")

    fig = plt.figure(figsize=(12.5, 7.6))
    gs = fig.add_gridspec(3, 4, height_ratios=[0.5, 1.2, 1.2])
    kpi = [("Pesanan", f"{int(total.pesanan):,}"),
           ("Omzet bersih", rupiah_ringkas(total.omzet)),
           ("Rata-rata lama kirim", f"{total.sum_kirim / total.n_kirim:.2f} hari"),
           ("Rating rata-rata", f"{total.sum_rating / total.n_rating:.2f} / 5")]
    for i, (judul, nilai) in enumerate(kpi):
        a = fig.add_subplot(gs[0, i]); a.axis("off")
        a.add_patch(plt.Rectangle((0, 0), 1, 1, transform=a.transAxes,
                                  facecolor="#f4f6fb", edgecolor="#d0d7e5"))
        a.text(0.5, 0.58, nilai, ha="center", fontsize=15, weight="bold", transform=a.transAxes)
        a.text(0.5, 0.22, judul, ha="center", fontsize=9, color=TEKS2, transform=a.transAxes)

    a1 = fig.add_subplot(gs[1, :2])
    a1.plot(bulanan.bulan, bulanan.omzet / 1e9, marker="o", color=BIRU, lw=2)
    a1.set_title("Omzet bulanan (miliar Rp): tren naik, puncak saat Ramadan")
    a1.tick_params(axis="x", rotation=45, labelsize=7); a1.grid(axis="y", alpha=0.3)

    a2 = fig.add_subplot(gs[1:, 2:])
    w = [MERAH if v > config.AMBANG_KIRIM_LAMBAT else BIRU for v in per_prov.rata2_kirim]
    a2.barh(per_prov.provinsi, per_prov.omzet / 1e9, color=w, height=0.65)
    a2.set_title("Omzet per provinsi (merah = rata-rata kirim > 3 hari)")
    a2.set_xlabel("miliar Rp")

    a3 = fig.add_subplot(gs[2, :2])
    rk = heat[heat.provinsi == "NASIONAL"].copy()
    rk["hari"] = rk.bin_kirim.round()
    rk = rk.groupby("hari").apply(lambda d: pd.Series({"r": (d.rating * d.jumlah).sum() / d.jumlah.sum(),
                                                         "n": d.jumlah.sum()}), include_groups=False)
    rk = rk[rk.n > 1000].reset_index()
    a3.bar(rk.hari, rk.r, color=BIRU, width=0.7); a3.set_ylim(0, 5)
    a3.set_title("Rating turun seiring lamanya pengiriman")
    a3.set_xlabel("Lama kirim (hari)"); a3.set_ylabel("Rating rata-rata")

    fig.suptitle("Dashboard Penjualan Nasional -- Okt 2025 s.d. Sep 2026", fontsize=13, weight="bold")
    _sumber(fig); plt.tight_layout(); _simpan(fig, "05_dashboard_satu_halaman.png")


if __name__ == "__main__":
    from spark_session import buat_spark
    sp = buat_spark("BD-P09-Laporan")
    jalankan(sp)
    sp.stop()
