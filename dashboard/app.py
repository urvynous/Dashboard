"""
DASHBOARD PENJUALAN NASIONAL -- Big Data Visualization (BD P09)

Dashboard ini HANYA membaca tabel gold hasil pipeline Spark (ribuan baris),
bukan jutaan baris data mentah. Itulah sebabnya setiap klik filter terasa instan.

Jalankan:  streamlit run dashboard/app.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

# ------------------------------------------------------------------ gaya
BIRU, MERAH, ABU, HIJAU = "#257de0", "#ec4647", "#b8b8b8", "#008a17"
SEMUA = "(Semua)"
NAMA_HARI = {1: "Sen", 2: "Sel", 3: "Rab", 4: "Kam", 5: "Jum", 6: "Sab", 7: "Min"}
TEMA = dict(template="plotly_white", margin=dict(l=10, r=10, t=50, b=10),
            font=dict(family="Inter, Segoe UI, sans-serif", size=12),
            hoverlabel=dict(bgcolor="white"))

st.set_page_config(page_title="Dashboard Penjualan Nasional", page_icon="📊", layout="wide")
st.markdown("""
<style>
div[data-testid="stMetric"] {background:#f6f8fc;border:1px solid #e1e6f0;border-radius:10px;padding:10px 14px;}
div[data-testid="stMetricLabel"] p {font-size:0.85rem;color:#555;}
div[data-testid="stMetricValue"] {font-size:1.55rem;}
.block-container {padding-top:1.6rem;}
</style>""", unsafe_allow_html=True)


# ------------------------------------------------------------------ data
def _mtime():
    jalur = os.path.join(config.GOLD_DIR, "gold_metadata.json")
    return os.path.getmtime(jalur) if os.path.exists(jalur) else 0


@st.cache_data(show_spinner="Membaca tabel gold ...")
def muat_gold(_versi):
    g = {}
    for berkas in os.listdir(config.GOLD_DIR):
        if berkas.endswith(".parquet"):
            g[berkas.replace(".parquet", "")] = pd.read_parquet(os.path.join(config.GOLD_DIR, berkas))
    with open(os.path.join(config.GOLD_DIR, "gold_metadata.json")) as f:
        g["meta"] = json.load(f)
    g["gold_harian"]["tanggal"] = pd.to_datetime(g["gold_harian"]["tanggal"])
    return g


if not os.path.exists(os.path.join(config.GOLD_DIR, "gold_cube.parquet")):
    st.error("Tabel gold belum ada. Jalankan pipeline Spark terlebih dahulu:\n\n"
             "`python pipeline/run_pipeline.py --rows 1000000`")
    st.stop()

G = muat_gold(_mtime())
cube, meta = G["gold_cube"], G["meta"]
BULAN = sorted(cube["bulan"].dropna().unique())
PROVINSI = sorted(cube["provinsi"].dropna().unique())
KATEGORI = sorted(cube["kategori"].dropna().unique())


def rasio(df):
    """Menghitung metrik turunan dari kolom ADITIF (setelah dijumlahkan)."""
    df = df.copy()
    df["aov"] = df["omzet"] / df["pesanan_selesai"].replace(0, np.nan)
    df["rata2_kirim"] = df["sum_kirim"] / df["n_kirim"].replace(0, np.nan)
    df["rata2_rating"] = df["sum_rating"] / df["n_rating"].replace(0, np.nan)
    df["tingkat_batal"] = df["pesanan_batal"] / df["pesanan"]
    df["tingkat_retur"] = df["pesanan_retur"] / df["pesanan"]
    df["pct_kirim_lambat"] = df["n_kirim_lambat"] / df["n_kirim"].replace(0, np.nan)
    return df


ADITIF = ["pesanan", "pesanan_selesai", "pesanan_batal", "pesanan_retur", "omzet", "item_terjual",
          "total_diskon", "total_ongkir", "sum_kirim", "n_kirim", "n_kirim_lambat",
          "sum_rating", "n_rating", "n_rating_rendah"]


def pilih_cube(prov, kat, bulan_list, per=None):
    """
    Mengambil baris cube yang tepat lewat grouping_id (gid):
    bit 4 = bulan diagregasi, bit 2 = provinsi diagregasi, bit 1 = kategori diagregasi.
    `per` = dimensi yang dipecah (mis. 'provinsi'); dimensi lain mengikuti filter.
    """
    prov_semua = (prov == SEMUA) and per != "provinsi"
    kat_semua = (kat == SEMUA) and per != "kategori"
    gid = 2 * prov_semua + 1 * kat_semua        # bulan selalu dipecah (bit 4 = 0)
    d = cube[(cube.gid == gid) & cube.bulan.isin(bulan_list)]
    if prov != SEMUA:
        d = d[d.provinsi == prov]
    if kat != SEMUA:
        d = d[d.kategori == kat]
    return d


def ringkas(d, by=None):
    if by is None:
        return rasio(d[ADITIF].sum().to_frame().T).iloc[0]
    return rasio(d.groupby(by, as_index=False)[ADITIF].sum())


def rp(x):
    if pd.isna(x):
        return "-"
    if abs(x) >= 1e12:
        return f"Rp{x / 1e12:,.2f} T"
    if abs(x) >= 1e9:
        return f"Rp{x / 1e9:,.1f} M"
    if abs(x) >= 1e6:
        return f"Rp{x / 1e6:,.1f} jt"
    return f"Rp{x:,.0f}"


def delta_pct(a, b):
    if b is None or pd.isna(b) or b == 0:
        return None
    return f"{(a - b) / abs(b) * 100:+.1f}%"


def gambar(fig, tinggi=380, **kw):
    fig.update_layout(height=tinggi, **TEMA, **kw)
    st.plotly_chart(fig, width="stretch")


# ------------------------------------------------------------------ sidebar
st.sidebar.title("🎛️ Filter")
f_prov = st.sidebar.selectbox("Provinsi", [SEMUA] + PROVINSI)
f_kat = st.sidebar.selectbox("Kategori produk", [SEMUA] + KATEGORI)
b_awal, b_akhir = st.sidebar.select_slider("Rentang bulan", options=BULAN, value=(BULAN[0], BULAN[-1]))
i0, i1 = BULAN.index(b_awal), BULAN.index(b_akhir)
bulan_aktif = BULAN[i0:i1 + 1]
n_bln = len(bulan_aktif)
bulan_lalu = BULAN[i0 - n_bln:i0] if i0 - n_bln >= 0 else []

st.sidebar.markdown("---")
st.sidebar.markdown("**ℹ️ Sumber data**")
st.sidebar.caption(
    f"Tabel gold diperbarui: **{meta['diperbarui_pada']}**  \n"
    f"Spark {meta['spark']['spark_versi']} · master `{meta['spark']['master']}`  \n"
    f"{meta['baris_silver']:,} pesanan bersih → {sum(meta['baris_gold'].values()):,} baris gold")
st.sidebar.caption("Pembanding KPI = periode sebelumnya dengan panjang yang sama "
                   "(kosong bila periode sebelumnya di luar data).")

# ------------------------------------------------------------------ header + KPI
lingkup = " · ".join(x for x in [f_prov if f_prov != SEMUA else "Nasional",
                                 f_kat if f_kat != SEMUA else "semua kategori",
                                 f"{b_awal} s.d. {b_akhir}"])
st.title("📊 Dashboard Penjualan Nasional")
st.caption(f"Lingkup: **{lingkup}** — data sintetis marketplace, diolah dengan Apache Spark")

kini = ringkas(pilih_cube(f_prov, f_kat, bulan_aktif))
lalu = ringkas(pilih_cube(f_prov, f_kat, bulan_lalu)) if bulan_lalu else None
L = (lambda k: lalu[k]) if lalu is not None else (lambda k: None)

k = st.columns(6)
k[0].metric("Omzet bersih", rp(kini.omzet), delta_pct(kini.omzet, L("omzet")))
k[1].metric("Pesanan", f"{int(kini.pesanan):,}", delta_pct(kini.pesanan, L("pesanan")))
k[2].metric("Nilai rata-rata (AOV)", rp(kini.aov), delta_pct(kini.aov, L("aov")))
k[3].metric("Rata-rata lama kirim", f"{kini.rata2_kirim:.2f} hari",
            delta_pct(kini.rata2_kirim, L("rata2_kirim")), delta_color="inverse")
k[4].metric("Rating rata-rata", f"{kini.rata2_rating:.2f} / 5", delta_pct(kini.rata2_rating, L("rata2_rating")))
k[5].metric("Tingkat batal", f"{kini.tingkat_batal:.1%}",
            delta_pct(kini.tingkat_batal, L("tingkat_batal")), delta_color="inverse")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Ringkasan & Tren", "🗺️ Wilayah (drill-down)",
                                        "🚚 Logistik & Kepuasan", "🛒 Perilaku Pelanggan",
                                        "⚙️ Pipeline & Kualitas Data"])

# ================================================================== TAB 1
with tab1:
    c1, c2 = st.columns([3, 2])
    with c1:
        bln = ringkas(pilih_cube(f_prov, f_kat, bulan_aktif), by="bulan").sort_values("bulan")
        puncak = bln.loc[bln.omzet.idxmax(), "bulan"]
        fig = go.Figure(go.Bar(
            x=bln.bulan, y=bln.omzet / 1e9,
            marker_color=[MERAH if b == puncak else BIRU for b in bln.bulan],
            customdata=np.c_[bln.pesanan, bln.aov],
            hovertemplate="<b>%{x}</b><br>Omzet: Rp%{y:,.1f} M<br>Pesanan: %{customdata[0]:,}"
                          "<br>AOV: Rp%{customdata[1]:,.0f}<extra></extra>"))
        fig.update_yaxes(title="Omzet (miliar Rp)", rangemode="tozero")
        gambar(fig, title=f"Omzet bulanan — puncak pada {puncak}")
    with c2:
        kat_d = ringkas(pilih_cube(f_prov, f_kat, bulan_aktif, per="kategori"), by="kategori") \
            .sort_values("omzet")
        teratas = kat_d.kategori.iloc[-1]
        fig = go.Figure(go.Bar(
            y=kat_d.kategori, x=kat_d.omzet / 1e9, orientation="h",
            marker_color=[MERAH if x == teratas else ABU for x in kat_d.kategori],
            text=[f"{v:,.1f}" for v in kat_d.omzet / 1e9], textposition="outside",
            hovertemplate="<b>%{y}</b><br>Omzet: Rp%{x:,.1f} M<extra></extra>"))
        fig.update_xaxes(title="Omzet (miliar Rp)", range=[0, kat_d.omzet.max() / 1e9 * 1.18])
        gambar(fig, title=f"{teratas} menyumbang omzet terbesar")

    # Deret waktu harian dengan MA7 (dihitung Spark dengan window function).
    kunci = f_prov if f_prov != SEMUA else "NASIONAL"
    h = G["gold_harian"]
    h = h[(h.provinsi == kunci) & h.tanggal.dt.strftime("%Y-%m").isin(bulan_aktif)].sort_values("tanggal")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=h.tanggal, y=h.omzet / 1e9, name="Harian", mode="lines",
                             line=dict(color=ABU, width=1),
                             hovertemplate="%{x|%d %b %Y}<br>Rp%{y:,.2f} M<extra>Harian</extra>"))
    fig.add_trace(go.Scatter(x=h.tanggal, y=h.omzet_ma7 / 1e9, name="Rata-rata bergerak 7 hari",
                             mode="lines", line=dict(color=BIRU, width=2.5),
                             hovertemplate="%{x|%d %b %Y}<br>Rp%{y:,.2f} M<extra>MA7</extra>"))
    for tgl in config.HARI_PROMO_BESAR:
        t = pd.Timestamp(tgl)
        if t in set(h.tanggal):
            y = h.loc[h.tanggal == t, "omzet"].iloc[0] / 1e9
            fig.add_annotation(x=t, y=y, text=t.strftime("%d.%m"), showarrow=True, arrowhead=0,
                               ax=25, ay=-10, font=dict(color=MERAH, size=12))
    a, b = (pd.Timestamp(x) for x in config.RAMADAN)
    if h.tanggal.min() <= b and h.tanggal.max() >= a:
        fig.add_vrect(x0=a, x1=b, fillcolor=BIRU, opacity=0.07, line_width=0,
                      annotation_text="Ramadan", annotation_position="top left")
    fig.update_yaxes(title="Omzet (miliar Rp)")
    gambar(fig, tinggi=360, title=f"Omzet harian — {kunci} (filter kategori tidak berlaku pada grafik ini)",
           legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
           hovermode="x unified")

# ================================================================== TAB 2
with tab2:
    st.caption("Pola *overview first, zoom and filter, details on demand*: klik kotak treemap "
               "untuk masuk dari pulau → provinsi → kategori. (Sumber: `gold_rollup_wilayah`, periode penuh)")
    r = rasio(G["gold_rollup_wilayah"])
    ids, labels, parents, nilai, warna, hover = [], [], [], [], [], []
    for _, x in r.iterrows():
        if x.gid == 7:
            i, lab, par = "Indonesia", "Indonesia", ""
        elif x.gid == 3:
            i, lab, par = x.pulau, x.pulau, "Indonesia"
        elif x.gid == 1:
            i, lab, par = f"{x.pulau}/{x.provinsi}", x.provinsi, x.pulau
        else:
            i, lab, par = f"{x.pulau}/{x.provinsi}/{x.kategori}", x.kategori, f"{x.pulau}/{x.provinsi}"
        ids.append(i); labels.append(lab); parents.append(par)
        nilai.append(x.omzet / 1e9); warna.append(x.rata2_kirim)
        hover.append(f"Omzet Rp{x.omzet / 1e9:,.1f} M<br>Pesanan {int(x.pesanan):,}"
                     f"<br>Lama kirim {x.rata2_kirim:.2f} hari<br>Rating {x.rata2_rating:.2f}")
    fig = go.Figure(go.Treemap(
        ids=ids, labels=labels, parents=parents, values=nilai, branchvalues="total",
        marker=dict(colors=warna, colorscale=[[0, "#cfe2f8"], [0.5, "#f7d9b5"], [1, MERAH]],
                    colorbar=dict(title="Rata-rata<br>kirim (hari)")),
        customdata=hover, hovertemplate="<b>%{label}</b><br>%{customdata}<extra></extra>",
        maxdepth=2, textinfo="label+percent root"))
    gambar(fig, tinggi=460, title="Komposisi omzet menurut wilayah — warna = rata-rata lama kirim")

    c1, c2 = st.columns([3, 2])
    with c1:
        p = ringkas(pilih_cube(f_prov, f_kat, bulan_aktif, per="provinsi"), by="provinsi") \
            .sort_values("omzet")
        lambat = p.rata2_kirim > config.AMBANG_KIRIM_LAMBAT
        fig = go.Figure(go.Bar(
            y=p.provinsi, x=p.omzet / 1e9, orientation="h",
            marker_color=np.where(lambat, MERAH, BIRU),
            marker_line=dict(color=np.where(p.provinsi == f_prov, "#191919", "rgba(0,0,0,0)"), width=2),
            customdata=np.c_[p.rata2_kirim, p.rata2_rating, p.pesanan],
            hovertemplate="<b>%{y}</b><br>Omzet Rp%{x:,.1f} M<br>Lama kirim %{customdata[0]:.2f} hari"
                          "<br>Rating %{customdata[1]:.2f}<br>Pesanan %{customdata[2]:,}<extra></extra>"))
        fig.update_xaxes(title="Omzet (miliar Rp)")
        gambar(fig, tinggi=420,
               title=f"Peringkat provinsi — merah = rata-rata kirim > {config.AMBANG_KIRIM_LAMBAT:.0f} hari")
    with c2:
        tabel = p.sort_values("omzet", ascending=False).assign(
            omzet_m=lambda d: d.omzet / 1e9, aov_rb=lambda d: d.aov / 1e3,
            batal_pct=lambda d: d.tingkat_batal * 100)[
            ["provinsi", "omzet_m", "pesanan", "aov_rb", "rata2_kirim", "rata2_rating", "batal_pct"]]
        st.markdown("**Detail per provinsi** (*details on demand*)")
        st.dataframe(
            tabel, hide_index=True, width="stretch", height=400,
            column_config={
                "provinsi": "Provinsi",
                "omzet_m": st.column_config.NumberColumn("Omzet (M)", format="%.1f"),
                "pesanan": st.column_config.NumberColumn("Pesanan", format="%d"),
                "aov_rb": st.column_config.NumberColumn("AOV (rb)", format="%.0f"),
                "rata2_kirim": st.column_config.ProgressColumn("Kirim (hari)", format="%.2f",
                                                               min_value=0, max_value=6),
                "rata2_rating": st.column_config.NumberColumn("Rating", format="%.2f"),
                "batal_pct": st.column_config.NumberColumn("Batal %", format="%.1f"),
            })

# ================================================================== TAB 3
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        # Box plot dari kuantil yang dihitung Spark (tidak ada data mentah yang dikirim).
        q = G["gold_kuantil_kirim"]
        q = q[q.provinsi != "NASIONAL"].sort_values("rata2")
        fig = go.Figure()
        for _, x in q.iterrows():
            lambat = x.rata2 > config.AMBANG_KIRIM_LAMBAT
            fig.add_trace(go.Box(
                y=[x.provinsi], q1=[x.q1], median=[x["median"]], q3=[x.q3], lowerfence=[x.p05],
                upperfence=[x.p95], mean=[x.rata2], orientation="h", name=x.provinsi,
                fillcolor="#f9c9c9" if lambat else "#cfe2f8",
                line=dict(color="#191919" if x.provinsi == f_prov else "#555", width=2 if x.provinsi == f_prov else 1),
                showlegend=False))
        fig.add_vline(x=config.AMBANG_KIRIM_LAMBAT, line_dash="dash", line_color=MERAH,
                      annotation_text="target 3 hari", annotation_position="bottom right",
                      annotation_font_color=MERAH)
        fig.update_xaxes(title="Lama kirim (hari) — kumis = persentil 5 & 95")
        gambar(fig, tinggi=430, title="Luar Jawa-Bali: lebih lama DAN lebih tidak pasti")
    with c2:
        kunci = f_prov if f_prov != SEMUA else "NASIONAL"
        hm = G["gold_heatmap_kirim_rating"]
        hm = hm[hm.provinsi == kunci].pivot(index="rating", columns="bin_kirim", values="jumlah").sort_index()
        fig = go.Figure(go.Heatmap(
            z=np.log10(hm.values), x=hm.columns, y=hm.index, customdata=hm.values,
            colorscale="Blues", colorbar=dict(title="log10<br>pesanan"),
            hovertemplate="Kirim %{x} hari · rating %{y}<br>%{customdata:,.0f} pesanan<extra></extra>"))
        fig.update_xaxes(title="Lama kirim (bin 0,5 hari)")
        fig.update_yaxes(title="Rating", dtick=1)
        gambar(fig, tinggi=430, title=f"Heatmap seluruh data ({kunci}): rating turun saat kirim lama")

    hm = G["gold_heatmap_kirim_rating"]
    hm = hm[hm.provinsi == (f_prov if f_prov != SEMUA else "NASIONAL")].copy()
    hm["hari"] = hm.bin_kirim.round().astype(int)
    rk = hm.assign(rj=hm.rating * hm.jumlah).groupby("hari", as_index=False)[["rj", "jumlah"]].sum()
    rk = rk[rk.jumlah >= 200]
    rk["rating"] = rk.rj / rk.jumlah
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(go.Scatter(x=rk.hari, y=rk.rating, mode="lines+markers",
                                   line=dict(color=BIRU, width=2.5), marker=dict(size=9),
                                   customdata=rk.jumlah,
                                   hovertemplate="%{x} hari → rating %{y:.2f}<br>n = %{customdata:,}<extra></extra>"))
        fig.update_yaxes(title="Rating rata-rata", range=[1, 5])
        fig.update_xaxes(title="Lama kirim (hari, dibulatkan)", dtick=1)
        gambar(fig, tinggi=330, title="Setiap tambahan 1 hari kirim menurunkan rating ±0,35")
    with c2:
        p = ringkas(pilih_cube(SEMUA, f_kat, bulan_aktif, per="provinsi"), by="provinsi") \
            .sort_values("pct_kirim_lambat")
        fig = go.Figure(go.Bar(
            y=p.provinsi, x=p.pct_kirim_lambat * 100, orientation="h",
            marker_color=np.where(p.pct_kirim_lambat > 0.2, MERAH, ABU),
            text=[f"{v:.1f}%" for v in p.pct_kirim_lambat * 100], textposition="outside",
            hovertemplate="%{y}: %{x:.1f}% pesanan > 3 hari<extra></extra>"))
        fig.update_xaxes(title="% pesanan dikirim > 3 hari", range=[0, max(p.pct_kirim_lambat * 100) * 1.2])
        gambar(fig, tinggi=330, title="Porsi pesanan terlambat (> 3 hari) per provinsi")

# ================================================================== TAB 4
with tab4:
    kunci = f_prov if f_prov != SEMUA else "NASIONAL"
    jh = G["gold_jam_hari"]
    jh = jh[jh.provinsi == kunci].pivot(index="hari_minggu", columns="jam", values="pesanan").sort_index()
    fig = go.Figure(go.Heatmap(
        z=jh.values, x=[f"{j:02d}" for j in jh.columns], y=[NAMA_HARI[i] for i in jh.index],
        colorscale="Blues", colorbar=dict(title="Pesanan"),
        hovertemplate="%{y} jam %{x}.00<br>%{z:,} pesanan<extra></extra>"))
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(title="Jam")
    puncak = jh.stack().idxmax()
    gambar(fig, tinggi=330,
           title=f"Kapan pelanggan berbelanja? ({kunci}) — puncak {NAMA_HARI[puncak[0]]} pukul {puncak[1]:02d}.00")

    c1, c2 = st.columns(2)
    with c1:
        mb = G["gold_metode_bayar"].groupby("metode_bayar", as_index=False)[
            ["SELESAI", "DIBATALKAN", "DIKEMBALIKAN"]].sum()
        mb["total"] = mb[["SELESAI", "DIBATALKAN", "DIKEMBALIKAN"]].sum(axis=1)
        mb["batal"] = mb.DIBATALKAN / mb.total * 100
        mb = mb.sort_values("batal")
        fig = go.Figure(go.Bar(
            y=mb.metode_bayar, x=mb.batal, orientation="h",
            marker_color=[MERAH if m == mb.metode_bayar.iloc[-1] else ABU for m in mb.metode_bayar],
            text=[f"{v:.1f}%" for v in mb.batal], textposition="outside",
            customdata=mb.total, hovertemplate="%{y}: %{x:.1f}% batal<br>dari %{customdata:,} pesanan<extra></extra>"))
        fig.update_xaxes(title="Tingkat pembatalan (%)", range=[0, mb.batal.max() * 1.25])
        gambar(fig, tinggi=340, title=f"{mb.metode_bayar.iloc[-1]} paling sering dibatalkan")
    with c2:
        qn = G["gold_kuantil_nilai"].sort_values("median")
        fig = go.Figure()
        for _, x in qn.iterrows():
            fig.add_trace(go.Box(y=[x.kategori], q1=[x.q1 / 1e3], median=[x["median"] / 1e3], q3=[x.q3 / 1e3],
                                 lowerfence=[x.p05 / 1e3], upperfence=[x.p95 / 1e3], mean=[x.rata2 / 1e3],
                                 orientation="h", fillcolor="#cfe2f8", line=dict(color="#555"),
                                 showlegend=False, name=x.kategori))
        fig.update_xaxes(title="Nilai transaksi (ribu Rp, skala log)", type="log")
        gambar(fig, tinggi=340, title="Sebaran nilai transaksi per kategori (P5–P95)")

# ================================================================== TAB 5
with tab5:
    st.markdown("#### Arsitektur pipeline")
    st.graphviz_chart("""
    digraph {
      rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11];
      gen   [label="Pembangkit data\\n(spark.range)", fillcolor="#eeeeee"];
      bronze[label="BRONZE\\nParquet mentah\\n(partisi bulan)", fillcolor="#f4d8b8"];
      silver[label="SILVER\\nbersih + diperkaya", fillcolor="#e3e3e3"];
      gold  [label="GOLD\\ncube · rollup · kuantil\\nheatmap · deret waktu", fillcolor="#f6e7a6"];
      dash  [label="Dashboard\\nStreamlit", fillcolor="#cfe2f8"];
      bi    [label="CSV → Excel /\\nPower BI / Tableau", fillcolor="#cfe2f8"];
      png   [label="PNG laporan", fillcolor="#cfe2f8"];
      gen -> bronze -> silver -> gold; gold -> dash; gold -> bi; gold -> png;
      subgraph cluster_spark { label="Apache Spark (local[*] atau klaster virtual Docker)";
        style=dashed; color="#888"; bronze; silver; gold; }
    }""")

    m = st.columns(4)
    m[0].metric("Baris bronze (mentah)", f"{meta['baris_bronze']:,}")
    m[1].metric("Baris silver (bersih)", f"{meta['baris_silver']:,}")
    m[2].metric("Baris gold (semua tabel)", f"{sum(meta['baris_gold'].values()):,}")
    rasio_kompres = meta["baris_silver"] / max(1, sum(meta["baris_gold"].values()))
    m[3].metric("Rasio reduksi", f"{rasio_kompres:,.0f} : 1")

    m = st.columns(4)
    m[0].metric("Ukuran bronze", f"{meta['ukuran_mb']['bronze']:,.1f} MB")
    m[1].metric("Ukuran silver", f"{meta['ukuran_mb']['silver']:,.1f} MB")
    m[2].metric("Ukuran gold", f"{meta['ukuran_mb']['gold']:,.2f} MB")
    m[3].metric("Latensi: gold vs silver",
                f"{meta['latensi_detik']['gold'] * 1000:.0f} ms",
                f"silver {meta['latensi_detik']['silver']:.2f} dtk", delta_color="off")

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown("#### Laporan kualitas data (dari langkah silver)")
        st.dataframe(G["gold_data_quality"], hide_index=True, width="stretch",
                     column_config={"nilai": st.column_config.NumberColumn("Nilai", format="%d")})
    with c2:
        st.markdown("#### Durasi tiap langkah (detik)")
        d = pd.DataFrame(list(meta["durasi_detik"].items()), columns=["langkah", "detik"])
        fig = px.bar(d, x="detik", y="langkah", orientation="h", text="detik",
                     color_discrete_sequence=[BIRU])
        gambar(fig, tinggi=250)

    st.markdown("#### Unduh tabel gold (CSV untuk Excel / Power BI)")
    kolom = st.columns(4)
    for i, (nama, n) in enumerate(meta["baris_gold"].items()):
        jalur = os.path.join(config.GOLD_DIR, "csv", f"{nama}.csv")
        if os.path.exists(jalur):
            with open(jalur, "rb") as f:
                kolom[i % 4].download_button(f"⬇️ {nama} ({n:,} baris)", f, file_name=f"{nama}.csv",
                                             mime="text/csv", width="stretch")

st.markdown("---")
st.caption(f"Sumber: tabel gold pipeline Spark BD-P09 (data sintetis) · diperbarui {meta['diperbarui_pada']} · "
           f"periode Okt 2025 – Sep 2026 · Omzet = nilai bersih pesanan berstatus SELESAI setelah diskon.")
