"""
============================================================
  E-Commerce Sales Monitoring & Performance Analysis
  Global Superstore Dataset — Full Pipeline (VS Code)
  Author : Akash Manur | Data Analyst
============================================================

PIPELINE
  1.  Read CSV + XLSX files
  2.  Merge into one unified dataset
  3.  Data Cleaning & Preprocessing
  4.  Feature Engineering
  5.  EDA — Summary Statistics
  6.  MySQL Integration
  7.  12 Advanced Visualisations
  8.  Save all visualisations as one image
  9.  Save final unified cleaned CSV
============================================================

INSTALL REQUIREMENTS (run once in terminal):
  pip install pandas matplotlib seaborn scipy scikit-learn sqlalchemy mysql-connector-python openpyxl xlrd
"""

# ─────────────────────────────────────────────
# 0. IMPORTS
# ─────────────────────────────────────────────
import warnings
warnings.filterwarnings("ignore")

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns
from scipy import stats
from sklearn.preprocessing import LabelEncoder

print("=" * 65)
print("  E-COMMERCE SALES MONITORING & PERFORMANCE ANALYSIS")
print("  Global Superstore  |  Full Analytics Pipeline")
print("=" * 65)

# ─────────────────────────────────────────────
# FILE PATHS — update if needed
# ─────────────────────────────────────────────
BASE_DIR  = r"C:\Users\akash\E-commerce_Sales_Monitoring"
CSV_PATH  = os.path.join(BASE_DIR, "Global_Superstore2.csv")
XLSX_PATH = os.path.join(BASE_DIR, "Global_Superstore2.xlsx")
OUT_DIR   = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# 1. READ FILES
# ─────────────────────────────────────────────
print("\n[1/9]  Reading CSV and XLSX files …")

# ── Read CSV ──
df_csv = pd.read_csv(
    CSV_PATH,
    encoding="latin-1",       # handles special characters
    sep=",",
    low_memory=False
)
print(f"   CSV  loaded : {df_csv.shape[0]:,} rows × {df_csv.shape[1]} columns")
print(f"   CSV  columns: {list(df_csv.columns)}")

# ── Read XLSX ──
df_xlsx = pd.read_excel(
    XLSX_PATH,
    engine="openpyxl",
    sheet_name=0              # first sheet
)
print(f"\n   XLSX loaded : {df_xlsx.shape[0]:,} rows × {df_xlsx.shape[1]} columns")
print(f"   XLSX columns: {list(df_xlsx.columns)}")

# ─────────────────────────────────────────────
# 2. MERGE CSV + XLSX
# ─────────────────────────────────────────────
print("\n[2/9]  Merging CSV + XLSX …")

# Identify common merge key (Product Name is common in both)
MERGE_KEY = "Product Name"

# If XLSX has extra/different columns, suffix them
df = df_csv.merge(
    df_xlsx,
    on=MERGE_KEY,
    how="left",
    suffixes=("", "_xlsx")
)

# Drop exact duplicate columns brought in from xlsx
# (Sales, Quantity, Discount, Profit, Shipping Cost, Order Priority already in CSV)
dup_cols = [c for c in df.columns if c.endswith("_xlsx")]
if dup_cols:
    print(f"   Dropping duplicate xlsx columns: {dup_cols}")
    df.drop(columns=dup_cols, inplace=True)

print(f"   Merged shape : {df.shape[0]:,} rows × {df.shape[1]} columns")

# ─────────────────────────────────────────────
# 3. DATA CLEANING & PREPROCESSING
# ─────────────────────────────────────────────
print("\n[3/9]  Data Cleaning & Preprocessing …")

print(f"   Before cleaning  : {df.shape[0]:,} rows")

# ── 3a. Duplicates ──
dup_count = df.duplicated().sum()
print(f"   Duplicates found : {dup_count:,}")
df.drop_duplicates(inplace=True)

# ── 3b. Missing values ──
print(f"\n   Missing values per column:")
miss = df.isnull().sum()
miss = miss[miss > 0]
if len(miss):
    print(miss.to_string())
else:
    print("   None found.")

# Fill Postal Code missing (common in international data)
if "Postal Code" in df.columns:
    df["Postal Code"] = df["Postal Code"].fillna("N/A").astype(str)

# Fill numeric columns with median
num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
for col in num_cols:
    if df[col].isnull().sum() > 0:
        df[col].fillna(df[col].median(), inplace=True)

# Fill categorical columns with mode
cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
for col in cat_cols:
    if df[col].isnull().sum() > 0:
        df[col].fillna(df[col].mode()[0], inplace=True)

# ── 3c. Date parsing ──
for date_col in ["Order Date", "Ship Date"]:
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")

# ── 3d. Type fixes ──
if "Quantity" in df.columns:
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
if "Sales" in df.columns:
    df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce")
if "Profit" in df.columns:
    df["Profit"] = pd.to_numeric(df["Profit"], errors="coerce")
if "Discount" in df.columns:
    df["Discount"] = pd.to_numeric(df["Discount"], errors="coerce")

# ── 3e. Remove rows with non-positive sales ──
if "Sales" in df.columns:
    before = len(df)
    df = df[df["Sales"] > 0]
    print(f"\n   Removed {before - len(df)} rows with Sales ≤ 0")

# ── 3f. Outlier Report (IQR) ──
print("\n   Outlier check (IQR method):")
for col in ["Sales", "Profit", "Shipping Cost"]:
    if col in df.columns:
        Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[(df[col] < Q1 - 1.5*IQR) | (df[col] > Q3 + 1.5*IQR)]
        print(f"   {col:20s} → {len(outliers):,} outliers detected (kept for analysis)")

print(f"\n   After  cleaning  : {df.shape[0]:,} rows")
print(f"   Total missing now: {df.isnull().sum().sum()}")

# ─────────────────────────────────────────────
# 4. FEATURE ENGINEERING
# ─────────────────────────────────────────────
print("\n[4/9]  Feature Engineering …")

if "Order Date" in df.columns:
    df["Year"]        = df["Order Date"].dt.year
    df["Month"]       = df["Order Date"].dt.month
    df["Month Name"]  = df["Order Date"].dt.strftime("%b")
    df["Quarter"]     = df["Order Date"].dt.quarter
    df["Day of Week"] = df["Order Date"].dt.day_name()

if "Order Date" in df.columns and "Ship Date" in df.columns:
    df["Ship Days"] = (df["Ship Date"] - df["Order Date"]).dt.days
    df["Ship Days"] = df["Ship Days"].clip(lower=0)   # no negative ship days

if "Profit" in df.columns and "Sales" in df.columns:
    df["Profit Margin %"] = (df["Profit"] / df["Sales"] * 100).round(2)

if "Sales" in df.columns and "Quantity" in df.columns:
    df["Revenue per Unit"] = (df["Sales"] / df["Quantity"].replace(0, np.nan)).round(2)

if "Discount" in df.columns:
    df["Discount %"]    = (df["Discount"] * 100).round(1)
    df["Disc Category"] = pd.cut(
        df["Discount"],
        bins=[-0.01, 0, 0.1, 0.2, 0.3, 1.0],
        labels=["No Discount", "1-10%", "11-20%", "21-30%", ">30%"]
    )

if "Profit" in df.columns:
    df["Profit Status"] = df["Profit"].apply(lambda x: "Profit" if x >= 0 else "Loss")

if "Ship Days" in df.columns:
    df["Delivery Speed"] = pd.cut(
        df["Ship Days"],
        bins=[-1, 0, 2, 5, 100],
        labels=["Same Day", "Express (1-2d)", "Standard (3-5d)", "Slow (5d+)"]
    )

# Label encode for correlation analysis
le = LabelEncoder()
for col in ["Category", "Segment", "Ship Mode", "Region", "Market"]:
    if col in df.columns:
        df[col + " Code"] = le.fit_transform(df[col].astype(str))

new_features = ["Year","Month","Quarter","Ship Days","Profit Margin %",
                 "Revenue per Unit","Disc Category","Profit Status","Delivery Speed"]
print(f"   New features added: {[f for f in new_features if f in df.columns]}")

# ─────────────────────────────────────────────
# 5. EDA — SUMMARY STATISTICS
# ─────────────────────────────────────────────
print("\n[5/9]  Exploratory Data Analysis …")

print("\n── Descriptive Statistics ──")
eda_cols = [c for c in ["Sales","Profit","Discount","Quantity",
                          "Shipping Cost","Profit Margin %","Ship Days"] if c in df.columns]
print(df[eda_cols].describe().round(2).to_string())

print("\n── Business KPIs ──")
kpis = {
    "Total Sales"       : f"${df['Sales'].sum():>15,.2f}"      if "Sales"  in df.columns else "N/A",
    "Total Profit"      : f"${df['Profit'].sum():>15,.2f}"     if "Profit" in df.columns else "N/A",
    "Total Orders"      : f"{df['Order ID'].nunique():>16,}"   if "Order ID" in df.columns else "N/A",
    "Avg Profit Margin" : f"{df['Profit Margin %'].mean():>14.2f}%"  if "Profit Margin %" in df.columns else "N/A",
    "Avg Discount"      : f"{df['Discount %'].mean():>14.2f}%"       if "Discount %" in df.columns else "N/A",
    "Avg Ship Days"     : f"{df['Ship Days'].mean():>15.2f}"          if "Ship Days" in df.columns else "N/A",
    "Loss-Making Orders": f"{(df['Profit']<0).sum():>16,}"            if "Profit" in df.columns else "N/A",
}
for k, v in kpis.items():
    print(f"   {k:22s}: {v}")

print("\n── Sales & Profit by Category ──")
if "Category" in df.columns:
    print(df.groupby("Category")[["Sales","Profit"]].sum().round(2).to_string())

print("\n── Top 5 Regions by Sales ──")
if "Region" in df.columns:
    print(df.groupby("Region")["Sales"].sum().sort_values(ascending=False).head(5).to_string())

print("\n── Top 5 Countries by Profit ──")
if "Country" in df.columns:
    print(df.groupby("Country")["Profit"].sum().sort_values(ascending=False).head(5).to_string())

print("\n── Return / Loss Rate by Segment ──")
if "Segment" in df.columns and "Profit Status" in df.columns:
    print(df.groupby(["Segment","Profit Status"]).size().unstack(fill_value=0).to_string())

print("\n── Correlation Matrix ──")
corr_cols = [c for c in ["Sales","Profit","Discount","Quantity",
                           "Shipping Cost","Profit Margin %","Ship Days"] if c in df.columns]
print(df[corr_cols].corr().round(2).to_string())

# ─────────────────────────────────────────────
# 6. MYSQL INTEGRATION
# ─────────────────────────────────────────────
print("\n[6/9]  MySQL Integration …")

# ── OPTION A: MySQL (fill in your credentials) ──
USE_MYSQL = False   # Set to True when MySQL is running

if USE_MYSQL:
    try:
        from sqlalchemy import create_engine
        DB_HOST     = "localhost"
        DB_PORT     = 3306
        DB_USER     = "root"
        DB_PASSWORD = "system"      # ← change this
        DB_NAME     = "Sales_Monitoring"       # ← change this

        engine = create_engine(
            f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        df.to_sql("global_superstore", con=engine, if_exists="replace", index=False, chunksize=500)
        print("   ✓  Data pushed to MySQL successfully")

        # Run analytical queries
        import pandas as pd
        queries = {
            "Top 5 Countries by Profit": "SELECT Country, ROUND(SUM(Profit),2) AS Total_Profit FROM global_superstore GROUP BY Country ORDER BY Total_Profit DESC LIMIT 5",
            "Category Performance"      : "SELECT Category, ROUND(SUM(Sales),2) AS Sales, ROUND(SUM(Profit),2) AS Profit, ROUND(AVG(Discount)*100,2) AS Avg_Disc_Pct FROM global_superstore GROUP BY Category",
            "Yearly Revenue"            : "SELECT Year, ROUND(SUM(Sales),2) AS Revenue FROM global_superstore GROUP BY Year ORDER BY Year",
        }
        for title, sql in queries.items():
            print(f"\n  {title}:")
            print(pd.read_sql(sql, engine).to_string(index=False))

    except Exception as e:
        print(f"   ✗  MySQL connection failed: {e}")
        print("   → Skipping MySQL. Set USE_MYSQL=True and provide correct credentials.")
else:
    # ── OPTION B: SQLite (runs automatically, no setup needed) ──
    import sqlite3
    DB_PATH = os.path.join(OUT_DIR, "global_superstore.db")
    conn    = sqlite3.connect(DB_PATH)
    df.to_sql("global_superstore", conn, if_exists="replace", index=False)

    sql_queries = {
        "Top 5 Countries by Profit": """
            SELECT Country, ROUND(SUM(Profit),2) AS Total_Profit
            FROM global_superstore GROUP BY Country
            ORDER BY Total_Profit DESC LIMIT 5""",
        "Category Sales & Profit": """
            SELECT Category,
                   ROUND(SUM(Sales),2) AS Total_Sales,
                   ROUND(SUM(Profit),2) AS Total_Profit,
                   ROUND(AVG(Discount)*100,2) AS Avg_Discount_Pct
            FROM global_superstore GROUP BY Category""",
        "Yearly Revenue": """
            SELECT Year, ROUND(SUM(Sales),2) AS Revenue,
                   ROUND(SUM(Profit),2) AS Profit
            FROM global_superstore GROUP BY Year ORDER BY Year""",
        "Ship Mode Profitability": """
            SELECT "Ship Mode", COUNT(*) AS Orders,
                   ROUND(SUM(Profit),2) AS Total_Profit,
                   ROUND(AVG("Shipping Cost"),2) AS Avg_Ship_Cost
            FROM global_superstore
            GROUP BY "Ship Mode" ORDER BY Total_Profit DESC""",
    }
    print("\n── SQL Query Results ──")
    for title, sql in sql_queries.items():
        result = pd.read_sql(sql, conn)
        print(f"\n  {title}:")
        print(result.to_string(index=False))
    conn.close()
    print(f"\n   ✓  SQLite DB saved → {DB_PATH}")
    print("   ℹ  To use MySQL: set USE_MYSQL=True and fill in credentials above")

# ─────────────────────────────────────────────
# 7. ADVANCED VISUALISATIONS (12 charts)
# ─────────────────────────────────────────────
print("\n[7/9]  Generating 12 advanced visualisations …")

# ── Palette ──
TEAL   = "#0d9488"; CORAL = "#f97316"; NAVY  = "#1e3a5f"
PURPLE = "#7c3aed"; GOLD  = "#eab308"; RED   = "#dc2626"
GREEN  = "#16a34a"; BG    = "#f8fafc"; GREY  = "#64748b"

sns.set_theme(style="whitegrid", palette="muted")

fig = plt.figure(figsize=(28, 46))
fig.patch.set_facecolor(BG)

fig.text(0.5, 0.995,
         "E-Commerce Sales Monitoring & Performance Dashboard",
         ha="center", va="top", fontsize=22, fontweight="bold", color=NAVY)
fig.text(0.5, 0.990,
         "Global Superstore  |  Akash Manur — Data Analyst",
         ha="center", va="top", fontsize=11, color=GREY)

gs = gridspec.GridSpec(6, 4, figure=fig,
                        hspace=0.55, wspace=0.38,
                        top=0.983, bottom=0.01,
                        left=0.05, right=0.97)

def style_ax(ax, title, xlabel="", ylabel=""):
    ax.set_facecolor("white")
    ax.set_title(title, fontsize=11, fontweight="bold", color=NAVY, pad=8)
    if xlabel: ax.set_xlabel(xlabel, fontsize=9, color=GREY)
    if ylabel: ax.set_ylabel(ylabel, fontsize=9, color=GREY)
    ax.tick_params(labelsize=8, colors=GREY)
    for sp in ax.spines.values():
        sp.set_edgecolor("#e2e8f0")

# ── VIZ 01: Monthly Sales Trend ──
ax1 = fig.add_subplot(gs[0, :2])
if "Year" in df.columns and "Month" in df.columns:
    monthly = df.groupby(["Year","Month"])["Sales"].sum().reset_index()
    for yr, grp in monthly.groupby("Year"):
        grp = grp.sort_values("Month")
        ax1.plot(grp["Month"], grp["Sales"]/1e3, marker="o", markersize=4,
                 label=str(yr), linewidth=2)
        ax1.fill_between(grp["Month"], grp["Sales"]/1e3, alpha=0.07)
    ax1.set_xticks(range(1,13))
    ax1.set_xticklabels(["Jan","Feb","Mar","Apr","May","Jun",
                          "Jul","Aug","Sep","Oct","Nov","Dec"], fontsize=8)
    ax1.legend(title="Year", fontsize=8)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"${v:,.0f}K"))
style_ax(ax1, "VIZ 01 · Monthly Sales Trend by Year", "Month", "Sales ($000s)")

# ── VIZ 02: Category Sales vs Profit ──
ax2 = fig.add_subplot(gs[0, 2:])
if "Category" in df.columns:
    cat_perf = df.groupby("Category")[["Sales","Profit"]].sum() / 1e3
    x = np.arange(len(cat_perf)); w = 0.35
    b1 = ax2.bar(x-w/2, cat_perf["Sales"],  w, color=TEAL,  label="Sales",  zorder=3)
    b2 = ax2.bar(x+w/2, cat_perf["Profit"], w, color=CORAL, label="Profit", zorder=3)
    ax2.set_xticks(x); ax2.set_xticklabels(cat_perf.index, fontsize=9)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"${v:,.0f}K"))
    for bar in b1: ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+2,
                             f"${bar.get_height():,.0f}K", ha="center", fontsize=7, color=NAVY)
    for bar in b2: ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+2,
                             f"${bar.get_height():,.0f}K", ha="center", fontsize=7, color=RED)
    ax2.legend(fontsize=9)
style_ax(ax2, "VIZ 02 · Sales vs Profit by Category", ylabel="Amount ($000s)")

# ── VIZ 03: Region × Category Profit Heatmap ──
ax3 = fig.add_subplot(gs[1, :2])
if "Region" in df.columns and "Category" in df.columns:
    pivot = df.pivot_table(index="Region", columns="Category",
                            values="Profit", aggfunc="sum") / 1e3
    cmap_rg = LinearSegmentedColormap.from_list("rg", [RED,"white",GREEN], N=200)
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap=cmap_rg,
                linewidths=0.5, linecolor="#e2e8f0",
                ax=ax3, cbar_kws={"shrink":0.8,"label":"$000s"})
    ax3.set_ylabel(""); ax3.tick_params(labelsize=8, rotation=0)
style_ax(ax3, "VIZ 03 · Regional Profit Heatmap (Region × Category)")

# ── VIZ 04: Discount vs Profit Margin Scatter ──
ax4 = fig.add_subplot(gs[1, 2:])
if "Discount %" in df.columns and "Profit Margin %" in df.columns:
    samp = df[["Discount %","Profit Margin %","Category Code"]].dropna().sample(
           min(3000, len(df)), random_state=1)
    ax4.scatter(samp["Discount %"], samp["Profit Margin %"],
                c=samp["Category Code"], cmap="Set2", alpha=0.4, s=15, edgecolors="none")
    x_v = samp["Discount %"].values; y_v = samp["Profit Margin %"].values
    valid = np.isfinite(x_v) & np.isfinite(y_v)
    if valid.sum() > 2:
        z = np.polyfit(x_v[valid], y_v[valid], 1)
        xr = np.linspace(0, samp["Discount %"].max(), 100)
        ax4.plot(xr, np.poly1d(z)(xr), color=RED, lw=2, linestyle="--", label="Trend")
        r, _ = stats.pearsonr(x_v[valid], y_v[valid])
        ax4.text(0.97, 0.05, f"Pearson r = {r:.2f}", transform=ax4.transAxes,
                 ha="right", fontsize=9, color=RED)
    ax4.axhline(0, color=GREY, linestyle=":", lw=1)
    ax4.legend(fontsize=8)
style_ax(ax4, "VIZ 04 · Discount % vs Profit Margin %", "Discount (%)", "Profit Margin (%)")

# ── VIZ 05: Yearly Profit by Segment ──
ax5 = fig.add_subplot(gs[2, :2])
if "Year" in df.columns and "Segment" in df.columns:
    seg_yr = df.groupby(["Year","Segment"])["Profit"].sum().unstack() / 1e3
    seg_yr.plot(kind="bar", stacked=True, ax=ax5,
                color=[TEAL,CORAL,NAVY], width=0.6, zorder=3)
    ax5.set_xticklabels(ax5.get_xticklabels(), rotation=0, fontsize=9)
    ax5.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"${v:,.0f}K"))
    ax5.legend(title="Segment", fontsize=8)
style_ax(ax5, "VIZ 05 · Yearly Profit by Segment (Stacked)", "Year", "Profit ($000s)")

# ── VIZ 06: Top 10 Sub-Categories — Loss vs Profit ──
ax6 = fig.add_subplot(gs[2, 2:])
if "Sub-Category" in df.columns:
    sub_profit = df.groupby("Sub-Category")["Profit"].sum().sort_values()
    colors_sub = [RED if v < 0 else GREEN for v in sub_profit.values]
    bars = ax6.barh(sub_profit.index, sub_profit.values/1e3,
                    color=colors_sub, zorder=3, edgecolor="white", lw=0.5)
    ax6.axvline(0, color=GREY, lw=1)
    for bar in bars:
        w = bar.get_width()
        ax6.text(w + (0.5 if w >= 0 else -0.5),
                 bar.get_y() + bar.get_height()/2,
                 f"${w:,.0f}K", va="center", ha="left" if w>=0 else "right",
                 fontsize=7, color=GREY)
    legend_h = [mpatches.Patch(color=GREEN, label="Profit"),
                mpatches.Patch(color=RED,   label="Loss")]
    ax6.legend(handles=legend_h, fontsize=8)
style_ax(ax6, "VIZ 06 · Sub-Category Profitability (Profit vs Loss)", "Profit ($000s)")

# ── VIZ 07: Ship Mode — Donut ──
ax7 = fig.add_subplot(gs[3, :2])
if "Ship Mode" in df.columns:
    ship_sales = df.groupby("Ship Mode")["Sales"].sum()
    wedges, _, autotexts = ax7.pie(
        ship_sales, labels=None, autopct="%1.1f%%",
        colors=[TEAL,CORAL,NAVY,GOLD], startangle=90,
        wedgeprops={"edgecolor":"white","linewidth":2}, pctdistance=0.75)
    for at in autotexts: at.set_fontsize(9); at.set_color("white"); at.set_fontweight("bold")
    ax7.add_patch(plt.Circle((0,0), 0.5, fc="white"))
    ax7.text(0, 0, f"${ship_sales.sum()/1e6:.1f}M\nTotal",
             ha="center", va="center", fontsize=10, fontweight="bold", color=NAVY)
    ax7.legend(ship_sales.index, loc="lower right", fontsize=8, title="Ship Mode")
style_ax(ax7, "VIZ 07 · Sales Share by Shipping Mode")

# ── VIZ 08: Quarterly Sales Area ──
ax8 = fig.add_subplot(gs[3, 2:])
if "Year" in df.columns and "Quarter" in df.columns:
    qtr = df.groupby(["Year","Quarter"])["Sales"].sum().reset_index()
    qtr["Period"] = qtr["Year"].astype(str) + " Q" + qtr["Quarter"].astype(str)
    qtr = qtr.sort_values(["Year","Quarter"]).reset_index(drop=True)
    ax8.fill_between(range(len(qtr)), qtr["Sales"]/1e3, alpha=0.2, color=TEAL)
    ax8.plot(range(len(qtr)), qtr["Sales"]/1e3, color=TEAL, lw=2.5, marker="D", ms=5)
    ax8.set_xticks(range(len(qtr)))
    ax8.set_xticklabels(qtr["Period"], rotation=45, fontsize=7, ha="right")
    ax8.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"${v:,.0f}K"))
style_ax(ax8, "VIZ 08 · Quarterly Sales Trend (Area)", ylabel="Sales ($000s)")

# ── VIZ 09: Profit Margin Distribution — Violin ──
ax9 = fig.add_subplot(gs[4, :2])
if "Category" in df.columns and "Profit Margin %" in df.columns:
    cats = df["Category"].unique()
    data_v = [df[df["Category"]==c]["Profit Margin %"].dropna().clip(-100,100) for c in cats]
    palette_v = [TEAL, CORAL, NAVY, PURPLE, GOLD]
    parts = ax9.violinplot(data_v, positions=range(1,len(cats)+1),
                           showmeans=True, showmedians=True)
    for i, pc in enumerate(parts["bodies"]):
        pc.set_facecolor(palette_v[i % len(palette_v)]); pc.set_alpha(0.7)
    parts["cmeans"].set_color(GOLD)
    parts["cmedians"].set_color(RED)
    ax9.set_xticks(range(1,len(cats)+1)); ax9.set_xticklabels(cats, fontsize=9)
    ax9.axhline(0, color=GREY, linestyle=":", lw=1)
    ax9.legend(handles=[mpatches.Patch(color=GOLD,label="Mean"),
                         mpatches.Patch(color=RED, label="Median")], fontsize=8)
style_ax(ax9, "VIZ 09 · Profit Margin Distribution by Category (Violin)",
          ylabel="Profit Margin (%)")

# ── VIZ 10: Correlation Heatmap ──
ax10 = fig.add_subplot(gs[4, 2:])
corr_cols10 = [c for c in ["Sales","Profit","Discount","Quantity",
                             "Shipping Cost","Profit Margin %","Ship Days"] if c in df.columns]
corr10 = df[corr_cols10].corr()
mask10 = np.triu(np.ones_like(corr10, dtype=bool))
sns.heatmap(corr10, mask=mask10, annot=True, fmt=".2f",
            cmap="RdYlGn", center=0, vmin=-1, vmax=1,
            square=True, linewidths=0.5, linecolor="#e2e8f0",
            ax=ax10, cbar_kws={"shrink":0.8})
ax10.tick_params(labelsize=8)
style_ax(ax10, "VIZ 10 · Feature Correlation Heatmap")

# ── VIZ 11: Top 10 Sub-Categories Sales — Lollipop ──
ax11 = fig.add_subplot(gs[5, :2])
if "Sub-Category" in df.columns:
    top10 = df.groupby("Sub-Category")["Sales"].sum().nlargest(10).sort_values()
    ax11.hlines(y=top10.index, xmin=0, xmax=top10.values/1e3, color=TEAL, lw=2, zorder=2)
    ax11.scatter(top10.values/1e3, top10.index, color=CORAL, s=80, zorder=3,
                  edgecolors=NAVY, linewidths=0.8)
    for sub, val in zip(top10.index, top10.values):
        ax11.text(val/1e3 + top10.max()/1e3*0.01, sub,
                  f"${val/1e3:,.0f}K", va="center", fontsize=7.5, color=GREY)
    ax11.set_xlim(0, top10.max()/1e3 * 1.2)
style_ax(ax11, "VIZ 11 · Top 10 Sub-Categories by Sales (Lollipop)", "Sales ($000s)")

# ── VIZ 12: Market-wise Profit — Box Plot ──
ax12 = fig.add_subplot(gs[5, 2:])
if "Market" in df.columns:
    markets = df["Market"].unique()
    data_b = [df[df["Market"]==m]["Profit"].dropna().clip(-5000,5000) for m in markets]
    bp = ax12.boxplot(data_b, patch_artist=True, notch=False,
                      medianprops={"color":RED,"linewidth":2},
                      whiskerprops={"color":GREY},
                      capprops={"color":GREY},
                      flierprops={"marker":"o","markersize":2,"alpha":0.3,"color":CORAL})
    colors12 = [TEAL,CORAL,NAVY,GOLD,PURPLE,GREEN]
    for patch, color in zip(bp["boxes"], colors12*5):
        patch.set_facecolor(color); patch.set_alpha(0.7)
    ax12.set_xticks(range(1,len(markets)+1))
    ax12.set_xticklabels(markets, rotation=30, ha="right", fontsize=8)
    ax12.axhline(0, color=GREY, linestyle=":", lw=1)
style_ax(ax12, "VIZ 12 · Profit Distribution by Market (Box Plot)", ylabel="Profit ($)")

# ─────────────────────────────────────────────
# 8. SAVE COMBINED IMAGE
# ─────────────────────────────────────────────
print("\n[8/9]  Saving combined visualisation image …")
IMG_PATH = os.path.join(OUT_DIR, "ecommerce_dashboard_visualizations.png")
fig.savefig(IMG_PATH, dpi=150, bbox_inches="tight", facecolor=BG, edgecolor="none")
plt.close(fig)
print(f"   ✓  Image saved → {IMG_PATH}")

# ─────────────────────────────────────────────
# 9. SAVE FINAL UNIFIED CLEANED CSV
# ─────────────────────────────────────────────
print("\n[9/9]  Saving final unified cleaned CSV …")

FINAL_CSV = os.path.join(OUT_DIR, "Global_Superstore_Final_Cleaned.csv")
df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")   # utf-8-sig for Excel compatibility

print(f"   ✓  Final CSV saved → {FINAL_CSV}")
print(f"   ✓  Shape : {df.shape[0]:,} rows × {df.shape[1]} columns")
print(f"\n   Columns in final CSV:")
for i, col in enumerate(df.columns, 1):
    print(f"   {i:2d}. {col}")

print("\n" + "=" * 65)
print("  PIPELINE COMPLETE")
print(f"  Final CSV   : Global_Superstore_Final_Cleaned.csv")
print(f"  Dashboard   : ecommerce_dashboard_visualizations.png")
print(f"  Output Dir  : {OUT_DIR}")
print("=" * 65)