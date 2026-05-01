# visualize_table8.py
# ============================================================
# 第8表 可視化:
#   グラフ①: 23区 自宅就業率増加幅（2015→2020年）
#   グラフ②: 豊島区・新宿区・渋谷区・台東区 産業別昼間就業者増減
#
# 目的:
#   グラフ①:「豊島区の自宅就業率増加幅は23区中で中程度」を示し
#            リモートワーク仮説の棄却根拠とする
#   グラフ②: 産業構成の違いが昼間人口増減の原因ではないことを確認する
#
# 入力:
#   opendata/processed/agg_homeworkrate_23ku.csv   （aggregate_table8.py の出力）
#   opendata/processed/agg_industry_diff_4ku.csv   （同上）
#
# 出力:
#   opendata/output/homeworkrate_diff_23ku.png     （グラフ①）
#   opendata/output/industry_diff_4ku.png          （グラフ②）
#
# デザイン方針:
#   グラフ①: 他表と統一（増減色分け・ソートASC）
#             自宅就業率増加幅は全区プラスなのでBlue単色
#   グラフ②: 4区を縦に並べた4パネル構成
#             産業ごとに増減色分け、産業名を各パネルで共有
# ============================================================

import duckdb
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path

AGG1_PATH = "opendata/processed/agg_homeworkrate_23ku.csv"
AGG2_PATH = "opendata/processed/agg_industry_diff_4ku.csv"
OUTPUT_DIR = Path("opendata/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams["font.family"] = "Meiryo"

COLOR_INCREASE = "#C5D7FB"
COLOR_DECREASE = "#FFBBBB"
COLOR_SINGLE = "#C5D7FB"  # グラフ①は全区プラスなので単色
TEXT_COLOR = "#595959"

# ==========================================================
# グラフ①: 23区 自宅就業率増加幅
# ==========================================================
sql1 = "SELECT 地域, 増加幅_pt FROM read_csv_auto(?, header=True) ORDER BY 増加幅_pt ASC"
df1 = duckdb.execute(sql1, [AGG1_PATH]).df()

fig1, ax1 = plt.subplots(figsize=(9, 7), dpi=150)
bars1 = ax1.barh(df1["地域"], df1["増加幅_pt"], color=COLOR_SINGLE)

max_val1 = df1["増加幅_pt"].max()
for bar in bars1:
    width = bar.get_width()
    ax1.text(
        width + max_val1 * 0.01,
        bar.get_y() + bar.get_height() / 2,
        f"+{width:.1f}pt",
        va="center",
        ha="left",
        fontsize=9,
        color=TEXT_COLOR,
    )

ax1.axvline(x=0, color="lightgray", linewidth=0.8)
for spine in ax1.spines.values():
    spine.set_visible(False)
ax1.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax1.set_xticks([])
ax1.set_xlim(0, max_val1 * 1.25)
ax1.set_title(
    "23区 自宅就業率 増加幅（2015→2020年）",
    fontsize=12,
    fontweight="bold",
    pad=12,
    color=TEXT_COLOR,
)

plt.tight_layout()
fig1.savefig(OUTPUT_DIR / "homeworkrate_diff_23ku.png", bbox_inches="tight")
plt.close(fig1)
print("保存完了: homeworkrate_diff_23ku.png")

# ==========================================================
# グラフ②: 4区の産業別昼間就業者増減（4パネル）
#
# 産業名が長いため横棒グラフを採用し、区ごとに1パネルを割り当てる。
# 産業の並びはデータの産業分類並び順（国勢調査の標準順）に従う。
# 各パネルで産業名を共有し、左端パネルのみy軸ラベルを表示する。
# ==========================================================
sql2 = """
SELECT 地域, 産業名, 増減
FROM read_csv_auto(?, header=True)
ORDER BY 地域, CAST(産業分類並び順 AS INTEGER)
"""
df2 = duckdb.execute(sql2, [AGG2_PATH]).df()

# 区の表示順: 豊島区を先頭に置き注目させる
KU_ORDER = ["豊島区", "新宿区", "渋谷区", "台東区"]
industries = df2[df2["地域"] == "豊島区"]["産業名"].tolist()  # 産業順序はデータ順

fig2, axes = plt.subplots(1, 4, figsize=(16, 7), dpi=150, sharey=True)

for ax, ku in zip(axes, KU_ORDER):
    df_ku = df2[df2["地域"] == ku].copy()
    colors = [COLOR_DECREASE if v < 0 else COLOR_INCREASE for v in df_ku["増減"]]

    ax.barh(df_ku["産業名"], df_ku["増減"], color=colors, height=0.65)

    # 数値ラベル
    max_abs = df_ku["増減"].abs().max()
    for _, row in df_ku.iterrows():
        val = row["増減"]
        if val < 0:
            x_pos = max_abs * 0.03
            ha = "left"
        else:
            x_pos = val + max_abs * 0.02
            ha = "left"
        ax.text(
            x_pos,
            row["産業名"],
            f"{int(val):+,}",
            va="center",
            ha=ha,
            fontsize=6.5,
            color=TEXT_COLOR,
        )

    ax.axvline(x=0, color="lightgray", linewidth=0.8)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
    ax.set_xticks([])

    # x軸範囲を各パネルで統一（最大絶対値に合わせる）
    global_max = df2["増減"].abs().max()
    ax.set_xlim(-global_max * 1.1, global_max * 1.3)

    ax.set_title(ku, fontsize=11, fontweight="bold", pad=8, color=TEXT_COLOR)

# y軸ラベルは sharey=True で左端パネルのみ表示される
axes[0].tick_params(axis="y", labelsize=8, colors=TEXT_COLOR)

# 色の凡例（右端パネル外側）
axes[-1].text(
    1.02,
    0.02,
    "■ 減少",
    transform=axes[-1].transAxes,
    color=COLOR_DECREASE,
    fontsize=8,
    va="bottom",
)
axes[-1].text(
    1.02,
    0.07,
    "■ 増加",
    transform=axes[-1].transAxes,
    color=COLOR_INCREASE,
    fontsize=8,
    va="bottom",
)

fig2.suptitle(
    "産業別 昼間就業者増減（2015→2020年）",
    fontsize=12,
    fontweight="bold",
    y=1.01,
    color=TEXT_COLOR,
)

plt.tight_layout()
fig2.savefig(OUTPUT_DIR / "industry_diff_4ku.png", bbox_inches="tight")
plt.close(fig2)
print("保存完了: industry_diff_4ku.png")
