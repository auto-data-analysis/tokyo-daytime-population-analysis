# visualize_table7_3.py
# ============================================================
# 第7表の3 可視化:
#   グラフ①: 23区 都外通学者増減（2015→2020年）
#   グラフ②: 豊島区 都外通学者の内訳（2015 vs 2020）
#
# 目的:
#   - グラフ①: 豊島区の都外通学者減少が23区中4位であることを示す
#   - グラフ②: 埼玉県からの減少が突出していることを示す
#              （池袋＝埼玉アクセスが良い → 遠距離通学者のリモート化の示唆）
#
# 入力:
#   opendata/processed/agg_outside_students_23ku.csv   （aggregate_table7_3.py の出力）
#   opendata/processed/agg_outside_students_toshima.csv（同上）
#
# 出力:
#   opendata/output/outside_students_diff_23ku.png    （グラフ①）
#   opendata/output/outside_students_toshima.png      （グラフ②）
#
# デザイン方針:
#   グラフ①: 第2・3表グラフと設計を統一（差分・増減色分け）
#   グラフ②: Blueグラデーション4色で内訳を積み上げ
#             薄色=2015年（alpha=0.45）、濃色=2020年（alpha=1.0）
# ============================================================

import duckdb
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

AGG1_PATH = "opendata/processed/agg_outside_students_23ku.csv"
AGG2_PATH = "opendata/processed/agg_outside_students_toshima.csv"
OUTPUT_DIR = Path("opendata/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams["font.family"] = "Meiryo"

COLOR_INCREASE = "#C5D7FB"
COLOR_DECREASE = "#FFBBBB"
TEXT_COLOR = "#595959"

# グラフ②用 Blueグラデーション（デジタル庁スケール）
COLORS_BREAKDOWN = {
    "埼玉県": "#4979F5",  # Blue 500（最大・最濃）
    "神奈川県": "#8FAFF9",  # Blue 300相当
    "千葉県": "#C4D9FD",  # Blue 200相当
    "その他": "#E8F1FF",  # Blue 100相当（最小・最薄）
}


# ==========================================================
# グラフ①: 23区 都外通学者増減
# ==========================================================
sql1 = "SELECT 地域, 増減 FROM read_csv_auto(?, header=True) ORDER BY 増減 ASC"
df1 = duckdb.execute(sql1, [AGG1_PATH]).df()

colors1 = [COLOR_DECREASE if v < 0 else COLOR_INCREASE for v in df1["増減"]]

fig1, ax1 = plt.subplots(figsize=(9, 7), dpi=150)
bars1 = ax1.barh(df1["地域"], df1["増減"], color=colors1)

max_abs1 = df1["増減"].abs().max()
for bar in bars1:
    width = bar.get_width()
    if width < 0:
        x_pos = max_abs1 * 0.02
        ha = "left"
    else:
        x_pos = width + max_abs1 * 0.01
        ha = "left"
    ax1.text(
        x_pos,
        bar.get_y() + bar.get_height() / 2,
        f"{int(width):+,}",
        va="center",
        ha=ha,
        fontsize=9,
        color=TEXT_COLOR,
    )

ax1.axvline(x=0, color="lightgray", linewidth=0.8)
for spine in ax1.spines.values():
    spine.set_visible(False)
ax1.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax1.set_xticks([])
ax1.set_xlim(df1["増減"].min() * 1.3, df1["増減"].max() * 1.2)
ax1.set_title(
    "23区 都外通学者増減（2015→2020年）",
    fontsize=12,
    fontweight="bold",
    pad=12,
    color=TEXT_COLOR,
)
ax1.text(0.02, 0.02, "■ 減少", transform=ax1.transAxes, color=COLOR_DECREASE, fontsize=9)
ax1.text(0.12, 0.02, "■ 増加", transform=ax1.transAxes, color=COLOR_INCREASE, fontsize=9)

plt.tight_layout()
fig1.savefig(OUTPUT_DIR / "outside_students_diff_23ku.png", bbox_inches="tight")
plt.close(fig1)
print("保存完了: outside_students_diff_23ku.png")

# ==========================================================
# グラフ②: 豊島区 都外通学者の内訳（2015 vs 2020）
#
# 埼玉・神奈川・千葉・その他の4カテゴリを積み上げ横棒で表示。
# 埼玉を最初（左端・最濃色）に配置し、最大の減少を視覚的に強調する。
# ==========================================================
sql2 = "SELECT year, 埼玉県, 千葉県, 神奈川県, その他 FROM read_csv_auto(?, header=True) ORDER BY year ASC"
df2 = duckdb.execute(sql2, [AGG2_PATH]).df()

CATEGORIES2 = ["埼玉県", "神奈川県", "千葉県", "その他"]

BAR_HEIGHT = 0.5
fig2, ax2 = plt.subplots(figsize=(7, 3), dpi=150)

y_positions = [1.0, 0.3]  # 2020を上、2015を下
y_labels = ["2020年", "2015年"]

for year_val, y_pos in zip([2020, 2015], y_positions):
    row = df2[df2["year"] == year_val].iloc[0]
    left = 0
    for cat in CATEGORIES2:
        val = float(row[cat])
        ax2.barh(y_pos, val, left=left, height=BAR_HEIGHT, color=COLORS_BREAKDOWN[cat], linewidth=0)
        left += val
    # 総数ラベル
    total = sum(float(row[cat]) for cat in CATEGORIES2)
    ax2.text(
        total + 200,
        y_pos,
        f"{int(total):,}人",
        va="center",
        ha="left",
        fontsize=9,
        color=TEXT_COLOR,
    )

# y軸ラベル
ax2.set_yticks(y_positions)
ax2.set_yticklabels(y_labels, fontsize=10, color=TEXT_COLOR)

# 凡例
legend_handles = [mpatches.Patch(color=COLORS_BREAKDOWN[cat], label=cat) for cat in CATEGORIES2]
ax2.legend(
    handles=legend_handles,
    loc="lower right",
    bbox_to_anchor=(1.0, -0.25),
    ncol=4,
    fontsize=8,
    frameon=False,
)

for spine in ax2.spines.values():
    spine.set_visible(False)
ax2.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax2.set_xticks([])

ax2.set_title(
    "豊島区への都外通学者の内訳（2015年・2020年）",
    fontsize=12,
    fontweight="bold",
    pad=12,
    color=TEXT_COLOR,
)


plt.tight_layout()
fig2.savefig(OUTPUT_DIR / "outside_students_toshima.png", bbox_inches="tight")
plt.close(fig2)
print("保存完了: outside_students_toshima.png")
