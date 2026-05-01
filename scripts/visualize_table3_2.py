# visualize_table3_2.py
# ============================================================
# 第3表の2 可視化: 23区 昼間通学者増減（2015→2020年）
#
# 目的:
#   - 昼間通学者の増減を23区で横棒グラフで示す
#   - 第2表（15歳以上の昼間人口はプラス）との対比として、
#     通学者に絞ると豊島区が上位の減少区に浮上することを示す
#
# 入力:
#   opendata/processed/agg_commuter_students_23ku.csv
#     （aggregate_table3_2.py の出力）
#
# 出力:
#   opendata/output/commuter_students_diff_23ku.png
#
# デザイン方針:
#   - 第2表グラフと設計を統一（色・ソート・装飾）
#   - 増加: #C5D7FB（ブルー系）、減少: #FFBBBB（ピンク系）
#   - データインクレシオに基づき装飾を最小限に
# ============================================================

import duckdb
import matplotlib.pyplot as plt
from pathlib import Path

AGG_PATH = "opendata/processed/agg_commuter_students_23ku.csv"
OUTPUT_DIR = Path("opendata/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams["font.family"] = "Meiryo"

COLOR_INCREASE = "#C5D7FB"
COLOR_DECREASE = "#FFBBBB"
TEXT_COLOR = "#595959"

# ----------------------------------------------------------
# データ読み込み
# ----------------------------------------------------------
sql = """
SELECT 地域, 増減
FROM read_csv_auto(?, header=True)
ORDER BY 増減 ASC
"""
df = duckdb.execute(sql, [AGG_PATH]).df()

# ----------------------------------------------------------
# グラフ描画
# ----------------------------------------------------------
colors = [COLOR_DECREASE if v < 0 else COLOR_INCREASE for v in df["増減"]]

fig, ax = plt.subplots(figsize=(9, 7), dpi=150)
bars = ax.barh(df["地域"], df["増減"], color=colors)

# 数値ラベル
max_abs = df["増減"].abs().max()
for bar in bars:
    width = bar.get_width()
    if width < 0:
        x_pos = max_abs * 0.02
        ha = "left"
    else:
        x_pos = width + max_abs * 0.01
        ha = "left"
    ax.text(
        x_pos,
        bar.get_y() + bar.get_height() / 2,
        f"{int(width):+,}",
        va="center",
        ha=ha,
        fontsize=9,
        color=TEXT_COLOR,
    )

# ゼロ基準線
ax.axvline(x=0, color="lightgray", linewidth=0.8)

# 装飾を最小限に
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax.set_xticks([])

ax.set_xlim(
    df["増減"].min() * 1.3,
    df["増減"].max() * 1.2,
)

ax.set_title(
    "23区 昼間通学者増減（2015→2020年）",
    fontsize=12,
    fontweight="bold",
    pad=12,
    color=TEXT_COLOR,
)

ax.text(0.02, 0.02, "■ 減少", transform=ax.transAxes, color=COLOR_DECREASE, fontsize=9)
ax.text(0.12, 0.02, "■ 増加", transform=ax.transAxes, color=COLOR_INCREASE, fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "commuter_students_diff_23ku.png", bbox_inches="tight")
plt.show()
print("保存完了: commuter_students_diff_23ku.png")
