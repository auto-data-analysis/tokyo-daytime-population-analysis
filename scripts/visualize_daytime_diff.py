import duckdb
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pathlib import Path

# ============================================================
# 23区 昼間人口増減（2020-2015）横棒グラフ
# ============================================================

# --- 定数 ---
FACT_PATH = "opendata/processed/fact_daytime_population.csv"
MASTER_PATH = "opendata/processed/master_region.csv"
OUTPUT_DIR = Path("opendata/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Windowsの日本語フォント設定
plt.rcParams["font.family"] = "Meiryo"

TEXT_COLOR = "#595959"
COLOR_DECREASE = "#FFBBBB"  # 減少：ピンク系
COLOR_INCREASE = "#C5D7FB"  # 増加：ブルー系

# --- DuckDBで増減を集計 ---
sql = """
WITH base AS (
    SELECT
        f.year,
        m.地域,
        f.昼間人口／総数（人） AS 昼間人口
    FROM
        read_csv_auto(?, header=True) AS f
    JOIN
        read_csv_auto(?, header=True) AS m
        ON f.地域コード = m.地域コード
    WHERE
        m.階層 = 2
        AND m.地域コード BETWEEN 13101 AND 13123
)
SELECT
    a.地域,
    b.昼間人口 - a.昼間人口 AS 昼間人口_増減
FROM
    base a
JOIN
    base b
    ON a.地域 = b.地域
    AND a.year = 2015
    AND b.year = 2020
ORDER BY
    昼間人口_増減 ASC
"""

df = duckdb.execute(sql, [FACT_PATH, MASTER_PATH]).df()

# --- 可視化 ---
colors = [COLOR_DECREASE if v < 0 else COLOR_INCREASE for v in df["昼間人口_増減"]]

fig, ax = plt.subplots(figsize=(9, 7), dpi=150)

bars = ax.barh(df["地域"], df["昼間人口_増減"], color=colors)

# 数値ラベル
# マイナス値は左端に、プラス値は右端に配置し区名との重なりを避ける
max_val = df["昼間人口_増減"].abs().max()
for bar in bars:
    width = bar.get_width()
    if width < 0:
        # マイナス：バーの右側（ゼロ寄り）に表示して区名と重ならないようにする
        x_pos = max_val * 0.02
        ha = "left"
    else:
        # プラス：バーの右端の外側に表示
        x_pos = width + max_val * 0.01
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

# 装飾を最小限に（データインクレシオの考え方に基づく）
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax.set_xticks([])

# x軸の範囲：左側に区名分の余白を確保する
ax.set_xlim(
    df["昼間人口_増減"].min() * 1.5,
    df["昼間人口_増減"].max() * 1.2,
)

ax.set_title(
    "23区 昼間人口増減（2015→2020年）",
    fontsize=12,
    fontweight="bold",
    pad=12,
    color=TEXT_COLOR,
)

# 色の凡例
ax.text(0.02, 0.02, "■ 減少", transform=ax.transAxes, color=COLOR_DECREASE, fontsize=9)
ax.text(0.12, 0.02, "■ 増加", transform=ax.transAxes, color=COLOR_INCREASE, fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "daytime_population_diff.png", bbox_inches="tight")
plt.show()
print("保存完了: daytime_population_diff.png")
