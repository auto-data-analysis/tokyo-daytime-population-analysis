# visualize_table2.py
# ============================================================
# 第2表 可視化: 23区 昼間人口（15歳以上）増減（2020-2015）
#
# 目的:
#   - 就業者・通学者・従業も通学もしない の合計（15歳以上）の
#     増減を23区で横棒グラフで示す
#   - 「豊島区はむしろプラス」という事実を視覚化し、
#     リモートワーク仮説の棄却根拠とする
#
# 入力:
#   opendata/processed/agg_status_23ku.csv  （aggregate_table2.py の出力）
#
# 出力:
#   opendata/output/status_diff_23ku.png
#
# デザイン方針:
#   - 増加: #C5D7FB（ブルー系）、減少: #FFBBBB（ピンク系）
#   - 強調・ハイライトなし（豊島区の順位が際立てば十分）
#   - ソートはマイナスの多い順（増減値の昇順）
#   - データインクレシオに基づき装飾を最小限に
# ============================================================

import duckdb
import matplotlib.pyplot as plt
from pathlib import Path

AGG_PATH   = 'opendata/processed/agg_status_23ku.csv'
OUTPUT_DIR = Path('opendata/output')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams['font.family'] = 'Meiryo'

COLOR_INCREASE = '#C5D7FB'
COLOR_DECREASE = '#FFBBBB'
TEXT_COLOR     = '#595959'

# ----------------------------------------------------------
# DuckDBで差分を集計
#
# 第2表は15歳以上のデータ。就業者・通学者・その他の3カテゴリ合計の
# 2020-2015差分を区ごとに計算する。
# ソートはマイナスの多い順（昇順）にし、減少区が上に来るようにする。
# ----------------------------------------------------------
sql = """
WITH base AS (
    SELECT
        地域,
        MAX(CASE WHEN year = 2015 THEN 就業者  END) AS 就業者_2015,
        MAX(CASE WHEN year = 2020 THEN 就業者  END) AS 就業者_2020,
        MAX(CASE WHEN year = 2015 THEN 通学者  END) AS 通学者_2015,
        MAX(CASE WHEN year = 2020 THEN 通学者  END) AS 通学者_2020,
        MAX(CASE WHEN year = 2015 THEN その他  END) AS その他_2015,
        MAX(CASE WHEN year = 2020 THEN その他  END) AS その他_2020
    FROM read_csv_auto(?, header=True)
    GROUP BY 地域
)
SELECT
    地域,
    (就業者_2020  - 就業者_2015)
  + (通学者_2020  - 通学者_2015)
  + (その他_2020  - その他_2015) AS 増減_15歳以上
FROM base
ORDER BY 増減_15歳以上 ASC
"""
df = duckdb.execute(sql, [AGG_PATH]).df()

# ----------------------------------------------------------
# グラフ描画
# ----------------------------------------------------------
colors = [
    COLOR_DECREASE if v < 0 else COLOR_INCREASE
    for v in df['増減_15歳以上']
]

fig, ax = plt.subplots(figsize=(9, 7), dpi=150)
bars = ax.barh(df['地域'], df['増減_15歳以上'], color=colors)

# 数値ラベル
max_abs = df['増減_15歳以上'].abs().max()
for bar in bars:
    width = bar.get_width()
    if width < 0:
        x_pos = max_abs * 0.02
        ha = 'left'
    else:
        x_pos = width + max_abs * 0.01
        ha = 'left'
    ax.text(
        x_pos,
        bar.get_y() + bar.get_height() / 2,
        f'{int(width):+,}',
        va='center', ha=ha,
        fontsize=9, color=TEXT_COLOR,
    )

ax.axvline(x=0, color='lightgray', linewidth=0.8)

for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(left=False, bottom=False, colors=TEXT_COLOR)
ax.set_xticks([])

ax.set_xlim(
    df['増減_15歳以上'].min() * 1.3,
    df['増減_15歳以上'].max() * 1.2,
)

ax.set_title(
    '23区 昼間人口（15歳以上）増減（2015→2020年）',
    fontsize=12, fontweight='bold', pad=12, color=TEXT_COLOR,
)

ax.text(0.02, 0.02, '■ 減少', transform=ax.transAxes,
        color=COLOR_DECREASE, fontsize=9)
ax.text(0.12, 0.02, '■ 増加', transform=ax.transAxes,
        color=COLOR_INCREASE, fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'status_diff_23ku.png', bbox_inches='tight')
plt.show()
print('保存完了: status_diff_23ku.png')
