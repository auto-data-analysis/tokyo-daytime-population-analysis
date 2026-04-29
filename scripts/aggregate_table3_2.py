# aggregate_table3_2.py
# ============================================================
# 第3表の2（昼間通学者の増減）集計
#
# 目的:
#   - 23区の昼間通学者総数の2020-2015差分を集計する
#   - 第2表で「15歳以上の昼間人口は増加」と示した後、
#     「では通学者に絞ると豊島区はどうか」という問いに答える
#
# 入力:
#   opendata/processed/fact_commuter_students.csv  （第3表の2 縦結合済み）
#   opendata/processed/master_region.csv
#
# 出力:
#   opendata/processed/agg_commuter_students_23ku.csv
#     - 23区の昼間通学者増減（2020-2015）
# ============================================================

import duckdb
from pathlib import Path

STUDENTS_PATH = "opendata/processed/fact_commuter_students.csv"
MASTER_PATH = "opendata/processed/master_region.csv"
OUTPUT_PATH = Path("opendata/processed/agg_commuter_students_23ku.csv")

# ----------------------------------------------------------
# DuckDB で集計
#
# 設計方針:
#   - 第3表の2は全年齢の通学者データ（第2表の15歳以上と対比させる）
#   - 昼間通学者総数の差分のみを集計する（流入・流出の内訳は別途第7表で扱う）
#   - ソートはプラスが小さい順（ASC）で第2表グラフと設計を統一する
# ----------------------------------------------------------

sql = """
WITH
ku AS (
    SELECT 地域コード, 地域
    FROM read_csv_auto(?, header=True)
    WHERE 階層 = 2
      AND CAST(地域コード AS INTEGER) BETWEEN 13101 AND 13123
),
base AS (
    SELECT
        ku.地域,
        MAX(CASE WHEN year = 2015 THEN "昼間通学者／総数（人）" END) AS 昼間通学者_2015,
        MAX(CASE WHEN year = 2020 THEN "昼間通学者／総数（人）" END) AS 昼間通学者_2020
    FROM read_csv_auto(?, header=True) f
    JOIN ku ON f.地域コード = ku.地域コード
    GROUP BY ku.地域
)
SELECT
    地域,
    昼間通学者_2015,
    昼間通学者_2020,
    昼間通学者_2020 - 昼間通学者_2015 AS 増減
FROM base
ORDER BY 増減 ASC
"""

df = duckdb.execute(sql, [MASTER_PATH, STUDENTS_PATH]).df()

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

print(f"出力完了: {OUTPUT_PATH}")
print(f"レコード数: {len(df)}")
print(df.to_string(index=False))
