# aggregate_table7_3.py
# ============================================================
# 第7表の3（通学者の常住地別流入状況）集計
#
# 目的:
#   - 23区への都外通学者（他道府県からの流入）の増減を集計する
#   - 豊島区に絞って埼玉・千葉・神奈川・その他の内訳を集計する
#
# 入力:
#   opendata/processed/fact_commuter_origin.csv  （第7表の3 縦結合済み）
#   opendata/processed/master_region.csv
#
# 出力:
#   opendata/processed/agg_outside_students_23ku.csv
#     - 23区の都外通学者増減（グラフ①用）
#   opendata/processed/agg_outside_students_toshima.csv
#     - 豊島区への都外通学者内訳（埼玉・千葉・神奈川・その他）（グラフ②用）
#
# データ構造の注意:
#   第7表の3は縦軸=通学地・横軸=常住地のクロス集計。
#   「通学地コード」が各区を指し、常住地は横持ちの列として展開されている。
#   都外通学者は「常住地が他道府県／総数（人）」列で集計する。
# ============================================================

import duckdb
from pathlib import Path

ORIGIN_PATH = "opendata/processed/fact_commuter_origin.csv"
MASTER_PATH = "opendata/processed/master_region.csv"
OUT1_PATH = Path("opendata/processed/agg_outside_students_23ku.csv")
OUT2_PATH = Path("opendata/processed/agg_outside_students_toshima.csv")

# ----------------------------------------------------------
# グラフ①用: 23区の都外通学者増減
# ----------------------------------------------------------
sql1 = """
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
        MAX(CASE WHEN year = 2015
            THEN "常住地が他道府県／総数（人）" END) AS 都外通学者_2015,
        MAX(CASE WHEN year = 2020
            THEN "常住地が他道府県／総数（人）" END) AS 都外通学者_2020
    FROM read_csv_auto(?, header=True) f
    JOIN ku ON CAST(f.通学地コード AS INTEGER) = CAST(ku.地域コード AS INTEGER)
    GROUP BY ku.地域
)
SELECT
    地域,
    都外通学者_2015,
    都外通学者_2020,
    都外通学者_2020 - 都外通学者_2015 AS 増減
FROM base
ORDER BY 増減 ASC
"""

df1 = duckdb.execute(sql1, [MASTER_PATH, ORIGIN_PATH]).df()
OUT1_PATH.parent.mkdir(parents=True, exist_ok=True)
df1.to_csv(OUT1_PATH, index=False)
print(f"出力完了: {OUT1_PATH}")
print(df1.to_string(index=False))
print()

# ----------------------------------------------------------
# グラフ②用: 豊島区への都外通学者内訳（埼玉・千葉・神奈川・その他）
#
# 「その他」= 都外通学者総数 - 埼玉 - 千葉 - 神奈川
# ----------------------------------------------------------
sql2 = """
SELECT
    year,
    "常住地が他道府県／関東地方／埼玉県（人）"  AS 埼玉県,
    "常住地が他道府県／関東地方／千葉県（人）"  AS 千葉県,
    "常住地が他道府県／関東地方／神奈川県（人）" AS 神奈川県,
    "常住地が他道府県／総数（人）"
        - "常住地が他道府県／関東地方／埼玉県（人）"
        - "常住地が他道府県／関東地方／千葉県（人）"
        - "常住地が他道府県／関東地方／神奈川県（人）" AS その他
FROM read_csv_auto(?, header=True)
WHERE 通学地 = '豊島区'
ORDER BY year ASC
"""

df2 = duckdb.execute(sql2, [ORIGIN_PATH]).df()
OUT2_PATH.parent.mkdir(parents=True, exist_ok=True)
df2.to_csv(OUT2_PATH, index=False)
print(f"出力完了: {OUT2_PATH}")
print(df2.to_string(index=False))
