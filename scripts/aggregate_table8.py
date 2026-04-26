# aggregate_table8.py
# ============================================================
# 第8表（産業別就業者・自宅就業率）集計
#
# 目的:
#   - 23区の自宅就業率（2015→2020）の増加幅を集計する（グラフ①用）
#   - 豊島区・新宿区・渋谷区・台東区の産業中分類別昼間就業者増減を集計する
#     （グラフ②用）
#
# 入力:
#   opendata/processed/fact_industry_workers.csv  （第8表 縦結合済み）
#   opendata/processed/master_region.csv
#
# 出力:
#   opendata/processed/agg_homeworkrate_23ku.csv
#     - 23区の自宅就業率増加幅（グラフ①用）
#   opendata/processed/agg_industry_diff_4ku.csv
#     - 4区の産業中分類別昼間就業者増減（グラフ②用）
#
# データ上の注意:
#   - 産業名は平成27年が半角数字（第1次産業）、令和2年が全角数字（第１次産業）
#     と表記ゆれあり。産業分類並び順（数値列）でフィルタする。
#   - 自宅就業率 = 常住就業者／うち自宅就業者 ÷ 常住就業者（総数）× 100
# ============================================================

import duckdb
from pathlib import Path

INDUSTRY_PATH = "opendata/processed/fact_industry_workers.csv"
MASTER_PATH = "opendata/processed/master_region.csv"
OUT1_PATH = Path("opendata/processed/agg_homeworkrate_23ku.csv")
OUT2_PATH = Path("opendata/processed/agg_industry_diff_4ku.csv")

# ----------------------------------------------------------
# グラフ①用: 23区の自宅就業率増加幅
#
# 産業分類並び順=1（総数）・産業階層=0 で絞り、
# 自宅就業率 = 自宅就業者 ÷ 常住就業者 × 100 を計算する。
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
            THEN "常住就業者／うち自宅就業者（人）"
                 / "常住就業者（人）" * 100 END) AS 自宅就業率_2015,
        MAX(CASE WHEN year = 2020
            THEN "常住就業者／うち自宅就業者（人）"
                 / "常住就業者（人）" * 100 END) AS 自宅就業率_2020
    FROM read_csv_auto(?, header=True) f
    JOIN ku ON f.地域コード = ku.地域コード
    WHERE CAST(産業分類並び順 AS INTEGER) = 1
      AND CAST(産業階層 AS INTEGER) = 0
    GROUP BY ku.地域
)
SELECT
    地域,
    ROUND(自宅就業率_2015, 1) AS 自宅就業率_2015,
    ROUND(自宅就業率_2020, 1) AS 自宅就業率_2020,
    ROUND(自宅就業率_2020 - 自宅就業率_2015, 1) AS 増加幅_pt
FROM base
ORDER BY 増加幅_pt ASC
"""

df1 = duckdb.execute(sql1, [MASTER_PATH, INDUSTRY_PATH]).df()
OUT1_PATH.parent.mkdir(parents=True, exist_ok=True)
df1.to_csv(OUT1_PATH, index=False)
print(f"出力完了: {OUT1_PATH}")
print(df1.to_string(index=False))
print()

# ----------------------------------------------------------
# グラフ②用: 4区の産業中分類別昼間就業者増減
#
# 産業階層=2（中分類）で絞り、2020-2015の差分を計算する。
# 産業名の表記ゆれ（全角/半角数字）を避けるため、
# 産業分類並び順でフィルタし、2020年の産業名を正式名として使う。
# ----------------------------------------------------------
sql2 = """
WITH
target_ku AS (
    -- 豊島区・新宿区・渋谷区・台東区
    SELECT 地域コード, 地域
    FROM read_csv_auto(?, header=True)
    WHERE 地域コード IN ('13104', '13106', '13113', '13116')
),
base AS (
    SELECT
        ku.地域,
        f.産業分類並び順,
        MAX(CASE WHEN f.year = 2020 THEN f.産業 END) AS 産業名,  -- 2020年の表記を正式名とする
        MAX(CASE WHEN f.year = 2015 THEN f."昼間就業者（人）" END) AS 就業者_2015,
        MAX(CASE WHEN f.year = 2020 THEN f."昼間就業者（人）" END) AS 就業者_2020
    FROM read_csv_auto(?, header=True) f
    JOIN target_ku ku ON f.地域コード = ku.地域コード
    WHERE CAST(f.産業階層 AS INTEGER) = 2
    GROUP BY ku.地域, f.産業分類並び順
)
SELECT
    地域,
    産業分類並び順,
    産業名,
    就業者_2015,
    就業者_2020,
    就業者_2020 - 就業者_2015 AS 増減
FROM base
ORDER BY 地域, CAST(産業分類並び順 AS INTEGER)
"""

df2 = duckdb.execute(sql2, [MASTER_PATH, INDUSTRY_PATH]).df()
OUT2_PATH.parent.mkdir(parents=True, exist_ok=True)
df2.to_csv(OUT2_PATH, index=False)
print(f"出力完了: {OUT2_PATH}")
print(df2.to_string(index=False))
