import duckdb
import pandas as pd
from pathlib import Path

# ============================================================
# DuckDBで昼間人口の集計
# 対象：23区（階層=2、地域コード 13101〜13123）
# ============================================================

# --- 定数 ---
FACT_PATH = "opendata/processed/fact_daytime_population.csv"
MASTER_PATH = "opendata/processed/master_region.csv"

# --- DuckDBで集計 ---
# CSVを直接読み込んでSQLで集計できるのがDuckDBの強み
# pandasのDataFrameに変換せずそのまま処理できる
sql = """
SELECT
    f.year,
    m.地域コード,
    m.地域,
    f.昼間人口／総数（人）    AS 昼間人口,
    f.常住人口／総数（人）    AS 常住人口,
    f.昼夜間人口比率／総数（％） AS 昼夜間比率
FROM
    read_csv_auto(?, header=True) AS f
JOIN
    read_csv_auto(?, header=True) AS m
    ON f.地域コード = m.地域コード
WHERE
    m.階層 = 2
    AND m.地域コード BETWEEN 13101 AND 13123
ORDER BY
    f.year,
    f.昼間人口／総数（人） DESC
"""

df = duckdb.execute(sql, [FACT_PATH, MASTER_PATH]).df()

# --- 確認 ---
print("=== 23区 昼間人口集計 ===")
print("shape:", df.shape)
print(df.to_string())

# --- 平成27年→令和2年の増減を計算 ---
sql_diff = """
WITH base AS (
    SELECT
        f.year,
        m.地域コード,
        m.地域,
        f.昼間人口／総数（人） AS 昼間人口,
        f.常住人口／総数（人） AS 常住人口
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
    a.地域コード,
    a.地域,
    a.昼間人口                              AS 昼間人口_2015,
    b.昼間人口                              AS 昼間人口_2020,
    b.昼間人口 - a.昼間人口                 AS 昼間人口_増減,
    ROUND(
        (b.昼間人口 - a.昼間人口)
        / a.昼間人口 * 100, 1
    )                                       AS 昼間人口_増減率,
    a.常住人口                              AS 常住人口_2015,
    b.常住人口                              AS 常住人口_2020,
    b.常住人口 - a.常住人口                 AS 常住人口_増減
FROM
    base a
JOIN
    base b
    ON a.地域コード = b.地域コード
    AND a.year = 2015
    AND b.year = 2020
ORDER BY
    昼間人口_増減 ASC
"""

df_diff = duckdb.execute(sql_diff, [FACT_PATH, MASTER_PATH]).df()

print("\n=== 昼間人口 増減ランキング（少ない順）===")
print(df_diff.to_string())

# --- 保存 ---
output_dir = Path("opendata/processed")
df.to_csv(output_dir / "agg_daytime_population_23ku.csv", index=False, encoding="utf-8")
df_diff.to_csv(
    output_dir / "agg_daytime_population_diff.csv", index=False, encoding="utf-8"
)
print("\n保存完了")
