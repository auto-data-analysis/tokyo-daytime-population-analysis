# aggregate_table2.py
# ============================================================
# 第2表（就業者・通学者・従業も通学もしない）集計
#
# 目的:
#   - 23区の昼間人口内訳（就業者・通学者・その他・15歳未満）を集計
#   - 第1表の昼間人口総数と突き合わせて15歳未満を推計
#   - 可視化用のCSVを出力する
#
# 入力:
#   opendata/processed/fact_population_by_status.csv  （第2表 縦結合済み）
#   opendata/processed/fact_daytime_population.csv    （第1表 縦結合済み）
#   opendata/processed/master_region.csv
#
# 出力:
#   opendata/processed/agg_status_23ku.csv
#     - 23区 × year × カテゴリ別昼間人口（15歳未満を含む4カテゴリ）
# ============================================================

import duckdb
from pathlib import Path

STATUS_PATH = "opendata/processed/fact_population_by_status.csv"
FACT1_PATH = "opendata/processed/fact_daytime_population.csv"
MASTER_PATH = "opendata/processed/master_region.csv"
OUTPUT_PATH = Path("opendata/processed/agg_status_23ku.csv")

# ----------------------------------------------------------
# DuckDB で集計
#
# 設計方針:
#   - 第2表は15歳以上のみのデータ。就業者＋通学者＋その他の合計が
#     昼間人口総数とは一致しない（差分＝15歳未満の昼間人口）。
#   - 第1表の昼間人口総数から第2表の合計を引くことで15歳未満を推計する。
#   - この4カテゴリで可視化することで、「就業者が増えても
#     15歳未満の減少で相殺された」という結論を図で示せる。
#   - ソート順は第1表の昼間人口増減に合わせる（記事内で図を並べたとき
#     読者が混乱しないように統一する）。
# ----------------------------------------------------------

sql = """
WITH
-- 23区マスタ
ku AS (
    SELECT 地域コード, 地域
    FROM read_csv_auto(?, header=True)
    WHERE 階層 = 2
      AND CAST(地域コード AS INTEGER) BETWEEN 13101 AND 13123
),

-- 第1表: 昼間人口総数（全年齢）
fact1 AS (
    SELECT 地域コード, year, "昼間人口／総数（人）" AS 昼間人口_総数
    FROM read_csv_auto(?, header=True)
),

-- 第2表: 就業者・通学者・その他（15歳以上）
fact2 AS (
    SELECT
        地域コード,
        year,
        "昼間人口／就業者（人）"            AS 就業者,
        "昼間人口／通学者（人）"            AS 通学者,
        "昼間人口／従業も通学もしない（人）" AS その他
    FROM read_csv_auto(?, header=True)
),

-- 内訳を結合し、15歳未満を推計
combined AS (
    SELECT
        ku.地域,
        f2.year,
        f2.就業者,
        f2.通学者,
        f2.その他,
        -- 第1表総数 - 第2表合計 = 15歳未満（15歳以上でも就業・通学・その他に
        -- 分類されない人が若干含まれる可能性があるが、誤差として扱う）
        f1.昼間人口_総数 - (f2.就業者 + f2.通学者 + f2.その他) AS 未満15歳
    FROM fact2 f2
    JOIN ku   ON f2.地域コード = ku.地域コード
    JOIN fact1 f1
      ON f2.地域コード = f1.地域コード
     AND f2.year       = f1.year
),

-- 第1表ベースの昼間人口増減を計算（ソート用）
sort_key AS (
    SELECT
        ku.地域,
        MAX(CASE WHEN f1.year = 2020 THEN f1.昼間人口_総数 END)
      - MAX(CASE WHEN f1.year = 2015 THEN f1.昼間人口_総数 END) AS 昼間人口_増減
    FROM fact1 f1
    JOIN ku ON f1.地域コード = ku.地域コード
    GROUP BY ku.地域
)

SELECT
    c.地域,
    c.year,
    c.就業者,
    c.通学者,
    c.その他,
    c.未満15歳,
    s.昼間人口_増減  -- ソート用（CSVには出力するが可視化では使わない）
FROM combined c
JOIN sort_key s ON c.地域 = s.地域
ORDER BY s.昼間人口_増減 ASC, c.year ASC
"""

df = duckdb.execute(sql, [MASTER_PATH, FACT1_PATH, STATUS_PATH]).df()

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

print(f"出力完了: {OUTPUT_PATH}")
print(f"レコード数: {len(df)}")
print(df.head(6).to_string(index=False))
