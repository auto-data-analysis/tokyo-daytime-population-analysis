import pandas as pd
from pathlib import Path

# ============================================================
# 令和2年第1表 地域 昼間 常住 男女別人口
# ファクトテーブルに正規化する
# ※ master_region は令和2年を基準に作成する
# ============================================================

# --- 定数 ---
INPUT_PATH = "opendata/raw/令和2年第1表_地域_昼間_常住_男女別人口.csv"
VALID_LAYERS = {"0", "1", "2"}
YEAR = 2020

# --- 読み込み ---
df_raw = pd.read_csv(INPUT_PATH, encoding="utf-8-sig")

# --- データ行のみ抽出（注記行・ヘッダー混入行を除去）---
df = df_raw[df_raw["階層"].isin(VALID_LAYERS)].copy()

# --- 型変換 ---
df["地域コード"] = pd.to_numeric(df["地域コード"], errors="coerce").astype(int)
df["階層"] = df["階層"].astype(int)

num_cols = [
    "昼間人口／総数（人）",
    "昼間人口／男（人）",
    "昼間人口／女（人）",
    "常住人口／総数（人）",
    "常住人口／男（人）",
    "常住人口／女（人）",
    "昼夜間人口比率／総数（％）",
    "昼夜間人口比率／男（％）",
    "昼夜間人口比率／女（％）",
    "面積（平方キロメートル）",
    "昼間人口／人口密度（人／平方キロメートル）",
    "常住人口／人口密度（人／平方キロメートル）",
]
for col in num_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ============================================================
# ディメンションテーブル: master_region
# 地域コード・地域名・階層・面積
# → 年度によって変わらない地域の属性情報を切り出す
# ============================================================
master_region = df[["地域コード", "地域", "階層", "面積（平方キロメートル）"]].copy()
master_region = master_region.rename(columns={"面積（平方キロメートル）": "面積_平方km"})
master_region = master_region.reset_index(drop=True)

# ============================================================
# ファクトテーブル: fact_daytime_population_2020
# ============================================================
fact_cols = [
    "地域コード",
    "昼間人口／総数（人）",
    "昼間人口／男（人）",
    "昼間人口／女（人）",
    "常住人口／総数（人）",
    "常住人口／男（人）",
    "常住人口／女（人）",
    "昼夜間人口比率／総数（％）",
    "昼夜間人口比率／男（％）",
    "昼夜間人口比率／女（％）",
    "昼間人口／人口密度（人／平方キロメートル）",
    "常住人口／人口密度（人／平方キロメートル）",
]
fact_daytime = df[fact_cols].copy()
fact_daytime.insert(0, "year", YEAR)
fact_daytime = fact_daytime.reset_index(drop=True)

# --- 確認 ---
print("=== master_region ===")
print("shape:", master_region.shape)
print(master_region.head(3).to_string())
print(f"\n欠損値:\n{master_region.isnull().sum()}")

print("\n=== fact_daytime_population_2020 ===")
print("shape:", fact_daytime.shape)
print(fact_daytime.head(3).to_string())
print(f"\n欠損値:\n{fact_daytime.isnull().sum()}")

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

master_region.to_csv(output_dir / "master_region.csv", index=False, encoding="utf-8")
fact_daytime.to_csv(output_dir / "fact_daytime_population_2020.csv", index=False, encoding="utf-8")
print("\n保存完了: master_region.csv / fact_daytime_population_2020.csv")
