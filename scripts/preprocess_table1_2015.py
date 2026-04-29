import pandas as pd
from pathlib import Path

# ============================================================
# 平成27年第1表 地域 昼間 常住 男女別人口
# ファクトテーブルに正規化する
# ※ master_region は令和2年と共通のため作成しない
# ============================================================

# --- 定数 ---
INPUT_PATH = "opendata/raw/平成27年第1表_地域_昼間_常住_男女別人口.csv"
VALID_LAYERS = {"0", "1", "2"}
YEAR = 2015

# 令和2年と列名が異なる箇所を統一する
# 「地域階層」→「階層」
# 「面積（キロ平方メートル）」→「面積（平方キロメートル）」
# 人口密度の列名順が逆になっている
RENAME_MAP = {
    "地域階層": "階層",
    "面積（キロ平方メートル）": "面積（平方キロメートル）",
    "人口密度／昼間人口（人/キロ平方メートル）": "昼間人口／人口密度（人／平方キロメートル）",
    "人口密度／常住人口（人/キロ平方メートル）": "常住人口／人口密度（人／平方キロメートル）",
}

# --- 読み込み ---
df_raw = pd.read_csv(INPUT_PATH, encoding="utf-8-sig")

# --- 列名を統一 ---
df = df_raw.rename(columns=RENAME_MAP)

# --- データ行のみ抽出（注記行・ヘッダー混入行を除去）---
df = df[df["階層"].isin(VALID_LAYERS)].copy()

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
# ファクトテーブル: fact_daytime_population_2015
# 令和2年と同じ列構成にする
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
print("=== fact_daytime_population_2015 ===")
print("shape:", fact_daytime.shape)
print(fact_daytime.head(3).to_string())
print(f"\n欠損値:\n{fact_daytime.isnull().sum()}")

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

fact_daytime.to_csv(output_dir / "fact_daytime_population_2015.csv", index=False)
print("\n保存完了: fact_daytime_population_2015.csv")
