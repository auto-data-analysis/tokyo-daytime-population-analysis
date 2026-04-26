import pandas as pd
from pathlib import Path

# ============================================================
# ファクトテーブルの縦結合
# fact_daytime_population_2015.csv
# fact_daytime_population_2020.csv
# → fact_daytime_population.csv
# ============================================================

# --- 定数 ---
INPUT_DIR = Path("opendata/processed")
OUTPUT_DIR = Path("opendata/processed")

FILES = {
    2015: INPUT_DIR / "fact_daytime_population_2015.csv",
    2020: INPUT_DIR / "fact_daytime_population_2020.csv",
}

# --- 読み込み ---
dfs = []
for year, path in FILES.items():
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"{year}年 shape: {df.shape}")
    dfs.append(df)

# --- 縦結合 ---
df_all = pd.concat(dfs, ignore_index=True)

# --- 確認 ---
print("\n=== fact_daytime_population（縦結合後）===")
print("shape:", df_all.shape)
print("year の値:", sorted(df_all["year"].unique()))
print(f"\n欠損値:\n{df_all.isnull().sum()}")
print()
print(df_all.head(3).to_string())

# --- 保存 ---
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
df_all.to_csv(OUTPUT_DIR / "fact_daytime_population.csv", index=False)
print("\n保存完了: fact_daytime_population.csv")
