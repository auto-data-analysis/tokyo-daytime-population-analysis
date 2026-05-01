import pandas as pd
from pathlib import Path

# ============================================================
# 第2表 正規化スクリプト
# 平成27年・令和2年を統一列名でファクトテーブルに変換し縦結合する
# ============================================================

# --- 定数 ---
VALID_LAYERS = {"0", "1", "2"}

# 平成27年→統一列名のマッピング
RENAME_H27 = {
    "地域階層": "階層",
    "昼間人口／総数／（人）": "昼間人口／総数（人）",
    "昼間人口／就業者(15歳以上)（人）": "昼間人口／就業者（人）",
    "昼間人口／従業も通学もしない(人)": "昼間人口／従業も通学もしない（人）",
    "常住人口／総数(人）": "常住人口／総数（人）",
    "常住人口／就業者(15歳以上)(人）": "常住人口／就業者（人）",
    "常住人口／従業も通学もしない(人)": "常住人口／従業も通学もしない（人）",
}

# 令和2年→統一列名のマッピング
# ※令和2年は「夜間人口」という表記だが内容は常住人口と同じ
RENAME_R2 = {
    "昼間人口／総数／うち就業者（人）": "昼間人口／就業者（人）",
    "昼間人口／総数／うち通学者（人）": "昼間人口／通学者（人）",
    "昼間人口／総数／うち従業も通学もしない（人）": "昼間人口／従業も通学もしない（人）",
    "夜間人口／総数（人）": "常住人口／総数（人）",
    "夜間人口／総数／うち就業者（人）": "常住人口／就業者（人）",
    "夜間人口／総数／うち通学者（人）": "常住人口／通学者（人）",
    "夜間人口／総数／うち従業も通学もしない（人）": "常住人口／従業も通学もしない（人）",
}

# 統一後の列順
FACT_COLS = [
    "地域コード",
    "昼間人口／総数（人）",
    "昼間人口／就業者（人）",
    "昼間人口／通学者（人）",
    "昼間人口／従業も通学もしない（人）",
    "常住人口／総数（人）",
    "常住人口／就業者（人）",
    "常住人口／通学者（人）",
    "常住人口／従業も通学もしない（人）",
]

# ============================================================
# 平成27年の正規化
# ============================================================
df_h27_raw = pd.read_csv(
    "opendata/raw/平成27年第2表_地域_昼間_常住_就業者(15歳以上)_通学者状態別人口.csv",
    encoding="utf-8-sig",
)

df_h27 = df_h27_raw.rename(columns=RENAME_H27)
df_h27 = df_h27[df_h27["階層"].isin(VALID_LAYERS)].copy()
df_h27["地域コード"] = pd.to_numeric(df_h27["地域コード"], errors="coerce").astype(int)

for col in FACT_COLS[1:]:
    df_h27[col] = pd.to_numeric(df_h27[col], errors="coerce")

fact_h27 = df_h27[FACT_COLS].copy()
fact_h27.insert(0, "year", 2015)
fact_h27 = fact_h27.reset_index(drop=True)

# ============================================================
# 令和2年の正規化
# ============================================================
df_r2_raw = pd.read_csv(
    "opendata/raw/令和2年第2表_地域_昼間_常住_就業者(15歳以上)_通学者状態別人口.csv",
    encoding="utf-8-sig",
)

df_r2 = df_r2_raw.rename(columns=RENAME_R2)
df_r2["地域コード"] = pd.to_numeric(df_r2["地域コード"], errors="coerce")

# 注記行（地域コードがNaN）を除去
df_r2 = df_r2[df_r2["地域コード"].notna()].copy()
df_r2["地域コード"] = df_r2["地域コード"].astype(int)

for col in FACT_COLS[1:]:
    df_r2[col] = pd.to_numeric(df_r2[col], errors="coerce")

fact_r2 = df_r2[FACT_COLS].copy()
fact_r2.insert(0, "year", 2020)
fact_r2 = fact_r2.reset_index(drop=True)

# ============================================================
# 縦結合
# ============================================================
fact_all = pd.concat([fact_h27, fact_r2], ignore_index=True)

# --- 確認 ---
print("=== fact_population_by_status_2015 ===")
print("shape:", fact_h27.shape)
print(fact_h27.head(3).to_string())
print(f"\n欠損値:\n{fact_h27.isnull().sum()}")

print("\n=== fact_population_by_status_2020 ===")
print("shape:", fact_r2.shape)
print(fact_r2.head(3).to_string())
print(f"\n欠損値:\n{fact_r2.isnull().sum()}")

print("\n=== fact_population_by_status（縦結合後）===")
print("shape:", fact_all.shape)
print("year:", sorted(fact_all["year"].unique()))
print(f"\n欠損値:\n{fact_all.isnull().sum()}")

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

fact_h27.to_csv(output_dir / "fact_population_by_status_2015.csv", index=False, encoding="utf-8")
fact_r2.to_csv(output_dir / "fact_population_by_status_2020.csv", index=False, encoding="utf-8")
fact_all.to_csv(output_dir / "fact_population_by_status.csv", index=False, encoding="utf-8")
print("\n保存完了")
