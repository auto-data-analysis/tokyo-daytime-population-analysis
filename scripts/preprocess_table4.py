import pandas as pd
from pathlib import Path

# ============================================================
# 第4表 正規化スクリプト
# 平成27年・令和2年を統一列名でファクトテーブルに変換し縦結合する
#
# 【両ファイルの構造上の違い】
# - 平成27年: 「地域階層」列
# - 令和２年 : 「階層」列
# - 列名・内容はそれ以外ほぼ同一
# ============================================================

VALID_LAYERS = {"0", "1", "2"}

# 平成27年のみ列名ゆれを吸収（「地域階層」→「階層」）
RENAME_H27 = {
    "地域階層": "階層",
}

# 統一後の列順
# 増減数・増減率は含めない（year列の差分でDuckDBから算出可能）
# 「うち15歳以上」は通学者の年齢内訳として分析上有用なため残す
FACT_COLS = [
    "地域コード",
    # 流入
    "流入人口／総数（人）",
    "流入人口／都内他地域から（人）",
    "流入人口／他道府県から（人）",
    "流入人口／通勤者／総数（人）",
    "流入人口／通勤者／都内他地域から（人）",
    "流入人口／通勤者／他道府県から（人）",
    "流入人口／通学者／総数（人）",
    "流入人口／通学者／総数／うち15歳以上(人)",
    "流入人口／通学者／都内他地域から（人）",
    "流入人口／通学者／都内他地域から／うち15歳以上(人)",
    "流入人口／通学者／他道府県から（人）",
    "流入人口／通学者／他道府県から／うち15歳以上(人)",
    # 流出
    "流出人口／総数（人）",
    "流出人口／都内他地域へ（人）",
    "流出人口／他道府県へ（人）",
    "流出人口／通勤者／総数（人）",
    "流出人口／通勤者／都内他地域へ（人）",
    "流出人口／通勤者／他道府県へ（人）",
    "流出人口／通学者／総数（人）",
    "流出人口／通学者／総数／うち15歳以上(人)",
    "流出人口／通学者／都内他地域へ（人）",
    "流出人口／通学者／都内他地域へ／うち15歳以上(人)",
    "流出人口／通学者／他道府県へ（人）",
    "流出人口／通学者／他道府県へ／うち15歳以上(人)",
    # 流入超過
    "流入超過人口／総数（人）",
    "流入超過人口／都内他地域（人）",
    "流入超過人口／他道府県（人）",
    "流入超過人口／通勤者／総数（人）",
    "流入超過人口／通勤者／都内他地域（人）",
    "流入超過人口／通勤者／他道府県（人）",
    "流入超過人口／通学者／総数（人）",
    "流入超過人口／通学者／総数／うち15歳以上(人)",
    "流入超過人口／通学者／都内他地域（人）",
    "流入超過人口／通学者／都内他地域／うち15歳以上(人)",
    "流入超過人口／通学者／他道府県（人）",
    "流入超過人口／通学者／他道府県／うち15歳以上(人)",
]


# ============================================================
# 共通の正規化処理
# ============================================================
def normalize(path, rename_map, year):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = df.rename(columns=rename_map)
    # 注記行の除去（階層が"0"/"1"/"2"以外 または 地域コードがNaN）
    df = df[df["階層"].astype(str).isin(VALID_LAYERS)].copy()
    df["地域コード"] = pd.to_numeric(df["地域コード"], errors="coerce")
    df = df[df["地域コード"].notna()].copy()
    df["地域コード"] = df["地域コード"].astype(int)
    # 数値列の型変換（"-"などの文字列をNaNに）
    for col in FACT_COLS[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    fact = df[FACT_COLS].copy()
    fact.insert(0, "year", year)
    return fact.reset_index(drop=True)


# ============================================================
# 平成27年・令和2年の正規化
# ============================================================
fact_h27 = normalize(
    "opendata/raw/平成27年第4表_地域_流入_流出先_通勤者(15歳以上)_通学者状態別人口.csv",
    RENAME_H27,
    2015,
)
fact_r2 = normalize(
    "opendata/raw/令和2年第4表_地域_流入_流出先_通勤者(15歳以上)_通学者状態別人口.csv",
    {},  # 令和2年は列名ゆれなし
    2020,
)

# ============================================================
# 縦結合
# ============================================================
fact_all = pd.concat([fact_h27, fact_r2], ignore_index=True)

# --- 確認 ---
print("=== fact_flow_population_2015 ===")
print("shape:", fact_h27.shape)
print(fact_h27.head(3).to_string())
print(f"\n欠損値:\n{fact_h27.isnull().sum()}")

print("\n=== fact_flow_population_2020 ===")
print("shape:", fact_r2.shape)
print(fact_r2.head(3).to_string())
print(f"\n欠損値:\n{fact_r2.isnull().sum()}")

print("\n=== fact_flow_population（縦結合後）===")
print("shape:", fact_all.shape)
print("year:", sorted(fact_all["year"].unique()))
print(f"\n欠損値:\n{fact_all.isnull().sum()}")

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

fact_h27.to_csv(output_dir / "fact_flow_population_2015.csv", index=False)
fact_r2.to_csv(output_dir / "fact_flow_population_2020.csv", index=False)
fact_all.to_csv(output_dir / "fact_flow_population.csv", index=False)
print("\n保存完了")
