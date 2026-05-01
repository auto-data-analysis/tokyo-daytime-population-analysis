import pandas as pd
from pathlib import Path

# ============================================================
# 第8表 正規化スクリプト
# 平成27年・令和2年を統一列名でファクトテーブルに変換し縦結合する
#
# 【データ構造】
# 産業（大分類）× 地域 のクロステーブル（各年1501行）
# 産業階層: 0=総数 / 1=第1～3次産業 / 2=大分類 / 3=大分類内の細分
# 地域階層: 0=東京都総数 / 1=区部・市部等 / 2=区市町村
#
# 【両ファイルの構造上の違い】
# - 列名・内容は完全に同一（地域階層のゆれもなし）
# - 平成27年ファイル名: 「流出就業者」/ 令和2年ファイル名: 「流出通勤者」
#   （内容は同じ、ファイル名のみ異なる）
# ============================================================

# 地域階層・産業階層のフィルタ対象
VALID_REGION_LAYERS = {"0", "1", "2"}
VALID_INDUSTRY_LAYERS = {"0", "1", "2", "3"}  # 全産業階層を保持（集計時に絞る）

# 統一後の列順
FACT_COLS = [
    "産業分類並び順",
    "産業階層",
    "産業",
    "地域コード",
    "昼間就業者（人）",
    "常住就業者（人）",
    "常住就業者／うち自宅就業者（人）",
    "常住就業者／うち地域内通勤者（人）",
    "流入超過通勤者（人）",
    "流入通勤者（人）",
    "流入通勤者／うち都外から流入（人）",
    "流出通勤者（人）",
    "流出通勤者／うち都外へ流出（人）",
]


# ============================================================
# 共通の正規化処理
# ============================================================
def normalize(path, year):
    df = pd.read_csv(path, encoding="utf-8-sig")
    # 注記行の除去（地域階層・産業階層が対象外の行）
    df = df[df["地域階層"].astype(str).isin(VALID_REGION_LAYERS)].copy()
    df = df[df["産業階層"].astype(str).isin(VALID_INDUSTRY_LAYERS)].copy()
    # 地域コードの型変換
    df["地域コード"] = pd.to_numeric(df["地域コード"], errors="coerce")
    df = df[df["地域コード"].notna()].copy()
    df["地域コード"] = df["地域コード"].astype(int)
    # 数値列の型変換（"-"などの文字列をNaNに）
    for col in FACT_COLS[4:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    fact = df[FACT_COLS].copy()
    fact.insert(0, "year", year)
    return fact.reset_index(drop=True)


# ============================================================
# 平成27年・令和2年の正規化
# ============================================================
fact_h27 = normalize(
    "opendata/raw/平成27年第8表_地域_産業大分類別_昼間_常住就業者_流入_流出就業者(15歳以上).csv",
    2015,
)
fact_r2 = normalize(
    "opendata/raw/令和2年第8表_地域_産業大分類別_昼間_常住就業者_流入_流出通勤者(15歳以上).csv",
    2020,
)

# ============================================================
# 縦結合
# ============================================================
fact_all = pd.concat([fact_h27, fact_r2], ignore_index=True)

# --- 確認 ---
print("=== fact_industry_workers_2015 ===")
print("shape:", fact_h27.shape)
print(fact_h27.head(5).to_string())
print(f"\n欠損値:\n{fact_h27.isnull().sum()}")

print("\n=== fact_industry_workers_2020 ===")
print("shape:", fact_r2.shape)
print(fact_r2.head(5).to_string())
print(f"\n欠損値:\n{fact_r2.isnull().sum()}")

print("\n=== fact_industry_workers（縦結合後）===")
print("shape:", fact_all.shape)
print("year:", sorted(fact_all["year"].unique()))
print(f"\n欠損値:\n{fact_all.isnull().sum()}")

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

fact_h27.to_csv(output_dir / "fact_industry_workers_2015.csv", index=False, encoding="utf-8")
fact_r2.to_csv(output_dir / "fact_industry_workers_2020.csv", index=False, encoding="utf-8")
fact_all.to_csv(output_dir / "fact_industry_workers.csv", index=False, encoding="utf-8")
print("\n保存完了")
