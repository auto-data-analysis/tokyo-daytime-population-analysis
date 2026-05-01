import pandas as pd
from pathlib import Path

# ============================================================
# 第7表の3 正規化スクリプト
# 平成27年・令和2年を縦持ちファクトテーブルに変換し縦結合する
#
# 【データ構造】
# 縦軸: 従業地・通学地（行）× 横軸: 常住地（列）のクロス集計
# → melt()で縦持ちに変換し、通学地コード×常住地コードの組み合わせを1行に
#
# 【地域階層の定義（注記より）】
# 0: 全国 / 1: 東京都以外の道府県 / 2: 関東地方
# 3: 都道府県 / 4: 区部・市部・郡部・島部 / 5: 区市町村
#
# 【両ファイルの構造上の違い】
# - 平成27年: 「地域階層」列
# - 令和２年 : 「階層」列
# ============================================================

# 通学地（行）として残す地域階層
# 分析の主対象は23区（階層=5）と集計行（3,4）
# 注記行（NaN行）は地域コードのNaNフィルタで除去
VALID_STUDY_LAYERS = {"3", "4", "5"}

# 常住地（列）として残す列
# 東京都内の区市＋都外（他道府県総数・関東各県）に絞る
# 分析で特に重要な都外通学者（埼玉・千葉・神奈川）を含む
KEEP_COLS = [
    "通学地コード",
    "通学地",
    "常住地／全国総数（人）",
    "常住地／東京都総数（人）",
    "常住地／区部（人）",
    "常住地／千代田区（人）",
    "常住地／中央区（人）",
    "常住地／港区（人）",
    "常住地／新宿区（人）",
    "常住地／文京区（人）",
    "常住地／台東区（人）",
    "常住地／墨田区（人）",
    "常住地／江東区（人）",
    "常住地／品川区（人）",
    "常住地／目黒区（人）",
    "常住地／大田区（人）",
    "常住地／世田谷区（人）",
    "常住地／渋谷区（人）",
    "常住地／中野区（人）",
    "常住地／杉並区（人）",
    "常住地／豊島区（人）",
    "常住地／北区（人）",
    "常住地／荒川区（人）",
    "常住地／板橋区（人）",
    "常住地／練馬区（人）",
    "常住地／足立区（人）",
    "常住地／葛飾区（人）",
    "常住地／江戸川区（人）",
    "常住地が他道府県／総数（人）",
    "常住地が他道府県／関東地方／総数（人）",
    "常住地が他道府県／関東地方／茨城県（人）",
    "常住地が他道府県／関東地方／栃木県（人）",
    "常住地が他道府県／関東地方／群馬県（人）",
    "常住地が他道府県／関東地方／埼玉県（人）",
    "常住地が他道府県／関東地方／千葉県（人）",
    "常住地が他道府県／関東地方／神奈川県（人）",
    "常住地が他道府県／他の道府県（人）",
]


# ============================================================
# 共通の正規化処理
# ============================================================
def normalize(path, layer_col, year):
    df = pd.read_csv(path, encoding="utf-8-sig")

    # 列名ゆれ吸収（「地域階層」→「階層」に統一）
    df = df.rename(columns={layer_col: "階層"})

    # 地域コードがNaNの行（注記行・集計不能行）を除去
    df["地域コード"] = pd.to_numeric(df["地域コード"], errors="coerce")
    df = df[df["地域コード"].notna()].copy()

    # 通学地として対象の階層に絞る
    df = df[df["階層"].astype(str).isin(VALID_STUDY_LAYERS)].copy()

    # 地域コードをint変換・地域名から「従業地・通学地／」プレフィックスを除去
    df["地域コード"] = df["地域コード"].astype(int)
    df["地域"] = df["地域"].str.replace("従業地・通学地／", "", regex=False)

    # 通学地の列名を整理
    df = df.rename(columns={"地域コード": "通学地コード", "地域": "通学地"})

    # 数値列の型変換（"-"・"…"などをNaNに）
    value_cols = [c for c in KEEP_COLS if c not in ("通学地コード", "通学地")]
    for col in value_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    fact = df[KEEP_COLS].copy()
    fact.insert(0, "year", year)
    return fact.reset_index(drop=True)


# ============================================================
# 平成27年・令和2年の正規化
# ============================================================
fact_h27 = normalize(
    "opendata/raw/平成27年第7表の3地域相互間の昼夜間移動状況_通学者.csv",
    layer_col="地域階層",
    year=2015,
)
fact_r2 = normalize(
    "opendata/raw/令和2年第7表の3地域相互間の昼夜間移動状況_通学者.csv",
    layer_col="階層",
    year=2020,
)

# ============================================================
# 縦結合
# ============================================================
fact_all = pd.concat([fact_h27, fact_r2], ignore_index=True)

# --- 確認 ---
print("=== fact_commuter_origin_2015 ===")
print("shape:", fact_h27.shape)
print(fact_h27.head(5).to_string())
print(f"\n欠損値:\n{fact_h27.isnull().sum()}")

print("\n=== fact_commuter_origin_2020 ===")
print("shape:", fact_r2.shape)
print(fact_r2.head(5).to_string())
print(f"\n欠損値:\n{fact_r2.isnull().sum()}")

print("\n=== fact_commuter_origin（縦結合後）===")
print("shape:", fact_all.shape)
print("year:", sorted(fact_all["year"].unique()))
print(f"\n欠損値:\n{fact_all.isnull().sum()}")

# --- 豊島区の都外通学者確認（分析の主要論点）---
print("\n=== 豊島区（13116）の都外通学者確認 ===")
toshima = fact_all[fact_all["通学地コード"] == 13116][
    [
        "year",
        "通学地",
        "常住地が他道府県／総数（人）",
        "常住地が他道府県／関東地方／埼玉県（人）",
        "常住地が他道府県／関東地方／千葉県（人）",
        "常住地が他道府県／関東地方／神奈川県（人）",
    ]
]
print(toshima.to_string())

# --- 保存 ---
output_dir = Path("opendata/processed")
output_dir.mkdir(parents=True, exist_ok=True)

fact_h27.to_csv(output_dir / "fact_commuter_origin_2015.csv", index=False, encoding="utf-8")
fact_r2.to_csv(output_dir / "fact_commuter_origin_2020.csv", index=False, encoding="utf-8")
fact_all.to_csv(output_dir / "fact_commuter_origin.csv", index=False, encoding="utf-8")
print("\n保存完了")
