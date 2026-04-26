# 東京都23区の昼間人口分析（2015→2020年）

コロナ禍で東京23区の昼間人口はどう変わったかを、国勢調査データをもとに分析したプロジェクトです。

DuckDB・Python・pandas・matplotlibを使い、前処理・集計・可視化・仮説検証を行っています。

## 分析結果

2015年→2020年で23区中4区（豊島区・練馬区・世田谷区・荒川区）の昼間人口が減少し、豊島区が減少数（-5,076人）・減少率（-1.2%）ともに23区中最大でした。

リモートワーク仮説を検証した結果これは棄却され、都外（主に埼玉県）からの遠距離通学者のリモート授業化と、娯楽・消費目的の来街者減少が複合的に影響したと示唆されます。

詳細はZenn記事をご覧ください。

- [前処理編](https://zenn.dev/cool_crocus678/articles/tokyo-daytime-population-preprocess)
- [集計・可視化編](https://zenn.dev/cool_crocus678/articles/tokyo-daytime-population-visualize)
- [仮説検証編](https://zenn.dev/cool_crocus678/articles/tokyo-daytime-population-hypothesis)

## フォルダ構成

```
tokyo-daytime-population-analysis/
├── raw/          # 東京都オープンデータからダウンロードしたCSV（ファイル名のみリネーム、データは加工なし）
├── processed/    # 正規化・縦結合済みCSV（スクリプト実行により生成）
├── scripts/      # 前処理・集計・可視化スクリプト（.py）
└── output/       # 可視化画像の出力先
```

## スクリプトの実行方法

プロジェクトルートから以下のように実行します。

```bash
python scripts/スクリプト名.py
```

実行順序は以下の通りです。

1. 前処理（`preprocess_*.py`）
2. 集計（`aggregate_*.py`）
3. 可視化（`visualize_*.py`）

## データソース

東京都オープンデータカタログサイト「令和2年国勢調査による東京都の昼間人口（従業地・通学地による人口）」

https://portal.data.metro.tokyo.lg.jp
https://catalog.data.metro.tokyo.lg.jp/dataset/t000003d0000000627
https://catalog.data.metro.tokyo.lg.jp/dataset/t000003d0000000036

出典：東京都「令和2年国勢調査による東京都の昼間人口（従業地・通学地による人口）」、クリエイティブ・コモンズ・ライセンス 表示4.0国際（https://creativecommons.org/licenses/by/4.0/deed.ja）