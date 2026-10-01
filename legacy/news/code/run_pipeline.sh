#!/bin/bash

echo "📍 Step 1: 뉴스 수집 + 본문 수집"

source ../venv/bin/activate
# python 1st_stock_graph.py
python 2nd_create_csv_with_link.py
python 3rd_add_content_in_csv.py
python extract_3rd_add_content_in_csv.py

echo "📍 Step 2: 뉴스 전처리"
python 4th_analysis.py

echo "📍 Step 3: metadata 생성"
python 5th_make_metadata.py

echo "✅ 전체 파이프라인 실행 완료!"
