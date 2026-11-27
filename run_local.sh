#!/usr/bin/env bash
# Menjalankan seluruh proyek di laptop (Linux / macOS / WSL) dengan Spark lokal.
#   ./run_local.sh            -> 5 juta baris
#   ./run_local.sh 1000000    -> versi ringan 1 juta baris
set -e
cd "$(dirname "$0")"
ROWS="${1:-5000000}"

echo ">> Memeriksa Java ..."
java -version 2>&1 | head -n1 || { echo "Java belum terpasang (butuh JDK 17 atau 21)"; exit 1; }

if [ ! -d ".venv" ]; then
  echo ">> Membuat virtual environment .venv ..."
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r requirements.txt

echo ">> Menjalankan pipeline Spark ($ROWS baris) ..."
python pipeline/run_pipeline.py --rows "$ROWS"

echo ">> Membuka dashboard di http://localhost:8501 (Ctrl+C untuk berhenti)"
streamlit run dashboard/app.py
