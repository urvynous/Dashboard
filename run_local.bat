@echo off
REM Menjalankan proyek di Windows dengan Spark lokal.
REM   run_local.bat            -> 5 juta baris
REM   run_local.bat 1000000    -> versi ringan 1 juta baris
REM Catatan: menulis Parquet dengan Spark di Windows butuh winutils.exe + HADOOP_HOME.
REM Bila gagal, pakai Docker (docker compose) atau WSL. Dashboard tetap bisa dibuka
REM tanpa Spark karena tabel gold sudah disertakan di folder data\gold.
cd /d %~dp0
set ROWS=%1
if "%ROWS%"=="" set ROWS=5000000

java -version || (echo Java belum terpasang - butuh JDK 17 atau 21 & exit /b 1)

if not exist .venv (python -m venv .venv)
call .venv\Scripts\activate
pip install -q -r requirements.txt

python pipeline\run_pipeline.py --rows %ROWS%
streamlit run dashboard\app.py
