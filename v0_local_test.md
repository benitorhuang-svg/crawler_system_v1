#   快速測試 本地端 task  <啟動 / 關閉>

## 設定環境
ENV=DEV python genenv.py

## 將 專案 套件化
uv pip install -e .

## 啟動資料庫
docker compose -f mysql.yml up -d
    
# 測試指令 project_104
    python -m crawler.project_104.task_category_104
    python -m crawler.project_104.task_urls_104
    python -m crawler.project_104.task_jobs_104

# 測試指令 project_yes123
    python -m crawler.project_yes123.task_category_yes123
    python -m crawler.project_yes123.task_urls_yes123
    python -m crawler.project_yes123.task_jobs_yes123

# 測試指令 project_1111
    python -m crawler.project_1111.task_category_1111
    python -m crawler.project_1111.task_urls_1111
    python -m crawler.project_1111.task_jobs_1111

# 測試指令 project_cakeresume
    python -m crawler.project_cakeresume.task_category_cakeresume
    python -m crawler.project_cakeresume.task_urls_cakeresume
    python -m crawler.project_cakeresume.task_jobs_cakeresume