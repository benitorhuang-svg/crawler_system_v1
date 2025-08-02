#   快速測試 本地端 task  <啟動 / 關閉>

## 設定環境
ENV=DEV python genenv.py

## 將 專案 套件化
uv pip install -e .

## 啟動資料庫
docker compose -f mysql-network.yml up -d
    
## project_104
@crawler/project_104/task_category_104.py @crawler/project_104/task_urls_104.py @crawler/project_104/task_jobs_104.py

```bash
python -m crawler.project_104.task_category_104
python -m crawler.project_104.task_urls_104
python -m crawler.project_104.task_jobs_104
```

## project_1111
@crawler/project_1111/task_category_1111.py @crawler/project_1111/task_urls_1111.py @crawler/project_1111/task_jobs_1111.py

```bash
python -m crawler.project_1111.task_category_1111
python -m crawler.project_1111.task_urls_1111
python -m crawler.project_1111.task_jobs_1111
```

## project_cakeresume
@crawler/project_cakeresume/task_category_cakeresume.py @crawler/project_cakeresume/task_urls_cakeresume.py @crawler/project_cakeresume/task_jobs_cakeresume.py

```bash
python -m crawler.project_cakeresume.task_category_cakeresume
python -m crawler.project_cakeresume.task_urls_cakeresume
python -m crawler.project_cakeresume.task_jobs_cakeresume
```

## project_yes123
@crawler/project_yes123/task_category_yes123.py @crawler/project_yes123/task_urls_yes123.py @crawler/project_yes123/task_jobs_yes123.py

```bash
python -m crawler.project_yes123.task_category_yes123
python -m crawler.project_yes123.task_urls_yes123
python -m crawler.project_yes123.task_jobs_yes123
```
