#   快速測試 本地端 task  <啟動 / 關閉>

## 設定環境
ENV=DEV python genenv.py

## 將 專案 套件化
uv pip install -e .

## 啟動資料庫
docker compose -f mysql-network.yml up -d

## 資料庫表格初始化與資料遷移

**重要提示**：資料庫表格會自動建立（如果不存在），因為 `initialize_database()` 會在每個 `task_*.py` 腳本首次運行時被呼叫。

如果您有舊的資料在 `tb_jobs` 和 `tb_job_locations` 中，請執行以下腳本將其遷移到新的平台特定表格：

```bash
python database_migration.py
```

## 任務執行

**重要提示**：為了實現依據指定 category 順序呼叫 task URL，您需要手動修改各個 `producer_urls_*.py` 腳本，讓它們導入並使用 `crawler/utils/category_sorter.py` 中的 `get_sorted_categories` 函數來獲取排序後的類別列表，然後依照這個順序產生 URL 任務。

### project_104
@crawler/project_104/task_category_104.py @crawler/project_104/task_urls_104.py @crawler/project_104/task_jobs_104.py

```bash
python -m crawler.project_104.task_category_104
python -m crawler.project_104.task_urls_104
python -m crawler.project_104.task_jobs_104
```

### project_1111
@crawler/project_1111/task_category_1111.py @crawler/project_1111/task_urls_1111.py @crawler/project_1111/task_jobs_1111.py

```bash
python -m crawler.project_1111.task_category_1111
python -m crawler.project_1111.task_urls_1111
python -m crawler.project_1111.task_jobs_1111
```

### project_cakeresume
@crawler/project_cakeresume/task_category_cakeresume.py @crawler/project_cakeresume/task_urls_cakeresume.py @crawler/project_cakeresume/task_jobs_cakeresume.py

```bash
python -m crawler.project_cakeresume.task_category_cakeresume
python -m crawler.project_cakeresume.task_urls_cakeresume
python -m crawler.project_cakeresume.task_jobs_cakeresume
```

### project_yes123
@crawler/project_yes123/task_category_yes123.py @crawler/project_yes123/task_urls_yes123.py @crawler/project_yes123/task_jobs_yes123.py

```bash
python -m crawler.project_yes123.task_category_yes123
python -m crawler.project_yes123.task_urls_yes123
python -m crawler.project_yes123.task_jobs_yes123
```