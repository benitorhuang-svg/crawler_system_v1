# Project yes123 本地測試計畫

本文件旨在提供 `project_yes123` 相關 Producer 和 Task 的本地測試步驟，確保任務分發、Worker 執行及資料庫寫入流程正常運作。

## 測試前準備

1.  **確保所有 Docker 服務已啟動**：
    ```bash
    docker compose -f mysql-network.yml up -d
    docker compose -f rabbitmq-network.yml up -d
    ```

2.  **確保 `local.ini` 配置正確**：
    在 `local.ini` 的 `[DEV]` 區塊中，確保 `RABBITMQ_HOST` 和 `MYSQL_HOST` 都設定為 `127.0.0.1`，以便本地 Python 腳本能連接到 Docker 容器。

3.  **啟動 Celery Worker**：
    在一個**獨立的終端視窗**中，啟動 Celery Worker。讓此視窗保持開啟，以便觀察 Worker 的日誌輸出。
    ```bash
     celery -A crawler.worker worker --loglevel=info
    ```

## 測試 Task (自動使用 test_db)

本專案中的所有 `task_*.py` 檔案都已內建本地測試模式。當你直接執行這些檔案時，它們會自動將資料庫連線指向 `test_db`，確保測試不會影響到正式的開發資料庫 (`crawler_db`)。

### 測試 `task_category_yes123.py`

1.  **執行任務**：
    ```bash
    python -m crawler.project_yes123.task_category_yes123
    ```
2.  **觀察日誌**：日誌會顯示 `Connecting to database: test_db@...`，並記錄後續的抓取與同步過程。
3.  **驗證資料庫**：你可以連線到 `test_db` 來驗證 `tb_category_source` 表中是否已寫入 yes123 的類別資料。

### 測試 `task_urls_yes123.py`

1.  **執行任務**：
    ```bash
    python -m crawler.project_yes123.task_urls_yes123
    ```
2.  **觀察日誌**：日誌會顯示從 `test_db` 讀取類別，然後抓取 URL 並存入 `test_db` 的過程。
3.  **驗證資料庫**：驗證 `test_db` 中的 `tb_urls` 和 `tb_url_categories` 表是否已寫入資料。

### 測試 `task_jobs_yes123.py`

1.  **執行任務**：
    ```bash
    python -m crawler.project_yes123.task_jobs_yes123
    ```
2.  **觀察日誌**：日誌會顯示從 `test_db` 讀取待處理的 URL，抓取職缺詳情，並將結果存回 `test_db`。
3.  **驗證資料庫**：驗證 `test_db` 中的 `tb_jobs` 表是否已寫入資料，以及 `tb_urls` 表的 `details_crawl_status` 是否已更新。

## 測試 Producer (使用正式 crawler_db)

Producer 的職責是與正式的 `crawler_db` 互動，產生任務並發送到 RabbitMQ。測試 Producer 時，我們通常會驗證它是否能正確地將任務發送給 Worker。

### 測試 `producer_category_yes123`

1.  **執行 Producer**：
    ```bash
    python -m crawler.project_yes123.producer_category_yes123
    ```
2.  **觀察 Worker 日誌**：回到 Celery Worker 的終端視窗，觀察是否有 `fetch_and_sync_yes123_categories` 任務被接收並成功執行。

### 測試 `producer_urls_yes123`

1.  **執行 Producer**：
    ```bash
    python -m crawler.project_yes123.producer_urls_yes123
    ```
2.  **觀察 Worker 日誌**：觀察是否有 `crawl_and_store_yes123_category_urls` 任務被接收並成功執行。

### 測試 `producer_jobs_yes123`

1.  **執行 Producer**：
    ```bash
    python -m crawler.project_yes123.producer_jobs_yes123
    ```
2.  **觀察 Worker 日誌**：觀察是否有 `fetch_url_data_yes123` 任務被接收並成功執行。

## 監控任務與 Worker 狀態

你可以使用 Flower UI 或 Celery 的命令列工具來監控任務和 Worker 的狀態。

-   **Flower UI**: 訪問 `http://localhost:5555`
-   **Celery Inspect**: 
    ```bash
    # 顯示活躍的任務
    celery -A crawler.worker inspect active

    # 顯示排隊中的任務
    celery -A crawler.worker inspect scheduled
    ```
