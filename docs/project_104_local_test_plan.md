# Project 104 本地測試計畫

本文件旨在提供 `project_104` 相關 Producer 和 Task 的本地測試步驟，確保任務分發、Worker 執行及資料庫寫入流程正常運作。

## 測試前準備

1.  **確保所有 Docker 服務已啟動**：
    ```bash
    docker compose -f mysql-network.yml up -d
    docker compose -f rabbitmq-network.yml up -d
    ```

2.  **確保 `local.ini` 配置正確**：
    在 `local.ini` 的 `[DEV]` 區塊中，確保 `RABBITMQ_HOST` 和 `MYSQL_HOST` 都設定為 `127.0.0.1`，以便本地 Python 腳本能連接到 Docker 容器。

3.  **配置測試資料量限制 (可選)**：
    為了加快本地測試速度，你可以在 `local.ini` 的 `[DEV]` 區塊中配置以下參數，限制 Producer 分發的任務數量：
    *   `URL_PRODUCER_CATEGORY_LIMIT`: 限制 `producer_urls_104` 分發的類別數量。預設為 0 (不限制)。設定為正整數表示限制數量。
    *   `PRODUCER_BATCH_SIZE`: 限制 `producer_jobs_104` 每次從資料庫讀取並分發的職缺 URL 數量。
    ```ini
    [DEV]
    # ... 其他設定 ...
    URL_PRODUCER_CATEGORY_LIMIT = 2  # 例如，只分發 2 個類別的 URL 抓取任務
    PRODUCER_BATCH_SIZE = 10       # 例如，每次只處理 10 個職缺 URL
    # ... 其他設定 ...
    ```

4.  **啟動 Celery Worker**：
    在一個**獨立的終端視窗**中，啟動 Celery Worker。讓此視窗保持開啟，以便觀察 Worker 的日誌輸出。
    ```bash
     celery -A crawler.worker worker --loglevel=info
    ```

## 測試步驟

### 測試 `producer_category_104` (抓取職務類別)

1.  **執行 Producer**：
    在另一個終端視窗中執行以下命令：
    ```bash
     python -m crawler.project_104.producer_category_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `fetch_url_data_104` 任務被接收、執行，以及日誌中顯示類別資料被同步到資料庫的訊息。

3.  **驗證資料庫**：
    使用以下命令檢查 `tb_category_source` 表中是否有新的資料：
    ```bash
     python -m crawler.database.pandas_sql_config
    # 或者使用 temp_count_db.py 檢查數量
     python -m crawler.database.temp_count_db
    ```

### 測試 `producer_urls_104` (抓取職缺 URL)

1.  **執行 Producer**：
    在一個新的終端視窗中執行以下命令：
    ```bash
     python -m crawler.project_104.producer_urls_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `crawl_and_store_category_urls` 任務被接收、執行，以及日誌中顯示 URL 被儲存到資料庫的訊息。

3.  **驗證資料庫**：
    使用以下命令檢查 `tb_urls` 表中是否有新的 URL 資料：
    ```bash
     python -m crawler.database.pandas_sql_config
    # 或者使用 temp_count_db.py 檢查數量
     python -m crawler.database.temp_count_db
    ```

### 測試 `producer_jobs_104` (抓取職缺詳情)

**注意**：此測試依賴於 `producer_urls_104` 已經將足夠的未處理 URL 寫入 `tb_urls` 表。

1.  **執行 Producer**：
    在一個新的終端視窗中執行以下命令：
    ```bash
     python -m crawler.project_104.producer_jobs_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `fetch_url_data_104` 任務被接收、執行，以及日誌中顯示職缺詳情被儲存到資料庫的訊息。

3.  **驗證資料庫**：
    *   檢查 `tb_jobs` 表中是否有新的職缺詳情資料。
    *   檢查 `tb_urls` 表中對應的 URL 的 `details_crawl_status` 是否已更新為 `COMPLETED`。

## Celery Worker 命名與監控

### Worker 命名

Celery Worker 的標準命名格式是 `celery@hostname`。如果你希望自定義 Worker 的顯示名稱，可以使用 `-n` 或 `--hostname` 參數。

例如，為 `project_104` 專門啟動一個 Worker，並給它一個識別名稱：
```bash
 celery -A crawler.worker worker -n project_104_worker@%h --loglevel=info
```

**關於 `"project_104.{{.Task.Slot}}"` 這樣的動態命名**：
Celery Worker 的 `--hostname` 參數用於設定 Worker 實例的靜態名稱，不直接支援這種基於任務槽位的動態模板。這種模式通常與容器編排工具（如 Docker Swarm, Kubernetes）在部署多個 Worker 實例時，為每個容器或 Pod 動態生成主機名有關。在 Celery 層面，你主要透過**隊列 (Queues)** 來控制 Worker 處理哪些任務。

如果你想讓一個 Worker 專門處理 `project_104` 的任務，最常見且推薦的做法是讓它監聽 `project_104` 相關的隊列（例如 `urls_104`, `jobs_104`）。

例如，啟動一個只監聽 `urls_104` 和 `jobs_104` 隊列的 Worker：
```bash
 celery -A crawler.worker worker -Q urls_104,jobs_104 --loglevel=info
```

### 監控任務與 Worker 狀態

1.  **Celery Inspect 命令**：
    這些命令可以直接在終端中執行，用於查詢 Worker 的狀態和任務資訊。
    ```bash
    # 顯示所有活躍的 Worker
     celery -A crawler.worker inspect active_queues

    # 顯示所有活躍的任務（正在執行的任務）
     celery -A crawler.worker inspect active

    # 顯示所有已註冊的任務（Worker 知道的任務）
     celery -A crawler.worker inspect registered

    # 顯示所有排隊等待執行的任務
     celery -A crawler.worker inspect scheduled

    # 顯示所有被 Worker 預留但尚未執行的任務
     celery -A crawler.worker inspect reserved

    # 顯示 Worker 的統計資訊
     celery -A crawler.worker inspect stats
    ```

2.  **Flower UI**：
    Flower 是一個基於 Web 的 Celery 監控工具，提供更直觀的介面來查看 Worker、任務、隊列等狀態。你已經在 `rabbitmq-network.yml` 中配置了 Flower。
    *   確保 Flower 容器正在運行：
        ```bash
        docker compose -f rabbitmq-network.yml up -d
        ```
    *   在瀏覽器中訪問：`http://localhost:5555` (如果你的 Flower 端口映射是 5555)。
    *   在 Flower 介面中，你可以看到活躍的 Worker、任務的狀態、隊列的訊息數量等。

## 本地環境與 Docker 環境差異

### 1. 執行方式

在本地環境中，Producer 和 Worker 是直接透過 `python -m` 和 `celery -A` 命令在本地系統上執行。而在 Docker 環境中，這些服務則作為獨立的 Docker 容器運行，並由 Docker Compose 進行管理。

### 2. 環境變數與配置

本地環境主要依賴 `local.ini` 中的 `[DEV]` 區塊來獲取配置，例如 RabbitMQ 和 MySQL 的主機名通常設定為 `127.0.0.1`。Docker 環境則透過 `genenv.py` 腳本，根據 `local.ini` 中的 `[DOCKER]` 區塊生成 `.env` 檔案，供 Docker Compose 使用，其中服務主機名會設定為 Docker 網路中的服務名稱（例如 `rabbitmq`, `mysql`）。

### 3. 服務間通訊

在本地環境中，Python 腳本直接連接到本地運行或 Docker 容器映射到本地端口的 RabbitMQ 和 MySQL 服務。在 Docker 環境中，各服務（如 Producer, Worker, MySQL, RabbitMQ）透過 Docker 網路和服務名稱進行通訊，無需端口映射到本地。

### 4. 資料庫連接

本地環境中的 Python 腳本會連接到 `local.ini` 中 `[DEV]` 區塊設定的 `127.0.0.1` 上的 MySQL 服務。Docker 環境中的服務則會連接到 Docker 網路中的 MySQL 容器。
