# Crawler System 開發手冊

## 1. 總體哲學 (Philosophy)

本專案所有開發與重構工作，應遵循以下核心哲學：

- **清晰性 (Clarity)**：程式碼首先是寫給人看的，其次才是給機器執行的。優先選擇清晰、易於理解的寫法，避免過度炫技或使用晦澀的語法。
- **單一職責 (Single Responsibility)**：每個模組、每個類別、每個函式都應該只有一個明確的職責。這使得程式碼更容易測試、重用和維護。
- **穩定性 (Robustness)**：應用程式應具備容錯能力，並透過**嚴格的測試**來確保其穩定性。對於外部依賴（如資料庫、訊息佇列），必須有適當的重試和錯誤處理機制。
- **配置外部化 (Externalized Configuration)**：程式碼本身不應包含任何環境特定的設定（如密碼、主機位址）。所有設定都應透過外部設定檔管理。

---

## 2. 環境設定 (Environment Setup)

### 2.1. 必要工具

- **Python**: 版本定義於 `.python-version`。
- **uv**: 用於管理 Python 虛擬環境和套件。
- **Docker & Docker Compose**: 用於啟動外部服務（MySQL, RabbitMQ）。

### 2.2. 初始化步驟

1.  **初始化專案 (使用 uv)**:
    ```bash
    uv init
    ```

2.  **啟動基礎服務**:
    ```bash
    # 啟動 MySQL 和 RabbitMQ 服務
    docker-compose -f mysql-network.yml up -d
    docker-compose -f rabbitmq-network.yml up -d
    ```

3.  **建立虛擬環境並安裝依賴**:
    ```bash
    # 建立 .venv 虛擬環境
    uv venv

    # 啟用虛擬環境
    source .venv/bin/activate

    # 安裝專案依賴
    uv pip install -r requirements.txt
    ```

3.  **設定環境變數**:
    複製 `local.ini.example` (如果有的話) 為 `local.ini`，並根據本地開發需求修改。`APP_ENV` 環境變數用於切換不同的設定區塊。

---

## 3. 設定檔管理 (Configuration)

- **`local.ini`**: 這是唯一的設定來源 (Single Source of Truth)。它被分為不同的區塊，例如 `[DEV]`, `[DOCKER]`, `[PROD]`。
- **`crawler/config.py`**: 這是讀取 `local.ini` 的唯一模組。專案中任何其他地方需要設定值時，都應該**直接從 `crawler.config` 匯入**，而不是自己重新讀取 `.ini` 檔案。
- **`APP_ENV` 環境變數**: 這個環境變數決定了 `config.py` 要讀取 `local.ini` 中的哪一個區塊。預設為 `DOCKER`。

### 3.1. 核心爬蟲設定

以下是 `local.ini` 中一些影響爬蟲行為的關鍵設定：

-   **`PRODUCER_BATCH_SIZE`**:
    -   **作用**: 限制 Producer (例如 `producer_jobs_104`) 每次從資料庫讀取並分發的任務數量。
    -   **用途**: 在本地測試時，可以設定較小的值 (例如 `20`)，以控制單次測試的資料量，避免一次性處理過多任務。在生產環境中，可以根據系統資源和任務量設定較大的值。

-   **`PRODUCER_DISPATCH_INTERVAL_SECONDS`**:
    -   **作用**: 設定**持續運行**的 Producer (使用 `while True` 迴圈) 在分發完一批任務後，等待多久才開始下一批。
    -   **用途**: 用於控制常駐型 Producer 的任務分發頻率。對於**排程驅動、單次執行**的 Producer (如目前的 `producer_jobs_104`)，此設定**無效**，因為腳本執行完畢後即會終止。

-   **`URL_CRAWLER_SLEEP_MIN_SECONDS` / `URL_CRAWLER_SLEEP_MAX_SECONDS`**:
    -   **作用**: 定義 Worker 在每次發送 API 請求前，隨機暫停的最小和最大秒數。
    -   **用途**: 這是**避免被目標網站 API 封鎖的關鍵設定**。透過引入隨機延遲，模擬人類的瀏覽行為，降低請求頻率，有效規避 `429 Too Many Requests` 等反爬機制。在測試或生產環境中，應根據目標網站的限制策略進行調整。

---

## 4. 核心編碼原則 (Core Coding Principles)

### 4.1. 單一職責原則 (SRP)

- **模組層級**: `crawler/database/connection.py` 只負責資料庫連接，`crawler/config.py` 只負責設定讀取。
- **函式層級**: 一個函式只做一件事情。例如，`get_engine` 只負責取得引擎，而不應該包含建立資料庫的邏輯。

### 4.2. DRY (Don't Repeat Yourself)

- **避免重複程式碼**: 如果一段邏輯在兩個以上的地方出現，就應該將它抽像成一個函式或類別。
- **範例**: 專案中所有讀取設定的地方都應從 `crawler.config` 匯入，這就是 DRY 的體現。

### 4.3. 日誌記錄 (Logging)

- **使用 `structlog`**: 全面使用 `structlog` 進行結構化日誌記錄，而不是 `print()`。
- **日誌級別**:
    - `logger.debug()`: 用於開發時的詳細除錯資訊。
    - `logger.info()`: 用於記錄關鍵的業務流程節點（例如「服務啟動」、「收到新任務」）。
    - `logger.warning()`: 用於記錄可預期的、但需要注意的異常情況（例如「設定檔缺少某個非關鍵值，使用預設值」）。
    - `logger.error()`: 用於記錄發生了錯誤，但應用程式仍可繼續運行的情況（例如「處理單一任務失敗，但 worker 會繼續接收下一個任務」）。
    - `logger.critical()`: 用於記錄導致應用程式無法繼續運行的致命錯誤（例如「資料庫連接失敗」）。
- **包含上下文**: 在記錄日誌時，盡可能帶上關鍵的上下文資訊，例如 `logger.info("任務處理完成", task_id=123, duration_ms=500)`。

---

## 5. 測試策略 (Testing Strategy)

本專案高度重視程式碼品質與穩定性，因此測試是開發流程中不可或缺的一環。所有程式碼在提交前都必須經過適當的測試。

### 5.1. 測試類型與目的

-   **單元測試 (Unit Tests)**：
    -   **目的**：驗證程式碼中最小可測試單元（如函式、方法）的行為是否符合預期。
    -   **特點**：隔離外部依賴（透過 Mocking），確保測試的快速、可靠和可重複性。
    -   **範圍**：針對核心業務邏輯、數據處理、轉換函式等。

-   **整合測試 (Integration Tests)**：
    -   **目的**：驗證不同模組或服務之間協同工作的正確性，以及與外部系統（如資料庫、訊息佇列、外部 API）的互動是否正常。
    -   **特點**：通常需要啟動部分或全部依賴服務。
    -   **範圍**：資料庫操作、API 客戶端、任務分派與執行流程等。

-   **端到端測試 (End-to-End Tests)**：
    -   **目的**：模擬真實用戶場景，驗證整個系統從輸入到輸出的完整流程是否正確。
    -   **特點**：通常在接近生產的環境中運行，涉及所有組件。
    -   **範圍**：從 Producer 啟動到數據最終存入資料庫的完整爬取流程。

### 5.2. 測試工具

-   **`pytest`**: 作為主要的測試框架，提供豐富的功能和靈活的擴展性。
-   **`unittest.mock`**: 用於單元測試中模擬外部依賴，確保測試的隔離性。

### 5.3. 測試流程與規範

1.  **編寫測試**：為新功能或修復的 Bug 編寫相應的測試案例。
2.  **運行測試**：在提交程式碼前，必須運行所有相關測試，並確保其通過。
    ```bash
    # 運行所有測試 (如果配置正確)
    python -m pytest

    # 運行特定測試檔案
    python -m pytest tests/path/to/your_test_file.py
    ```
3.  **測試覆蓋率**：鼓勵提高測試覆蓋率，但更重要的是測試的品質和有效性。
4.  **測試環境**：單元測試應盡可能在隔離的環境中運行，不依賴外部服務。整合測試和端到端測試則需要適當的環境配置。

---

## 6. 資料庫互動 (Database Interaction)

本專案**強制使用 Pydantic 模型**來定義和處理所有與資料庫互動的資料結構，以確保資料的類型安全、一致性與自動驗證。

### 5.1. ORM 與 Session 管理

-   **使用 `get_session`**: 所有對資料庫的讀寫操作，都必須透過 `crawler.database.connection.get_session` 的上下文管理器來完成。
    ```python
    from crawler.database.connection import get_session
    from crawler.database.models import MyDataPydantic # 假設這是你的 Pydantic 模型

    with get_session() as session:
        # 從資料庫讀取資料後，應立即轉換為 Pydantic 模型
        orm_object = session.query(MyORMModel).first()
        if orm_object:
            pydantic_instance = MyDataPydantic.model_validate(orm_object)
            # 現在你可以安全地使用 pydantic_instance

        # 寫入資料庫時，應使用 Pydantic 模型定義的資料
        new_data = MyDataPydantic(field1="value1", field2="value2")
        session.add(MyORMModel(**new_data.model_dump())) # 將 Pydantic 轉換為 ORM 可接受的格式
        # session.commit() 和 session.rollback() 會由 get_session 自動處理
    ```
-   **禁止直接使用 `engine.execute()`**: 除非是像 `initialize_database` 這樣的一次性管理腳本，否則業務邏輯中應避免直接使用 `engine`。
-   **Pydantic 的優勢**: 
    -   **類型安全**: 明確定義資料類型，減少運行時錯誤。
    -   **資料驗證**: 自動驗證輸入資料是否符合預期結構和類型。
    -   **清晰的資料結構**: 讓程式碼更易於理解和維護。
    -   **與 API 整合**: Pydantic 模型可以輕鬆地用於定義 RESTful API 的請求和響應模型。

### 5.2. 爬取狀態生命週期 (Crawl Status Lifecycle)

為了確保任務不被重複抓取且具備重試能力，URL 的爬取狀態遵循以下生命週期：

1.  **`PENDING`**:
    -   **定義**: URL 已被收集，等待 Producer 分發。這是 URL 的初始狀態。
    -   **觸發**: `producer_category_104` 或 `producer_urls_104` 首次將 URL 存入資料庫時。

2.  **`QUEUED`**:
    -   **定義**: Producer 已從資料庫讀取此 URL，並準備將其作為任務發送到訊息佇列 (RabbitMQ)。
    -   **觸發**: `producer_jobs_104` 讀取到 `PENDING` 或 `FAILED` 的 URL 後，會**立即**將其狀態更新為 `QUEUED`，以防止其他 Producer 實例重複選取。

3.  **`PROCESSING`**:
    -   **定義**: Worker 已從訊息佇列接收到任務，正在進行資料抓取和處理。
    -   **觸發**: `worker.py` 中的 `fetch_url_data_104` 任務開始執行時，會將 URL 狀態更新為 `PROCESSING`。

4.  **`SUCCESS`**:
    -   **定義**: Worker 已成功完成資料抓取和儲存。
    -   **觸發**: `fetch_url_data_104` 任務成功執行完畢。

5.  **`FAILED`**:
    -   **定義**: Worker 在處理過程中遇到錯誤 (例如 API 請求失敗、資料驗證錯誤)。
    -   **觸發**: `fetch_url_data_104` 任務執行期間發生異常。失敗的任務將在未來的某個時間點由 Producer 重新選取並分發。

這個狀態機能夠確保系統的穩定性和資料處理的原子性。

### 5.3. 透過 Pandas 直接連線資料庫 (僅限讀取或特定用途)

在某些特定場景下，例如進行資料分析或快速查詢時，你可能希望直接透過 Pandas 連線到資料庫。此時，你可以使用 `sqlalchemy` 的 `create_engine` 搭配專案的設定來建立連接。

**注意**：這種方式通常用於讀取資料，對於寫入操作，仍建議使用 `get_session` 和 ORM，並透過 Pydantic 模型來確保事務的完整性和一致性。

1.  **安裝必要套件**: 
    ```bash
    uv pip install pandas sqlalchemy mysql-connector-python
    ```

2.  **範例程式碼**: 
    ```python
    import pandas as pd
    from sqlalchemy import create_engine
    from crawler.config import (
        MYSQL_HOST,
        MYSQL_PORT,
        MYSQL_ACCOUNT,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
    )

    # 建立資料庫連接 URL
    # 使用 mysql+pymysql 驅動
    db_url = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 使用 pandas 讀取資料表
        # 替換 'your_table_name' 為你實際的資料表名稱
        df = pd.read_sql("SELECT * FROM your_table_name", engine)

        # 顯示 DataFrame 的前幾行
        print(df.head())

    except Exception as e:
        print(f"連線或查詢時發生錯誤: {e}")

    finally:
        # 關閉引擎連接池
        engine.dispose()
    ```

---

## 6. 執行爬蟲 (Running the Crawler)

為了確保 Python 的 `import` 路徑正確，應從專案根目錄使用 `-m` 參數來執行模組。

- **啟動 Producer**:
  ```bash
  APP_ENV=DEV python -m crawler.project_104.producer_category_104
  ```
- **啟動 Worker**:
  ```bash
  APP_ENV=DEV celery -A crawler.worker worker --loglevel=info
  ```

---

## 7. 程式碼風格與檢查 (Linting & Formatting)

為了確保程式碼的一致性、可讀性和品質，本專案強制執行自動化的程式碼風格檢查和格式化。

### 7.1. 為什麼需要程式碼風格與檢查？

-   **提高可讀性**：統一的風格讓所有開發者更容易閱讀和理解程式碼。
-   **減少錯誤**：Linter 可以捕捉潛在的錯誤、不一致的行為和不良的程式碼實踐。
-   **加速開發**：減少程式碼審查中關於風格的討論，讓開發者專注於業務邏輯。
-   **自動化**：透過工具自動執行，減少人工干預。

### 7.2. 推薦工具

-   **`ruff` (Linter & Formatter)**: 一個極速的 Python Linter 和 Formatter，旨在取代 `Flake8`, `isort`, `pylint`, `black` 等多個工具，提供統一的程式碼檢查和格式化體驗。

### 7.3. 安裝與使用

請確保你的虛擬環境已啟用。

1.  **安裝工具**:
    ```bash
    uv pip install ruff
    ```

2.  **配置 `ruff`**:
    `ruff` 的配置通常放在 `pyproject.toml` 中。請確保 `pyproject.toml` 中包含以下或類似的配置：
    ```toml
    [tool.ruff]
    line-length = 120
    select = ["E", "F", "W", "I", "N", "D", "UP", "ANN", "ASYNC", "B", "C4", "DTZ", "ERA", "ISC", "ICN", "PIE", "PT", "RSE", "RET", "SIM", "TID", "ARG", "PLC", "PLE", "PLR", "PLW", "TRY", "PERF"]
    ignore = [] # ruff format 會處理行長度，所以不需要忽略 E501

    [tool.ruff.per-file-ignores]
    "__init__.py" = ["F401"] # 忽略 __init__.py 中未使用的 import 警告
    "tests/*" = ["S101"] # 忽略測試檔案中的 assert 警告
    ```
    （**注意**：上述 `select` 和 `ignore` 列表僅為範例，應根據專案實際需求進行調整。）

3.  **執行檢查與格式化**: 

    -   **格式化 (使用 `ruff`)**:
        ```bash
        ruff format .
        ```
        這會自動格式化專案中的所有 Python 檔案。

    -   **檢查 (使用 `ruff`)**:
        ```bash
        ruff check .
        ```
        這會檢查程式碼中的潛在問題。如果發現問題，`ruff` 會提供建議。

    -   **自動修復 (使用 `ruff`)**:
        ```bash
        ruff check . --fix
        ```
        `ruff` 可以自動修復大部分簡單的問題。

### 7.4. 開發流程整合

強烈建議在提交程式碼前執行 `ruff format .` 和 `ruff check . --fix`。

未來可以考慮整合 `pre-commit` hooks 或 CI/CD 流程，在程式碼提交或推送到遠端倉庫時自動執行這些檢查，以確保程式碼品質。
