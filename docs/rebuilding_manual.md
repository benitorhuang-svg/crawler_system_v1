# 資料庫正規化重構手冊 (Database Rebuilding Manual)

## 1. 目標 (Objective)

本文件的核心目標是提供一個清晰、標準化的開發指南，說明如何將專案中各個爬蟲平台 (`104`, `1111`, `cakeresume`, `yes123` 等) 的資料儲存邏輯，對接到一個統一的、正規化的新資料庫架構中。透過遵循本手冊，我們可以確保所有平台的資料儲存邏輯保持一致，從而提升資料品質、降低維護成本，並為後續的資料分析和應用奠定堅實的基礎。

## 2. 核心資料庫模型 (Core Database Models)

我們的正規化架構圍繞以下幾個核心表格構建，這些表格定義在 `crawler/database/models.py` 中：

- **`tb_companies`**: 公司資訊表，儲存唯一的公司實體資訊。
- **`tb_locations`**: 地點資訊表，儲存唯一的、結構化的地理位置資訊。
- **`tb_jobs`**: 職缺核心資訊表，儲存所有平台通用的職缺屬性，並透過外鍵關聯到其他核心實體。
- **`tb_skills`**: 技能總表，儲存唯一的技能名稱。
- **關聯表 (Linking Tables)**: 
    - `tb_job_locations`: 建立職缺與地點的「多對多」關聯。
    - `tb_job_skills`: 建立職缺與技能的「多對多」關聯。
    - `tb_url_categories`: 建立職缺與分類的「多對多」關聯。

## 3. Pydantic Schemas

為了確保資料在應用程式各層之間傳遞時的型別安全與結構一致，我們在 `crawler/database/schemas.py` 中定義了對應的 Pydantic 模型。在開發過程中，最核心的 Schema 是 `JobPydantic`，它的結構設計為一個包含所有關聯資訊的巢狀物件：

```python
# crawler/database/schemas.py

class JobPydantic(BaseModel):
    # ... 職缺的核心欄位 ...

    # 巢狀的關聯物件
    company: Optional[CompanyPydantic] = None
    locations: List[LocationPydantic] = []
    skills: List[SkillPydantic] = []
    category_tags: List[str] = [] # 儲存原始的分類 ID 字串列表
```

## 4. 實作步驟 (Implementation Steps)

將一個新的爬蟲平台（例如 `platform_new`) 對接到此架構，主要需要修改該平台的 `parser` 和 `task` 檔案，並確保 `repository` 層的函式能被正確呼叫。

### 步驟一：修改 Parser (`parser_*.py`)

這是最核心的修改。你需要修改該平台的解析器函式，使其不再回傳一個扁平的資料字典或 Pydantic 物件，而是回傳一個**包含巢狀關聯物件**的 `JobPydantic` 實例。

**範例 (`parser_apidata_104.py`)**:

```python
# crawler/project_104/parser_apidata_104.py

from crawler.database.schemas import JobPydantic, CompanyPydantic, LocationPydantic, ...

def parse_job_item_to_pydantic(job_item: dict) -> Optional[JobPydantic]:
    try:
        # 1. 解析公司資訊
        # 注意：source_company_id 應優先使用從公司 URL 解析出的唯一字串
        company_pydantic = CompanyPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_company_id=str(job_item.get("custNo")),
            name=job_item.get("custName"),
            url=job_item.get("custUrl"),
            industry=job_item.get("industryDesc"),
            employees=job_item.get("employees"),
        )

        # 2. 解析地點資訊
        # 注意：address_detail 應為包含縣市區的完整地址
        location_pydantic = LocationPydantic(
            region=job_item.get("addressRegion"),
            district=None, # 104 API 中無此欄位
            address_detail=job_item.get("addressRegion", "") + job_item.get("addressDetail", ""),
        )

        # 3. 建立核心 JobPydantic 物件，並將關聯物件賦予其屬性
        job_pydantic = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=str(job_item.get("jobNo")),
            # ... 其他職缺欄位 ...

            # 賦予巢狀物件
            company=company_pydantic,
            locations=[location_pydantic],
        )

        return job_pydantic

    except Exception as e:
        logger.error("Failed to parse 104 job item", error=e, exc_info=True)
        return None
```

### 步驟二：修改 Celery Task (`task_urls_new.py`)

Celery Task 的職責是協調 `parser` 和 `repository`。這部分的修改相對簡單，只需確保它接收從 `parser` 回傳的、包含巢狀資料的 `JobPydantic` 物件，並將其直接傳遞給 `upsert_jobs` 函式即可。

**範例 (`task_urls_104.py`)**:

```python
# crawler/project_104/task_urls_104.py

# ... imports ...
from crawler.database.repository import upsert_jobs
from .parser_apidata_104 import parse_job_item_to_pydantic

@app.task()
def crawl_and_store_category_urls(job_category: dict, url_limit: int = 0, db_name_override: Optional[str] = None) -> int:
    # ... (省略抓取 API 資料的邏輯) ...

    # 解析資料，得到包含巢狀物件的 job_pydantic_data
    job_pydantic_data = parse_job_item_to_pydantic(job_api_data)

    if not job_pydantic_data:
        # ... (錯誤處理) ...
        return None

    try:
        # 將包含巢狀資料的物件直接傳遞給 upsert_jobs
        upsert_jobs([job_pydantic_data], db_name=db_name)
        logger.info("Job parsed and upserted successfully.", job_id=job_id, url=url)
        # ... (更新 URL 狀態) ...
        return job_pydantic_data.model_dump()

    except Exception as e:
        # ... (錯誤處理) ...
        return None
```

### 步驟三：本地測試與驗證

在完成程式碼修改後，必須進行端到端測試。請修改對應平台的 `task_urls_*.py` 檔案中的 `if __name__ == "__main__":` 區塊，確保它包含了資料庫初始化步驟。

**範例 (`task_urls_104.py`)**:

```python
if __name__ == "__main__":
    # 1. 獲取正確的資料庫名稱
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)
    logger.info(f"Local test mode: Using database '{db_name}'")

    # 2. 執行初始化，確保表格已建立
    initialize_database(db_name=db_name)
    logger.info(f"Database '{db_name}' initialized for local test.")

    # 3. 執行爬取與寫入邏輯
    # ... (省略 get_urls_by_crawl_status 和迴圈呼叫任務的邏輯) ...
```

執行此腳本 (`uv run python3 -m crawler.project_104.task_urls_104`)，並檢查日誌輸出和資料庫中的資料，確認職缺、公司、地點等資料是否已正確地寫入各自的表格中，並且關聯是否正確建立。

### 步驟三：本地測試與驗證

在完成程式碼修改後，必須進行端到端測試。請修改對應平台的 `task_jobs_*.py` 檔案中的 `if __name__ == "__main__":` 區塊，確保它包含了資料庫初始化步驟。

**範例 (`task_jobs_104.py`)**:

```python
if __name__ == "__main__":
    # 1. 獲取正確的資料庫名稱
    db_name = get_db_name_for_platform(SourcePlatform.PLATFORM_104.value)
    logger.info(f"Local test mode: Using database '{db_name}'")

    # 2. 執行初始化，確保表格已建立
    initialize_database(db_name=db_name)
    logger.info(f"Database '{db_name}' initialized for local test.")

    # 3. 執行爬取與寫入邏輯
    # ... (省略 get_urls_by_crawl_status 和迴圈呼叫任務的邏輯) ...
```

執行此腳本 (`uv run python3 -m crawler.project_104.task_jobs_104`)，並檢查日誌輸出和資料庫中的資料，確認職缺、公司、地點等資料是否已正確地寫入各自的表格中，並且關聯是否正確建立。
