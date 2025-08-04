# 技能提取工具 (`run_skill_extraction.py`) 使用說明

本文件旨在說明如何使用 `run_skill_extraction.py` 腳本，該腳本的主要功能是從職位描述中提取相關的專業技能。

## 功能總結

此腳本會讀取一份包含大量職位描述的 JSON 檔案，並為每一筆職缺資料，分析其 `description` 欄位，然後提取出其中包含的硬技能（Hard Skills）。最終，它會產生一個新的 JSON 檔案，其中包含了原始資料以及一個名為 `skill` 的新欄位，該欄位以 JSON 陣列的格式存放著提取出的技能。

## 核心邏輯

為了確保技能提取的準確性與全面性，本腳本採用了「兩階段混合知識庫」的策略：

1.  **結構化技能樹解析**：
    *   腳本會優先讀取並解析 `skill_tool/topic_tree.md` 檔案。
    *   它會根據 Markdown 的標題層級（`#`, `##`, `###`）來理解技能的分類，並將其轉換為結構化的資料。
    *   此檔案被視為主要的、高品質的技能來源。

2.  **CSV 資料補充**：
    *   接著，腳本會讀取 `skill_tool/104_skill_category.csv` 檔案。
    *   它會從 `hardToolList` 和 `hardSkillList` 欄位中提取出所有技能，並將其補充到第一步建立的知識庫中。
    *   這個步驟確保了 104 人力銀行特有的技能不會被遺漏。

最終，腳本會使用這個合併後的、全面的技能知識庫來進行精準的關鍵字比對。

## 如何執行

請在專案的根目錄 (`/home/soldier/crawler_system_v0_local_test`) 下，執行以下指令：

```bash
python -m skill_tool.run_skill_extraction <input_json_file_path>
```

**範例：**

```bash
python -m skill_tool.run_skill_extraction crawler/project_cakeresume/tb_jobs_cakeresume.json
```

腳本會自動處理所有流程，並在完成後於輸入檔案的相同目錄下產生結果檔案。

## 輸入檔案

本腳本依賴以下三個關鍵檔案：

1.  **`<input_json_file_path>` (位置參數)**
    *   **角色**：這是待處理的原始職缺資料檔案，腳本會讀取此檔案中的 `description` 欄位進行分析。
    *   **範例**：`crawler/project_cakeresume/tb_jobs_cakeresume.json`

2.  **`skill_tool/topic_tree.md` (預設路徑，可選 `--topic_tree <path>`)**
    *   **角色**：主要的技能知識庫來源，提供了一個結構化、分類過的技能體系。

3.  **`skill_tool/104_skill_category.csv` (預設路徑，可選 `--csv_knowledge <path>`)**
    *   **角色**：補充的技能知識庫來源，主要用於增加 104 人力銀行特有的硬技能和工具。

## 輸出結果

*   **檔案名稱**：`[原始輸入檔名]_skill.json` (例如：`tb_jobs_cakeresume_skill.json`)
*   **位置**：與輸入的 JSON 檔案位於**相同的目錄**下。
*   **內容**：此 JSON 檔案會包含原始輸入資料的所有欄位，並額外新增一欄：
    *   `skill`：此欄位的內容是一個 JSON 格式的字串，其中包含從對應職缺的 `description` 中提取出的所有硬技能列表（例如：`["python", "django", "mysql"]`）。

## 主要函式說明

*   `create_enhanced_knowledge_base(topic_tree_path, csv_path)`：負責解析 `topic_tree.md` 和 `104_skill_category.csv`，建立增強型技能知識庫和 `KeywordProcessor` 實例。
*   `extract_skills_precise(description, knowledge_base, keyword_processor)`：使用 `KeywordProcessor` 從職位描述中提取技能，並進行上下文判斷和語義過濾。