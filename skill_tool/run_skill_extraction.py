import pandas as pd
import json
import csv
import re
import os
import structlog
import argparse
from flashtext import KeywordProcessor

logger = structlog.get_logger(__name__)

# 定義需要上下文判斷的模糊技能及其上下文關鍵詞
# 只有當這些技能在文本中與其上下文關鍵詞同時出現時，才被視為有效技能
AMBIGUOUS_SKILLS_CONTEXT = {
    "c": ["語言", "程式", "開發", "++", "#"], # C, C++, C#
    "r": ["語言", "程式", "統計", "studio"], # R語言
    "go": ["語言", "程式", "golang"], # Go語言
    "ui": ["設計", "介面", "使用者"], # UI設計
    "ux": ["設計", "體驗", "使用者"], # UX設計
    "ai": ["人工智慧", "機器學習", "深度學習"], # AI相關
    "ml": ["機器學習", "學習", "模型"], # ML相關
    "bi": ["商業智慧", "報表", "分析"], # BI相關
    "qa": ["測試", "品質", "品保", "品質保證", "軟體測試"], # QA相關
    "pm": ["專案", "產品", "管理", "專案管理", "產品管理"], # PM相關
    "erp": ["系統", "導入", "sap", "企業資源規劃"], # ERP系統
    "crm": ["系統", "客戶", "管理", "客戶關係管理"], # CRM系統
    "scm": ["供應鏈", "管理", "系統", "供應鏈管理"], # SCM系統
    "mes": ["製造", "執行", "系統", "製造執行系統", "生產"], # MES系統
    "plm": ["產品生命週期", "管理", "系統", "產品生命週期管理"], # PLM系統
    "pos": ["銷售點", "系統", "收銀", "銷售時點情報系統", "零售"], # POS系統
    "his": ["醫院資訊", "系統", "醫院資訊系統", "醫療"], # HIS系統
    "phr": ["個人健康", "記錄", "系統", "個人健康記錄", "醫療"], # PHR系統
    "lms": ["學習管理", "系統", "學習管理系統", "教育"], # LMS系統
    "hl7": ["醫療", "標準", "醫療保健"], # HL7標準
    "fhir": ["醫療", "標準", "快速醫療保健互通性資源"], # FHIR標準
    "ems": ["能源管理", "系統", "能源管理系統", "能源"], # EMS系統
    "plc": ["可程式邏輯", "控制器", "可程式邏輯控制器", "自動化"], # PLC
    "scada": ["監控", "資料採集", "監控與資料採集", "自動化"], # SCADA
    "cim": ["電腦整合", "製造", "電腦整合製造"], # CIM
    "iot": ["物聯網", "設備", "智能"], # IoT
    "ros": ["機器人", "作業系統", "機器人作業系統"], # ROS
    "api": ["介面", "串接", "開發", "應用程式介面"], # API
    "sql": ["資料庫", "查詢", "結構化查詢語言"], # SQL
    "nosql": ["資料庫", "非關聯", "非關聯式資料庫"], # NoSQL
    "etl": ["資料整合", "轉換", "資料擷取轉換載入", "資料工程"], # ETL
    "ci": ["持續整合", "cd", "開發"], # CI/CD
    "cd": ["持續交付", "ci", "部署"], # CD
    "dr": ["災難復原", "備份", "業務連續性"], # Disaster Recovery
    "ha": ["高可用性", "容錯", "系統架構"], # High Availability
    "qc": ["品質控制", "檢驗"], # QC
    "pr": ["公關", "媒體", "公共關係"], # PR
    "hr": ["人力資源", "人資", "招聘"], # HR
    "rd": ["研發", "研究", "研究與開發", "創新"], # R&D
    "ae": ["廣告業務", "客戶經理", "客戶經理", "業務"], # AE
    "fae": ["現場應用工程師", "技術支援"], # FAE
    "mis": ["管理資訊系統", "資訊", "資訊管理"], # MIS
    "sdr": ["銷售開發代表", "業務", "業務開發"], # SDR
    "bdr": ["業務開發代表", "業務", "銷售"], # BDR
    "saas": ["軟體即服務", "雲端", "雲端服務"], # SaaS
    "paas": ["平台即服務", "雲端", "雲端服務"], # PaaS
    "iaas": ["基礎設施即服務", "雲端", "雲端服務"], # IaaS
    "esg": ["環境社會治理", "永續", "環境、社會和公司治理", "永續發展"], # ESG
    "csr": ["企業社會責任", "永續", "永續發展"], # CSR
    "computer vision": ["電腦視覺", "影像處理"], # Computer Vision
}

def create_enhanced_knowledge_base(topic_tree_path, csv_path):
    """
    從 topic_tree.md 和 104_skill_category.csv 建立一個增強的、
    帶有權重和別名的結構化技能知識庫。
    同時返回一個配置好的 KeywordProcessor 實例。
    """
    kb = {}
    keyword_processor = KeywordProcessor()

    # 1. 解析 topic_tree.md
    try:
        with open(topic_tree_path, mode='r', encoding='utf-8') as f:
            current_categories = [None, None, None]
            skill_type = 'hard_skill' # 預設為硬技能
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('#'):
                    level = line.count('#')
                    name = line[level:].strip()
                    current_categories[level-1] = name
                    for i in range(level, 3):
                        current_categories[i] = None
                    skill_type = 'soft_skill' if name in ["Soft Skills & Other", "Management & Leadership"] else 'hard_skill'
                    continue

                if line.startswith('-'):
                    match = re.match(r'-\s*([^\(]+)(?:\((.*)\))?', line)
                    if match:
                        main_skill = match.group(1).strip().lower()
                        aliases_str = match.group(2)
                        aliases = [alias.strip().lower() for alias in aliases_str.split(',')] if aliases_str else []
                        
                        if main_skill:
                            # 將主技能添加到知識庫
                            kb[main_skill] = {
                                "type": skill_type,
                                "priority": 1 if skill_type == 'hard_skill' else 3,
                                "aliases": aliases,
                                "source": "topic_tree"
                            }
                            # 將主技能和別名添加到 KeywordProcessor
                            if skill_type == 'hard_skill':
                                keyword_processor.add_keyword(main_skill)
                                for alias in aliases:
                                    keyword_processor.add_keyword(alias, main_skill) # 別名指向主技能

    except FileNotFoundError:
        logger.warning(f"警告：找不到檔案 {topic_tree_path}，將僅使用CSV檔案。" )

    # 2. 補充 104 CSV 的技能
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in ['hardToolList', 'hardSkillList']:
                    try:
                        skills = json.loads(row[key].replace("'", '"'))
                        for skill in skills:
                            skill_name = skill.get('name').strip().lower()
                            if skill_name and skill_name not in kb:
                                kb[skill_name] = {
                                    "type": "hard_skill",
                                    "priority": 2,
                                    "aliases": [],
                                    "source": "104_csv"
                                }
                                # 將新的硬技能添加到 KeywordProcessor
                                keyword_processor.add_keyword(skill_name)
                    except (json.JSONDecodeError, KeyError):
                        continue
    except FileNotFoundError:
        logger.warning(f"警告：找不到 CSV 檔案 {csv_path}，將跳過此步驟。" )

    return kb, keyword_processor

def extract_skills_precise(description, knowledge_base, keyword_processor, skill_type_filter='hard_skill'):
    """
    使用 KeywordProcessor 從職位描述中提取技能，並進行上下文判斷和語義過濾。
    """
    if not isinstance(description, str):
        return []

    # 1. 文本預處理：移除所有非中文字符、字母、數字和空格，並轉為小寫
    # 這樣可以避免像 "computer vision" 這種因為特殊符號導致的誤報
    # 保留中文、英文、數字和空格
    cleaned_description = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5\s]', '', description.lower())

    # 2. 使用 KeywordProcessor 提取關鍵詞
    # KeywordProcessor 已經處理了大小寫和別名，直接提取
    found_keywords_raw = keyword_processor.extract_keywords(cleaned_description)
    
    # 轉換為集合以便後續處理
    found_skills_set = set(found_keywords_raw)

    final_extracted_skills = set()

    # 3. 針對模糊技能進行上下文判斷
    for skill_name in found_skills_set:
        # 檢查是否為需要上下文判斷的模糊技能
        if skill_name in AMBIGUOUS_SKILLS_CONTEXT:
            is_valid_context = False
            # 檢查原始描述中是否包含上下文關鍵詞
            for context_word in AMBIGUOUS_SKILLS_CONTEXT[skill_name]:
                # 這裡使用 re.search 確保上下文詞是獨立的單詞或詞組
                if re.search(r'\b' + re.escape(context_word.lower()) + r'\b', description.lower()):
                    is_valid_context = True
                    break
            if is_valid_context:
                final_extracted_skills.add(skill_name)
        else:
            # 非模糊技能直接加入
            final_extracted_skills.add(skill_name)

    # 4. 語義相關性過濾 (針對明顯不相關的誤報)
    # 這裡可以加入更複雜的邏輯，例如判斷職位類型與技能的相關性
    # 為了簡化，這裡只處理您提到的 "computer vision" 誤報
    # 判斷是否為銷售職位
    is_sales_job = "銷售" in description or "sales" in description.lower() or "業務" in description
    # 判斷是否為工程師職位
    is_engineer_job = "工程師" in description or "engineer" in description.lower()

    if "computer vision" in final_extracted_skills:
        # 如果是銷售職位且不是工程師職位，則移除 "computer vision"
        if is_sales_job and not is_engineer_job:
            final_extracted_skills.remove("computer vision")

    return sorted(list(final_extracted_skills))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract skills from job descriptions.")
    parser.add_argument(
        "input_json", 
        type=str, 
        help="Path to the input job descriptions JSON file."
    )
    parser.add_argument(
        "--topic_tree", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'skill_tool', 'topic_tree.md'),
        help="Path to the topic tree Markdown file."
    )
    parser.add_argument(
        "--csv_knowledge", 
        type=str, 
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'skill_tool', '104_skill_category.csv'),
        help="Path to the CSV knowledge base file."
    )

    args = parser.parse_args()

    # 自動推導輸出檔案路徑
    input_file_path = args.input_json
    output_dir = os.path.dirname(input_file_path)
    input_file_name_without_ext = os.path.splitext(os.path.basename(input_file_path))[0]
    output_file = os.path.join(output_dir, f"{input_file_name_without_ext}_skill.json")

    logger.info("--- 開始執行技能提取（精進版）---")

    logger.info("步驟 1/4: 正在建立增強型技能知識庫和 KeywordProcessor...")
    knowledge_base, kp = create_enhanced_knowledge_base(args.topic_tree, args.csv_knowledge)
    
    # 統計實際添加到 KeywordProcessor 的硬技能數量
    unique_hard_skills_in_kp = set()
    for keyword, replace_with in kp.get_all_keywords().items():
        if isinstance(replace_with, str): # 這是別名，replace_with 是主技能
            unique_hard_skills_in_kp.add(replace_with)
        else: # 這是主技能
            unique_hard_skills_in_kp.add(keyword)

    logger.info(f"知識庫建立完成，總共包含 {len(unique_hard_skills_in_kp)} 個獨特的硬技能。" )

    logger.info(f"步驟 2/4: 正在讀取職缺檔案: {input_file_path}..." )
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            data_list = next((item['data'] for item in raw_data if isinstance(item, dict) and 'data' in item), raw_data if isinstance(raw_data, list) else [])
            df = pd.DataFrame(data_list)
    except Exception as e:
        logger.error(f"讀取或解析JSON檔案時發生錯誤: {e}")
        exit()
    logger.info(f"職缺檔案讀取成功，共 {len(df)} 筆資料。" )

    logger.info("步驟 3/4: 正在處理 description 欄位並提取技能...")
    df['skill'] = df['description'].apply(lambda desc: json.dumps(extract_skills_precise(desc, knowledge_base, kp), ensure_ascii=False))

    logger.info("步驟 4/4: 正在清理資料並儲存至 JSON...")
    clean_re = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: clean_re.sub('', str(x)) if isinstance(x, str) else x)
    
    try:
        # 輸出為 JSON Lines 格式
        df.to_json(output_file, orient='records', lines=True, force_ascii=False)
        logger.info("--- 處理完成！---")
        logger.info(f"最終成果已成功儲存至: {output_file}")
    except Exception as e:
        logger.error(f"寫入JSON檔案時發生錯誤: {e}")
