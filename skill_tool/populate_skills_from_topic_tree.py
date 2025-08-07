import json
import os
import structlog
import re

logger = structlog.get_logger(__name__)

# 定義 topic_tree 類別到 104 jobCode 範圍的映射
# 這裡的映射需要根據實際的 jobCode 範圍和 topic_tree 結構進行細緻定義
# 為了簡化，我將使用 jobCode 的前綴來判斷大類別
CATEGORY_MAPPING = {
    "Software Development": {
        "jobCode_prefixes": ["2007", "2013"], # 資訊軟體系統類, 傳播藝術／設計類 (因為網頁設計師)
        "jobNames_keywords": ["軟體", "程式", "前端", "後端", "全端", "網站", "電玩", "韌體", "系統分析", "數據分析", "資料科學", "區塊鏈", "AI", "演算法", "MIS", "設計師", "開發", "工程師", "應用程式", "軟體工程師", "網頁設計師", "後端開發", "前端開發", "全端開發", "行動應用程式開發", "遊戲開發", "嵌入式系統", "物聯網", "區塊鏈開發", "WordPress開發", "PHP", "Java", "Python", "Go", "C#", "Ruby", "Node.js", "JavaScript", "TypeScript", "HTML", "CSS", "React", "Vue", "Angular", "Next.js", "Nuxt.js", "jQuery", "Laravel", "Spring", "Django", "Flask", "FastAPI", ".NET", "Express.js", "NestJS", "Ruby on Rails", "ASP.NET", "Tailwind", "Bootstrap"]
    },
    "Cloud Computing & DevOps/SRE": {
        "jobCode_prefixes": ["2007"], # 資訊軟體系統類
        "jobNames_keywords": ["雲端", "DevOps", "SRE", "系統管理", "雲服務", "雲端工程師", "DevOps工程師", "AWS", "GCP", "Azure", "Docker", "Kubernetes", "CI/CD", "Terraform", "Ansible"]
    },
    "Data Management & Analytics": {
        "jobCode_prefixes": ["2007"], # 資訊軟體系統類
        "jobNames_keywords": ["數據", "資料", "分析師", "資料庫", "大數據", "商業智慧", "數據分析師", "資料科學家", "資料工程師", "SQL", "MySQL", "PostgreSQL", "MongoDB", "Redis", "ETL", "Power BI", "Tableau"]
    },
    "Networking & Infrastructure": {
        "jobCode_prefixes": ["2007"], # 資訊軟體系統類
        "jobNames_keywords": ["網路", "網管", "通訊", "電信", "基礎設施", "網路管理工程師", "電信工程師", "TCP/IP", "HTTP", "DNS", "VPN", "Firewall"]
    },
    "Cybersecurity": {
        "jobCode_prefixes": ["2007"], # 資訊軟體系統類
        "jobNames_keywords": ["資安", "安全", "網路安全", "資安工程師", "資安分析師", "ISO 27001", "GDPR", "SIEM"]
    },
    "Project & Product Management": {
        "jobCode_prefixes": ["2004", "2001"], # 行銷／企劃／專案管理類, 經營／幕僚類
        "jobNames_keywords": ["專案", "產品經理", "企劃", "管理師", "PM", "產品企劃", "專案管理", "Agile", "Scrum", "Kanban", "PMP"]
    },
    "Management & Leadership": {
        "jobCode_prefixes": ["2001"], # 經營／幕僚類
        "jobNames_keywords": ["主管", "經理", "總監", "執行長", "營運長", "總經理", "領導", "管理", "人資", "行政"]
    },
    "General IT & Support": {
        "jobCode_prefixes": ["2007", "2002"], # 資訊軟體系統類, 行政／總務類
        "jobNames_keywords": ["資訊助理", "系統維護", "設備管制", "MIS", "IT", "技術支援", "辦公室軟體", "Windows", "Linux"]
    },
    "Industry Specific Knowledge": {
        "jobCode_prefixes": [], # 根據具體行業知識，可能需要更細緻的匹配
        "jobNames_keywords": []
    },
    "Soft Skills & Other": {
        "jobCode_prefixes": [], # 軟技能適用於所有職位
        "jobNames_keywords": ["溝通", "解決問題", "分析", "團隊合作", "領導力", "適應性", "學習", "時間管理", "客戶導向", "Inventory Management", "Advertising Effectiveness Evaluation"]
    }
}


def flatten_topic_tree(node, path=None, skills=None):
    if path is None:
        path = []
    if skills is None:
        skills = []

    if isinstance(node, dict):
        for key, value in node.items():
            flatten_topic_tree(value, path + [key], skills)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, str):
                # Extract main skill from potential (alias) format
                match = re.match(r'([^(]+)(?:\((.*)\))?', item)
                main_skill = match.group(1).strip()
                
                # Handle skills with multiple items like "CSS (SCSS, Tailwind, Bootstrap)"
                if match.group(2):
                    sub_skills = [s.strip() for s in match.group(2).split(',')]
                    for sub_skill in sub_skills:
                        skills.append({
                            "Skill_Name": sub_skill,
                            "L1_Category": path[0] if len(path) > 0 else "",
                            "L2_Category": path[1] if len(path) > 1 else "",
                            "L3_Category": path[2] if len(path) > 2 else "",
                        })
                
                skills.append({
                    "Skill_Name": main_skill,
                    "L1_Category": path[0] if len(path) > 0 else "",
                    "L2_Category": path[1] if len(path) > 1 else "",
                    "L3_Category": path[2] if len(path) > 2 else "",
                })
            elif isinstance(item, dict):
                flatten_topic_tree(item, path, skills)
    return skills

def populate_skills_from_topic_tree(topic_tree_path, category_json_path):
    logger.info(f"開始將 {topic_tree_path} 中的技能填充到 {category_json_path}...")
    try:
        with open(topic_tree_path, 'r', encoding='utf-8') as f:
            topic_tree_data = json.load(f)
        
        with open(category_json_path, 'r', encoding='utf-8') as f:
            category_data = json.load(f)

        flat_skills = flatten_topic_tree(topic_tree_data)
        logger.info(f"從 topic_tree.json 提取了 {len(flat_skills)} 個技能。")

        # 創建一個技能名稱到其完整物件的映射，方便查找
        skill_map = {skill["Skill_Name"].lower(): skill for skill in flat_skills}

        modified_count = 0
        unmatched_skills = set(skill_map.keys()) # 初始化為所有技能，後續移除匹配到的

        for job_entry in category_data:
            job_code_str = str(job_entry.get('jobCode', ''))
            job_name = job_entry.get('jobName', '')
            job_task = job_entry.get('jobTask', '')
            job_summary = job_entry.get('jobSummary', '')
            description_text = job_name + " " + job_task + " " + job_summary
            description_text_lower = description_text.lower()

            # Ensure hardSkillList is a list
            hard_skill_list = job_entry.get('hardSkillList', [])
            if isinstance(hard_skill_list, str):
                hard_skill_list = [hard_skill_list] if hard_skill_list else []
            job_entry['hardSkillList'] = hard_skill_list

            current_hard_skills = set(job_entry.get('hardSkillList', []))
            initial_skill_count = len(current_hard_skills)

            # 根據 jobCode 和 jobName 關鍵字判斷所屬類別，並添加相關技能
            for category_name, mapping_info in CATEGORY_MAPPING.items():
                should_add_category_skills = False
                
                # 檢查 jobCode 前綴
                for prefix in mapping_info["jobCode_prefixes"]:
                    if job_code_str.startswith(prefix):
                        should_add_category_skills = True
                        break
                
                # 檢查 jobName, jobSummary, and jobTask 關鍵字
                if not should_add_category_skills:
                    full_text_to_check = (job_name + " " + job_summary + " " + job_task).lower()
                    for keyword in mapping_info["jobNames_keywords"]:
                        if keyword.lower() in full_text_to_check:
                            should_add_category_skills = True
                            break

                if should_add_category_skills:
                    # 獲取該類別下的所有技能
                    category_skills = [s["Skill_Name"] for s in flat_skills if s["L1_Category"] == category_name or s["L2_Category"] == category_name or s["L3_Category"] == category_name]
                    
                    for skill_to_add in category_skills:
                        if skill_to_add not in current_hard_skills:
                            job_entry['hardSkillList'].append(skill_to_add)
                            current_hard_skills.add(skill_to_add)
                            modified_count += 1
                            unmatched_skills.discard(skill_to_add.lower()) # 技能被添加，從未匹配列表中移除
                            logger.debug(f"JobCode {job_entry['jobCode']} 添加了類別技能: {skill_to_add} (來自 {category_name})")

            # 針對描述文本中的關鍵字進行匹配，作為補充
            for skill_name_lower, skill_obj in skill_map.items():
                patterns_to_check = []
                if any('\u4e00' <= char <= '\u9fff' for char in skill_obj["Skill_Name"]): # 中文技能
                    patterns_to_check.append(re.escape(skill_name_lower))
                else: # 英文技能，考慮有無空格兩種形式，並使用更寬鬆的匹配
                    # 精確匹配模式
                    patterns_to_check.append(r'(?i)' + re.escape(skill_name_lower))
                    # 清理後匹配模式 (移除所有非字母數字字元)
                    cleaned_skill_name = re.sub(r'[^a-zA-Z0-9]', '', skill_name_lower)
                    if cleaned_skill_name != skill_name_lower: # 避免重複添加相同的模式
                        patterns_to_check.append(r'(?i)' + re.escape(cleaned_skill_name))
                
                for pattern_str in patterns_to_check:
                    if re.search(pattern_str, description_text_lower):
                        if skill_obj["Skill_Name"] not in current_hard_skills:
                            job_entry['hardSkillList'].append(skill_obj["Skill_Name"])
                            current_hard_skills.add(skill_obj["Skill_Name"])
                            modified_count += 1
                            unmatched_skills.discard(skill_obj["Skill_Name"].lower()) # 技能被添加，從未匹配列表中移除
                            logger.debug(f"JobCode {job_entry['jobCode']} 添加了文本匹配技能: {skill_obj["Skill_Name"]}")
                        break # 找到一個匹配就跳出，避免重複添加

            if len(current_hard_skills) > initial_skill_count:
                logger.debug(f"JobCode {job_entry['jobCode']} 總共添加了 {len(current_hard_skills) - initial_skill_count} 個新技能。")

        with open(category_json_path, 'w', encoding='utf-8') as f:
            json.dump(category_data, f, ensure_ascii=False, indent=2)
        logger.info(f"成功將技能填充到 {category_json_path}。共修改了 {modified_count} 處。")
        if unmatched_skills:
            logger.warning(f"以下技能在 topic_tree.json 中存在，但未被匹配到任何 jobCode: {unmatched_skills}")

    except Exception as e:
        logger.error(f"填充技能失敗: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    topic_tree_file = os.path.join(current_dir, 'source_data', 'topic_tree.json')
    category_json_file = os.path.join(current_dir, 'generated_data', '104_skill_category.json')
    populate_skills_from_topic_tree(topic_tree_file, category_json_file)