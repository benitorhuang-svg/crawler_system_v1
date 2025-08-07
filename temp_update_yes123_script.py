import json

# Load skill_to_job_mapping.json
with open('/home/soldier/crawler_system_v0_local_test/skill_tool/generated_data/skill_to_job_mapping.json', 'r', encoding='utf-8') as f:
    skill_mapping_data = json.load(f)

# Load yes123_人力銀行_jobcat_json.txt
with open('/home/soldier/crawler_system_v0_local_test/crawler/project_yes123/yes123_人力銀行_jobcat_json.txt', 'r', encoding='utf-8') as f:
    yes123_categories_data = json.load(f)

# Create a lookup for YES123 category names to codes
yes123_category_lookup = {}
for level1_entry in yes123_categories_data:
    level1_name = level1_entry['level_1_name']
    level1_code = level1_entry['list_2'][0]['code'] # Assuming the first entry in list_2 is the overall category
    yes123_category_lookup[level1_name] = level1_code
    for level2_entry in level1_entry['list_2']:
        yes123_category_lookup[level2_entry['level_2_name']] = level2_entry['code']

# Update skill_mapping_data for PLATFORM_YES123
for skill_entry in skill_mapping_data:
    skill_name = skill_entry['Skill_Name']
    for category_name, category_code in yes123_category_lookup.items():
        # Simple check: if skill_name is part of category_name or vice-versa
        if skill_name.lower() in category_name.lower() or category_name.lower() in skill_name.lower():
            if category_code not in skill_entry['PLATFORM_YES123']:
                skill_entry['PLATFORM_YES123'].append(category_code)

# Save the updated skill_to_job_mapping.json
with open('/home/soldier/crawler_system_v0_local_test/skill_tool/generated_data/skill_to_job_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(skill_mapping_data, f, ensure_ascii=False, indent=2)

print("Updated skill_to_job_mapping.json for PLATFORM_YES123 successfully.")
