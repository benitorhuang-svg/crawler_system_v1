
import json

def update_skill_mapping(yes123_skill_file, skill_mapping_file):
    with open(yes123_skill_file, 'r', encoding='utf-8') as f:
        yes123_skills_data = json.load(f)

    with open(skill_mapping_file, 'r', encoding='utf-8') as f:
        skill_mapping_data = json.load(f)

    # Create a dictionary for quick lookup of yes123 skills
    yes123_skill_map = {}
    for item in yes123_skills_data:
        skill_name = item.get("skill")
        source_id = item.get("source_id")
        if skill_name and source_id:
            yes123_skill_map[skill_name] = source_id

    # Update skill_mapping_data
    updated_count = 0
    for skill_entry in skill_mapping_data:
        skill_name = skill_entry.get("Skill_Name")
        if skill_name in yes123_skill_map:
            source_id = yes123_skill_map[skill_name]
            if "PLATFORM_YES123" in skill_entry and source_id not in skill_entry["PLATFORM_YES123"]:
                skill_entry["PLATFORM_YES123"].append(source_id)
                updated_count += 1
            elif "PLATFORM_YES123" not in skill_entry:
                skill_entry["PLATFORM_YES123"] = [source_id]
                updated_count += 1

    with open(skill_mapping_file, 'w', encoding='utf-8') as f:
        json.dump(skill_mapping_data, f, ensure_ascii=False, indent=2)

    print(f"Updated {updated_count} skill entries in {skill_mapping_file}")

if __name__ == "__main__":
    yes123_skill_file = "/home/soldier/crawler_system_v0_local_test/tb_jobs_yes123_skill.json"
    skill_mapping_file = "/home/soldier/crawler_system_v0_local_test/skill_tool/generated_data/skill_to_job_mapping.json"
    update_skill_mapping(yes123_skill_file, skill_mapping_file)
