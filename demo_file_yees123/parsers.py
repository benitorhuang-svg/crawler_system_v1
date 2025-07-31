# crawler/projects/platform_yes123/parsers.py
"""Yes123平台的數據翻譯官 (Data Translator)。

此模組包含的純函數負責解析傳統的、非前端渲染的 Yes123 職缺頁面。
`transform_details_to_job_model` 通過遍歷頁面中的 `<li>` 標籤來
逐項提取職缺信息，並將其轉換為標準化的 `Job` 模型。
"""
import logging
import re
from datetime import datetime
from typing import Optional, Tuple, Dict

from bs4 import BeautifulSoup, Tag
from crawler.database.schema import Job
from crawler.enums import SourcePlatform, JobStatus, SalaryType, JobType
from crawler.utils import clean_text

logger = logging.getLogger(__name__)

def _parse_salary(text: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """從 yes123 的薪資文本中解析最低和最高薪資。"""
    if text is None:
        return None, None
    
    cleaned_text = text.replace(',', '')
    nums = [int(n) for n in re.findall(r'\d+', cleaned_text)]

    if not nums:
        return None, None

    if len(nums) == 1:
        min_salary = nums[0]
        max_salary = None
        if "以上" not in text:
            max_salary = min_salary
        return min_salary, max_salary
    
    if len(nums) >= 2:
        return nums[0], nums[1]
        
    return None, None

def _get_full_location(li_tag: Optional[Tag]) -> Optional[str]:
    """
    [最終修正] 從上班地點的 li 標籤中提取最完整的地址。
    此版本採納了 'find' 方法並增加了安全回退機制。
    """
    if li_tag is None:
        return None
    
    # 優先級 1: 使用 find('a', class_='companyLocation') 來抓取最精確的地址。
    # 這是最理想的情況。
    location_link = li_tag.find('a', class_='companyLocation')
    if location_link and location_link.text.strip():
        return clean_text(location_link.text)
    
    # 優先級 2: 如果找不到詳細地址連結，則回退到提取整個 li 標籤中的值。
    # 這種方法更通用，可以處理沒有地圖連結的頁面。
    full_text = li_tag.get_text(strip=True)
    if '：' in full_text:
        return full_text.split('：', 1)[-1].strip()

    return None


def transform_details_to_job_model(html_content: str, url: str) -> Optional[Job]:
    """
    將 yes123 的職缺詳情 HTML 轉換為標準化的 Job 模型。
    [升級] 此版本全面提取所有可見欄位。
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title_tag = soup.select_one('h1#limit_word_count')
        company_tag = soup.select_one('div.box_firm_name > a')

        if not title_tag or not company_tag:
            logger.error(f"[yes123] Parser failed for url {url}: 在 HTML 中未找到標題或公司名標籤。")
            raise ValueError("在 HTML 中未找到標題或公司名標籤。")
        
        title = clean_text(title_tag.text)
        company_name = clean_text(company_tag.text)
        
        company_source_id, company_url = None, None
        if href := company_tag.get('href'):
            company_url = f"https://www.yes123.com.tw/wk_index/{href}"
            if isinstance(href, str) and (match := re.search(r'p_id=([\w_]+)', href)):
                company_source_id = match.group(1)

        job_info_tags: Dict[str, Tag] = {}
        for li in soup.select("div.job_explain ul > li"):
            if key_tag := li.select_one("span.left_title"):
                key_text = clean_text(key_tag.text)
                if key_text is not None:
                    key = key_text.replace('：', '')
                    job_info_tags[key] = li

        def get_text_from_li(key: str) -> Optional[str]:
            tag = job_info_tags.get(key)
            if tag:
                full_text = tag.get_text(strip=True)
                if '：' in full_text:
                    return full_text.split('：', 1)[-1].strip()
            return None

        salary_text = get_text_from_li("薪資待遇")
        salary_min, salary_max = _parse_salary(salary_text)

        salary_type = SalaryType.NEGOTIABLE
        if salary_text is not None:
            if "月薪" in salary_text:
                salary_type = SalaryType.MONTHLY
            elif "時薪" in salary_text:
                salary_type = SalaryType.HOURLY
            elif "年薪" in salary_text:
                salary_type = SalaryType.YEARLY
            elif "日薪" in salary_text:
                salary_type = SalaryType.BY_CASE
            elif "論件計酬" in salary_text:
                salary_type = SalaryType.BY_CASE

        job_type_text = get_text_from_li("工作性質")
        job_type = None
        if job_type_text is not None:
            if "全職" in job_type_text:
                job_type = JobType.FULL_TIME
            elif "兼職" in job_type_text:
                job_type = JobType.PART_TIME
            elif "實習" in job_type_text:
                job_type = JobType.INTERNSHIP
            elif "派遣" in job_type_text:
                job_type = JobType.CONTRACT
        
        description_raw = get_text_from_li("工作內容")

        posted_at = None
        if posted_at_tag := soup.select_one("div.job_explain h2"):
            posted_at_text_raw = posted_at_tag.text
            posted_at_text = clean_text(posted_at_text_raw)
            if posted_at_text is not None:
                if "今天" in posted_at_text:
                    posted_at = datetime.utcnow().date()
                else:
                    if date_match := re.search(r'(\d{4}/\d{1,2}/\d{1,2})', posted_at_text):
                        try:
                            posted_at = datetime.strptime(date_match.group(1), "%Y/%m/%d").date()
                        except ValueError:
                            logger.warning(f"[yes123] 無法解析日期: {date_match.group(1)}")

        location_tag = job_info_tags.get("工作地點")
        location_text = _get_full_location(location_tag)

        return Job(
            source_platform=SourcePlatform.PLATFORM_YES123,
            source_job_id=url.split('job_id=')[-1].split('&')[0],
            url=url,
            status=JobStatus.ACTIVE,
            title=title,
            description=description_raw,
            job_type=job_type,
            location_text=location_text,
            posted_at=posted_at,
            salary_text=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=get_text_from_li("工作經驗"),
            education_required_text=get_text_from_li("學歷要求"),
            company_source_id=company_source_id,
            company_name=company_name,
            company_url=company_url,
        )
    except Exception as e:
        logger.error(f"[yes123] Parser failed for url {url}: {e}", exc_info=True)
        return None