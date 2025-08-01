import re
from crawler.database.schemas import SalaryType
import structlog

logger = structlog.get_logger(__name__)

def parse_salary_text(salary_text):
    """
    Parses a salary text string and extracts min/max salary values and salary type.
    Returns a tuple (min_salary, max_salary, salary_type).
    Returns (None, None, SalaryType.NEGOTIABLE) if parsing fails or type is not determined.
    """
    if not salary_text:
        return None, None, SalaryType.NEGOTIABLE

    salary_type = SalaryType.NEGOTIABLE

    # Determine salary type based on keywords
    if "月薪" in salary_text:
        salary_type = SalaryType.MONTHLY
    elif "年薪" in salary_text:
        salary_type = SalaryType.YEARLY
    elif "時薪" in salary_text:
        salary_type = SalaryType.HOURLY
    elif "日薪" in salary_text:
        salary_type = SalaryType.DAILY
    elif "論件計酬" in salary_text:
        salary_type = SalaryType.BY_CASE
    # Handle "面議" explicitly
    # Case 1: "薪資面議(經常性薪資達4萬元含以上)"
    negotiable_match = re.search(r'薪資面議\(經常性薪資達(\d+)萬元含以上\)', salary_text)
    if negotiable_match:
        min_val = int(negotiable_match.group(1)) * 10000
        logger.info("Parsed as negotiable with minimum", min_val=min_val, max_val=None, salary_type=SalaryType.MONTHLY)
        return min_val, None, SalaryType.MONTHLY

    # Case 2: General "面議"
    if "面議" in salary_text:
        logger.info("Parsed as negotiable", salary_text=salary_text)
        return None, None, SalaryType.NEGOTIABLE

    # Clean the input string: remove commas, '元', '約', '起' etc.
    # Keep "萬" for now, as it's handled in a specific regex
    cleaned_text_for_numbers = salary_text.replace(',', '').replace('元', '').replace('約', '').replace('起', '').strip()
    # Remove salary type prefixes for cleaner number parsing, but keep "萬" for now
    cleaned_text_for_numbers = cleaned_text_for_numbers.replace('月薪', '').replace('年薪', '').replace('時薪', '').replace('日薪', '').replace('論件計酬', '').strip()

    logger.info("Parsing salary text", original_text=salary_text, cleaned_text_for_numbers=cleaned_text_for_numbers)

    # 1. Try to parse with '萬' (e.g., "4萬", "3萬5", "4萬元或以上")
    # This regex handles "X萬" and "X萬Y" where Y is optional, and also "X萬含以上"
    wan_match = re.search(r'(\d+)\s*萬(?:(\d*))?\s*(?:元)?(?:或以上)?(?:含以上)?', salary_text)
    if wan_match:
        base_val = int(wan_match.group(1)) * 10000
        if wan_match.group(2): # If there's a second part like '5' in '3萬5'
            try:
                second_part = int(wan_match.group(2))
                if second_part < 10: # Assuming 'X萬Y' means X*10000 + Y*1000 (e.g., 3萬5 = 35000)
                    base_val += second_part * 1000
                else: # Likely a full number like '500' in '3萬500'
                    base_val += second_part
            except ValueError:
                pass # No valid second part number
        logger.info("Parsed as wan", min_val=base_val, max_val=None, salary_type=salary_type)
        return base_val, None, salary_type # Max is open-ended for '萬' format unless specified

    # 2. Try to parse as a range (e.g., "40000~70000", "40000-70000", "40000到70000", "40000 至 70000")
    range_match = re.search(r'(\d+)\s*[~-到至]\s*(\d+)', cleaned_text_for_numbers)
    if range_match:
        min_val = int(range_match.group(1))
        max_val = int(range_match.group(2))
        logger.info("Parsed as range", min_val=min_val, max_val=max_val, salary_type=salary_type)
        return min_val, max_val, salary_type

    # 3. Try to parse with "以上" (e.g., "55000以上")
    above_match = re.search(r'(\d+)\s*以上', salary_text) # Use original salary_text for "以上"
    if above_match:
        min_val = int(above_match.group(1))
        logger.info("Parsed as above", min_val=min_val, max_val=None, salary_type=salary_type)
        return min_val, None, salary_type

    # 4. Try to parse as a single number (e.g., "45000")
    single_match = re.search(r'(\d+)', cleaned_text_for_numbers)
    if single_match:
        single_val = int(single_match.group(1))
        logger.info("Parsed as single fixed", min_val=single_val, max_val=single_val, salary_type=salary_type)
        return single_val, single_val, salary_type

    logger.info("Could not parse salary text", salary_text=salary_text)
    return None, None, SalaryType.NEGOTIABLE # Could not parse