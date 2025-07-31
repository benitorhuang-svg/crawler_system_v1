import re

def parse_salary_text(salary_text):
    """
    Parses a salary text string and extracts min/max salary values.
    Returns a tuple (min_salary, max_salary).
    Returns (None, None) if parsing fails.
    """
    if not salary_text:
        return None, None

    # Clean the input string: remove commas, '元', '約', '以上', '起' etc.
    cleaned_text = salary_text.replace(',', '').replace('元', '').replace('約', '').strip()

    # 1. Try to parse as a range (e.g., "40000~70000", "40000-70000", "40000到70000")
    range_match = re.search(r'(\d+)\s*[~-到]\s*(\d+)', cleaned_text)
    if range_match:
        min_val = int(range_match.group(1))
        max_val = int(range_match.group(2))
        return min_val, max_val

    # 2. Try to parse with "以上" (e.g., "55000以上")
    above_match = re.search(r'(\d+)\s*以上', cleaned_text)
    if above_match:
        min_val = int(above_match.group(1))
        return min_val, None # Max is open-ended

    # 3. Try to parse with '萬' (e.g., "4萬", "3萬5", "4萬元或以上")
    # This regex handles "X萬" and "X萬Y" where Y is optional
    wan_match = re.search(r'(\d+)\s*萬(\d*)', cleaned_text)
    if wan_match:
        base_val = int(wan_match.group(1)) * 10000
        if wan_match.group(2): # If there's a second part like '5' in '3萬5'
            # Assuming 'X萬Y' means X*10000 + Y*1000 (e.g., 3萬5 = 35000)
            # Or it could be X*10000 + Y (e.g., 3萬500 = 35000)
            # Let's assume Y is in thousands if it's a single digit, otherwise it's the full number
            try:
                second_part = int(wan_match.group(2))
                if second_part < 10: # Likely a single digit like '5' in '3萬5'
                    base_val += second_part * 1000
                else: # Likely a full number like '500' in '3萬500'
                    base_val += second_part
            except ValueError:
                pass # No valid second part number
        return base_val, None # Max is open-ended for '萬' format unless specified

    # 4. Try to parse as a single number (e.g., "45000")
    single_match = re.search(r'(\d+)', cleaned_text)
    if single_match:
        single_val = int(single_match.group(1))
        return single_val, None

    return None, None # Could not parse
