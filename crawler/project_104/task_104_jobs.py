import requests
from requests.exceptions import HTTPError, JSONDecodeError
from crawler.worker import app


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def fetch_104_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'referer': 'https://www.104.com.tw/'
    }

    job_id = url.split('/')[-1].split('?')[0]
    url_api = f'https://www.104.com.tw/job/ajax/content/{job_id}'
    
    try:
        response = requests.get(url_api, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (HTTPError, JSONDecodeError) as err:
        print(f"發生錯誤: {err}")
        return {}
    
    job_data = data.get('data', {})
    if not job_data or job_data.get('custSwitch', {}) == "off":
        print("職缺內容不存在或已關閉")
        return {}

    extracted_info = {
        'job_id': job_id,
        'update_date': job_data.get('header', {}).get('appearDate'),
        'title': job_data.get('header', {}).get('jobName'),
        'description': job_data.get('jobDetail', {}).get('jobDescription'),
        'salary': job_data.get('jobDetail', {}).get('salary'),
        'work_type': job_data.get('jobDetail', {}).get('workType'),
        'work_time': job_data.get('jobDetail', {}).get('workPeriod'),
        'location': job_data.get('jobDetail', {}).get('addressRegion'),
        'degree': job_data.get('condition', {}).get('edu'),
        'department': job_data.get('jobDetail', {}).get('department'),
        'working_experience': job_data.get('condition', {}).get('workExp'),
        'qualification_required': job_data.get('condition', {}).get('other'),
        'qualification_bonus': job_data.get('welfare', {}).get('welfare'),
        'company_id': job_data.get('header', {}).get('custNo'),
        'company_name': job_data.get('header', {}).get('custName'),
        'company_address': job_data.get('company', {}).get('address'),
        'contact_person': job_data.get('contact', {}).get('hrName'),
        'contact_phone': job_data.get('contact', {}).get('email', '未提供')
    }

    print(extracted_info)
    return extracted_info