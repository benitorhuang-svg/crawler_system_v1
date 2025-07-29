import requests



def fetch_url_data_104(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'referer': 'https://www.104.com.tw/'
    }

    job_id = url.split('/')[-1].split('?')[0]
    url_api = f'https://www.104.com.tw/job/ajax/content/{job_id}'
    
  
    response = requests.get(url_api, headers=headers)
    response.raise_for_status()
    data = response.json()
    print(data)

url = "https://www.104.com.tw/job/2ews9"
fetch_url_data_104(url)