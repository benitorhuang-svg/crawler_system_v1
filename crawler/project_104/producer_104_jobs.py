from crawler.project_104.task_104_jobs import fetch_104_data

job_url = "https://www.104.com.tw/job/7anso"
fetch_104 = fetch_104_data.s(job_url)
fetch_104.apply_async() 
print("send task_104 task")

