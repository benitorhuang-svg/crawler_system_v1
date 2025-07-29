# pytest: disable-assertion-rewriting
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from crawler.project_104.task_jobs_104 import fetch_url_data_104, parse_salary, JOB_TYPE_MAPPING
from crawler.database.models import SourcePlatform, JobPydantic, JobStatus, SalaryType, CrawlStatus, JobType
from crawler.api_clients.client_104 import fetch_job_data_from_104_api

# Mock data for 104 job API response
MOCK_API_RESPONSE_JOB_SUCCESS = {
    "data": {
        "switch": "on",
        "header": {
            "jobName": "軟體工程師",
            "custNo": "C12345",
            "custName": "測試公司",
            "custUrl": "http://example.com/company/C12345",
            "appearDate": "2025/07/29",
        },
        "jobDetail": {
            "jobType": 1, # FULL_TIME
            "salary": "月薪30000至50000元",
            "jobDescription": "負責軟體開發",
            "addressRegion": "台北市",
            "addressDetail": "信義區",
        },
        "condition": {
            "workExp": "2年以上",
            "edu": "大學",
        },
    }
}

MOCK_API_RESPONSE_JOB_NO_DATA = {"data": None}
MOCK_API_RESPONSE_JOB_SWITCH_OFF = {"data": {"switch": "off"}}

@pytest.fixture
def mock_api_client_jobs():
    with patch('crawler.api_clients.client_104.fetch_job_data_from_104_api') as mock_fetch:
        yield mock_fetch

@pytest.fixture
def mock_repository_jobs():
    with patch('crawler.database.repository.upsert_jobs') as mock_upsert,
         patch('crawler.database.repository.mark_urls_as_crawled') as mock_mark_crawled:
        yield mock_upsert, mock_mark_crawled

def test_fetch_url_data_104_success(mock_api_client_jobs, mock_repository_jobs):
    """
    測試成功抓取、解析並上傳職缺數據。
    """
    mock_fetch = mock_api_client_jobs
    mock_upsert, mock_mark_crawled = mock_repository_jobs

    mock_fetch.return_value = MOCK_API_RESPONSE_JOB_SUCCESS

    test_url = "http://example.com/job/test_job_id"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with("test_job_id")
    mock_upsert.assert_called_once()
    mock_mark_crawled.assert_called_once_with({CrawlStatus.SUCCESS: [test_url]})

    # 驗證 upsert_jobs 呼叫的數據內容
    called_job_pydantic = mock_upsert.call_args[0][0][0]
    assert called_job_pydantic.source_job_id == "test_job_id"
    assert called_job_pydantic.title == "軟體工程師"
    assert called_job_pydantic.job_type == JobType.FULL_TIME
    assert called_job_pydantic.salary_min == 30000
    assert called_job_pydantic.salary_max == 50000
    assert called_job_pydantic.salary_type == SalaryType.MONTHLY

def test_fetch_url_data_104_api_failure(mock_api_client_jobs, mock_repository_jobs):
    """
    測試 API 呼叫失敗的情況。
    """
    mock_fetch = mock_api_client_jobs
    mock_upsert, mock_mark_crawled = mock_repository_jobs

    mock_fetch.return_value = None # 模擬 API 呼叫失敗

    test_url = "http://example.com/job/test_job_id"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with("test_job_id")
    mock_upsert.assert_not_called()
    mock_mark_crawled.assert_called_once_with({CrawlStatus.FAILED: [test_url]})

def test_fetch_url_data_104_no_job_data(mock_api_client_jobs, mock_repository_jobs):
    """
    測試 API 返回無職缺數據的情況。
    """
    mock_fetch = mock_api_client_jobs
    mock_upsert, mock_mark_crawled = mock_repository_jobs

    mock_fetch.return_value = MOCK_API_RESPONSE_JOB_NO_DATA

    test_url = "http://example.com/job/test_job_id"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with("test_job_id")
    mock_upsert.assert_not_called()
    mock_mark_crawled.assert_called_once_with({CrawlStatus.FAILED: [test_url]})

def test_fetch_url_data_104_switch_off(mock_api_client_jobs, mock_repository_jobs):
    """
    測試職缺開關為 off 的情況。
    """
    mock_fetch = mock_api_client_jobs
    mock_upsert, mock_mark_crawled = mock_repository_jobs

    mock_fetch.return_value = MOCK_API_RESPONSE_JOB_SWITCH_OFF

    test_url = "http://example.com/job/test_job_id"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with("test_job_id")
    mock_upsert.assert_not_called()
    mock_mark_crawled.assert_called_once_with({CrawlStatus.FAILED: [test_url]})

def test_parse_salary_monthly():
    min_s, max_s, s_type = parse_salary("月薪30000至50000元")
    assert min_s == 30000
    assert max_s == 50000
    assert s_type == SalaryType.MONTHLY

def test_parse_salary_yearly():
    min_s, max_s, s_type = parse_salary("年薪100萬至150萬元")
    assert min_s == 1000000
    assert max_s == 1500000
    assert s_type == SalaryType.YEARLY

def test_parse_salary_hourly():
    min_s, max_s, s_type = parse_salary("時薪180元")
    assert min_s == 180
    assert max_s == 180
    assert s_type == SalaryType.HOURLY

def test_parse_salary_by_case():
    min_s, max_s, s_type = parse_salary("論件計酬")
    assert min_s is None
    assert max_s is None
    assert s_type == SalaryType.BY_CASE

def test_parse_salary_negotiable():
    min_s, max_s, s_type = parse_salary("面議")
    assert min_s is None
    assert max_s is None
    assert s_type == SalaryType.NEGOTIABLE

def test_job_type_mapping():
    assert JOB_TYPE_MAPPING[1] == JobType.FULL_TIME
    assert JOB_TYPE_MAPPING[2] == JobType.PART_TIME
    assert JOB_TYPE_MAPPING[3] == JobType.INTERNSHIP
    assert JOB_TYPE_MAPPING[4] == JobType.CONTRACT
    assert JOB_TYPE_MAPPING[5] == JobType.TEMPORARY