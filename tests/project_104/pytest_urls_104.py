import pytest
from unittest.mock import patch, MagicMock
from collections import deque

from crawler.project_104.task_urls_104 import crawl_and_store_category_urls
from crawler.database.models import SourcePlatform
from crawler.database.repository import upsert_urls
from crawler.api_clients.client_104 import fetch_job_urls_from_104_api
from crawler.config import URL_CRAWLER_UPLOAD_BATCH_SIZE

# Mock data for 104 job URLs API response
MOCK_API_RESPONSE_URLS_PAGE1 = {
    "data": [
        {"link": {"job": "http://example.com/job/1"}},
        {"link": {"job": "http://example.com/job/2"}},
        {"link": {"job": "http://example.com/job/3"}},
    ]
}
MOCK_API_RESPONSE_URLS_PAGE2 = {
    "data": [
        {"link": {"job": "http://example.com/job/4"}},
        {"link": {"job": "http://example.com/job/5"}},
    ]
}
MOCK_API_RESPONSE_URLS_EMPTY = {"data": []}

@pytest.fixture
def mock_api_client_urls():
    with patch('crawler.api_clients.client_104.fetch_job_urls_from_104_api') as mock_fetch:
        yield mock_fetch

@pytest.fixture
def mock_repository_urls():
    with patch('crawler.database.repository.upsert_urls') as mock_upsert:
        yield mock_upsert

@pytest.fixture
def mock_sleep():
    with patch('time.sleep') as mock_time_sleep:
        yield mock_time_sleep

def test_crawl_and_store_category_urls_success(mock_api_client_urls, mock_repository_urls, mock_sleep):
    """
    測試成功抓取多頁 URL 並上傳到資料庫。
    """
    mock_api_client_urls.side_effect = [
        MOCK_API_RESPONSE_URLS_PAGE1, # Page 1
        MOCK_API_RESPONSE_URLS_PAGE2, # Page 2
        MOCK_API_RESPONSE_URLS_EMPTY, # Page 3 (to stop the loop)
    ]

    # 設置一個較小的批次大小，以便測試 upsert_urls 的呼叫
    with patch('crawler.config.URL_CRAWLER_UPLOAD_BATCH_SIZE', 3):
        crawl_and_store_category_urls("mock_category_code")

    # 驗證 API 呼叫次數
    assert mock_api_client_urls.call_count == 3

    # 驗證 upsert_urls 呼叫次數和內容
    # 第一次呼叫應該是 page1 的所有 URL
    mock_repository_urls.assert_any_call(SourcePlatform.PLATFORM_104, [
        "http://example.com/job/1",
        "http://example.com/job/2",
        "http://example.com/job/3",
    ])
    # 第二次呼叫應該是 page2 的所有 URL (因為批次大小為 3，所以會立即上傳)
    mock_repository_urls.assert_any_call(SourcePlatform.PLATFORM_104, [
        "http://example.com/job/4",
        "http://example.com/job/5",
    ])
    assert mock_repository_urls.call_count == 2

def test_crawl_and_store_category_urls_api_failure(mock_api_client_urls, mock_repository_urls, mock_sleep):
    """
    測試 API 呼叫失敗時的錯誤處理。
    """
    mock_api_client_urls.return_value = None # 模擬 API 呼叫失敗

    crawl_and_store_category_urls("mock_category_code")

    mock_api_client_urls.assert_called_once()
    mock_repository_urls.assert_not_called() # 預期不會呼叫 upsert_urls

def test_crawl_and_store_category_urls_empty_data(mock_api_client_urls, mock_repository_urls, mock_sleep):
    """
    測試 API 返回空數據時的處理。
    """
    mock_api_client_urls.return_value = MOCK_API_RESPONSE_URLS_EMPTY

    crawl_and_store_category_urls("mock_category_code")

    mock_api_client_urls.assert_called_once()
    mock_repository_urls.assert_not_called() # 預期不會呼叫 upsert_urls

def test_crawl_and_store_category_urls_non_list_data(mock_api_client_urls, mock_repository_urls, mock_sleep):
    """
    測試 API 返回非列表格式數據時的錯誤處理。
    """
    mock_api_client_urls.return_value = {"data": "not_a_list"}

    crawl_and_store_category_urls("mock_category_code")

    mock_api_client_urls.assert_called_once()
    mock_repository_urls.assert_not_called() # 預期不會呼叫 upsert_urls
