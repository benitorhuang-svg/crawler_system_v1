# pytest: disable-assertion-rewriting
import pytest
from unittest.mock import patch, MagicMock

from crawler.project_104.task_category_104 import fetch_url_data_104, flatten_jobcat_recursive
from crawler.database.models import SourcePlatform, CategorySourcePydantic
from crawler.database.repository import get_source_categories, sync_source_categories
from crawler.api_clients.client_104 import fetch_category_data_from_104_api

# Mock data for 104 category API response
MOCK_API_RESPONSE_CATEGORY = [
    {
        "no": "1000",
        "des": "軟體/工程類人員",
        "n": [
            {"no": "1001", "des": "軟體工程師", "n": []},
            {"no": "1002", "des": "韌體工程師", "n": []},
        ],
    },
    {"no": "2000", "des": "行銷/企劃/專案管理類人員", "n": []},
]

# Mock data for existing categories in DB
MOCK_EXISTING_CATEGORIES_DB = [
    CategorySourcePydantic(
        source_platform=SourcePlatform.PLATFORM_104,
        source_category_id="1000",
        source_category_name="軟體/工程類人員",
        parent_source_id=None,
    ),
    CategorySourcePydantic(
        source_platform=SourcePlatform.PLATFORM_104,
        source_category_id="1001",
        source_category_name="軟體工程師",
        parent_source_id="1000",
    ),
]

@pytest.fixture
def mock_api_client():
    with patch('crawler.api_clients.client_104.fetch_category_data_from_104_api') as mock_fetch:
        yield mock_fetch

@pytest.fixture
def mock_repository():
    with patch('crawler.database.repository.get_source_categories') as mock_get_categories,
         patch('crawler.database.repository.sync_source_categories') as mock_sync_categories:
        yield mock_get_categories, mock_sync_categories

def test_fetch_url_data_104_success_new_categories(mock_api_client, mock_repository):
    """
    測試成功抓取並同步新分類的情況。
    """
    mock_fetch, = mock_api_client
    mock_get_categories, mock_sync_categories = mock_repository

    mock_fetch.return_value = MOCK_API_RESPONSE_CATEGORY
    mock_get_categories.return_value = MOCK_EXISTING_CATEGORIES_DB

    test_url = "http://mock-104-category-api.com"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with(test_url, MagicMock())
    mock_get_categories.assert_called_once_with(SourcePlatform.PLATFORM_104)

    # 預期會同步一個新分類 (2000)
    expected_synced_data = [
        {
            'parent_source_id': None,
            'source_category_id': '2000',
            'source_category_name': '行銷/企劃/專案管理類人員',
            'source_platform': SourcePlatform.PLATFORM_104.value,
        }
    ]
    synced_args, _ = mock_sync_categories.call_args
    synced_platform, synced_data = synced_args
    assert synced_platform == SourcePlatform.PLATFORM_104
    assert len(synced_data) == 1
    assert synced_data[0]['source_category_id'] == '2000'
    mock_sync_categories.assert_called_once()

def test_fetch_url_data_104_success_no_new_categories(mock_api_client, mock_repository):
    """
    測試成功抓取但沒有新分類需要同步的情況。
    """
    mock_fetch, = mock_api_client
    mock_get_categories, mock_sync_categories = mock_repository

    mock_fetch.return_value = MOCK_API_RESPONSE_CATEGORY
    # 模擬所有 API 返回的分類都已存在於資料庫中
    mock_get_categories.return_value = [
        CategorySourcePydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_category_id="1000",
            source_category_name="軟體/工程類人員",
            parent_source_id=None,
        ),
        CategorySourcePydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_category_id="1001",
            source_category_name="軟體工程師",
            parent_source_id="1000",
        ),
        CategorySourcePydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_category_id="1002",
            source_category_name="韌體工程師",
            parent_source_id="1000",
        ),
        CategorySourcePydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_category_id="2000",
            source_category_name="行銷/企劃/專案管理類人員",
            parent_source_id=None,
        ),
    ]

    test_url = "http://mock-104-category-api.com"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with(test_url, MagicMock())
    mock_get_categories.assert_called_once_with(SourcePlatform.PLATFORM_104)
    mock_sync_categories.assert_not_called() # 預期不會呼叫同步函式

def test_fetch_url_data_104_api_failure(mock_api_client, mock_repository):
    """
    測試 API 呼叫失敗的情況。
    """
    mock_fetch, = mock_api_client
    mock_get_categories, mock_sync_categories = mock_repository

    mock_fetch.return_value = None # 模擬 API 呼叫失敗

    test_url = "http://mock-104-category-api.com"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with(test_url, MagicMock())
    mock_get_categories.assert_called_once_with(SourcePlatform.PLATFORM_104)
    mock_sync_categories.assert_not_called() # 預期不會呼叫同步函式

def test_fetch_url_data_104_initial_sync(mock_api_client, mock_repository):
    """
    測試資料庫為空時的初始同步情況。
    """
    mock_fetch, = mock_api_client
    mock_get_categories, mock_sync_categories = mock_repository

    mock_fetch.return_value = MOCK_API_RESPONSE_CATEGORY
    mock_get_categories.return_value = [] # 模擬資料庫為空

    test_url = "http://mock-104-category-api.com"
    fetch_url_data_104(test_url)

    mock_fetch.assert_called_once_with(test_url, MagicMock())
    mock_get_categories.assert_called_once_with(SourcePlatform.PLATFORM_104)

    # 預期會同步所有 API 返回的分類
    synced_args, _ = mock_sync_categories.call_args
    synced_platform, synced_data = synced_args
    assert synced_platform == SourcePlatform.PLATFORM_104
    assert len(synced_data) == 4 # 1000, 1001, 1002, 2000
    mock_sync_categories.assert_called_once()