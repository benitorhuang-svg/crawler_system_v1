Directory structure:
└── crawler_system/
    ├── README.md
    ├── docker-compose-producer-network.yml
    ├── docker-compose-worker-network.yml
    ├── Dockerfile
    ├── genenv.py
    ├── local.ini
    ├── mysql-network.yml
    ├── pyproject.toml
    ├── rabbitmq-network.yml
    ├── requirements.txt
    ├── uv.lock
    ├── .python-version
    ├── crawler/
    │   ├── __init__.py
    │   ├── check_crawler_config.py
    │   ├── config.py
    │   ├── logging_config.py
    │   ├── worker.py
    │   ├── database/
    │   │   ├── connection.py
    │   │   ├── get_category_ids.py
    │   │   ├── models.py
    │   │   ├── pandas_sql_config.py
    │   │   ├── pandas_sql_demo.py
    │   │   ├── repository.py
    │   │   ├── temp_count_db.py
    │   │   ├── test_upload_data_to_mysql.py
    │   │   └── test_upload_duplicate_data.py
    │   ├── finmind/
    │   │   └── config.py
    │   └── project_104/
    │       ├── config_104.py
    │       ├── local_fetch_104_url_data.py
    │       ├── producer_category_104.py
    │       ├── producer_jobs_104.py
    │       ├── producer_urls_104.py
    │       ├── single_url_api_data_104.py
    │       ├── single_url_api_data_104.txt
    │       ├── task_category_104.py
    │       ├── task_jobs_104.py
    │       └── task_urls_104.py
    └── docs/
        ├── development_manual.md
        └── project_104_local_test_plan.md

================================================
FILE: README.md
================================================
# crawler

# 環境設定

#### 安裝 uv
<!-- https://docs.astral.sh/uv/getting-started/installation/#standalone-installer -->
    curl -LsSf https://astral.sh/uv/install.sh | sh


#### 建立初始化環境
   uv init
   uv venv


#### 安裝 repo 套件
    uv add <package name>
    uv add ruff


#### 將專案套件化 / 輸出到 requirements.txt
    uv pip install -e .
    uv pip compile pyproject.toml -o requirements.txt


#### 建立環境變數
    ENV=DEV python genenv.py
    ENV=DOCKER python genenv.py
    ENV=PRODUCTION python genenv.py


#### 排版
    ruff check
    ruff check --fix


# 測試 rabbitmq docker image
    docker compose -f rabbitmq-network.yml up -d
        - rabbitmq UI : http://localhost:15672 worker worker
        - flowler UI  : http://localhost:5555
    docker compose -f rabbitmq-network.yml down


#### 查看 docker container 服務運作狀況
    docker ps -a

#### 查看 服務 log
    docker logs container_name    


# 本地執行檔案測試
### Worker 啟動預設執行 celery 的 queue 的工人
    uv run celery -A crawler.worker worker --loglevel=info
    uv run celery -A crawler.worker worker -Q crawler_category_104 --loglevel=info
### Producer 發送任務
    uv run python crawler/project_104/producer_104_jobs.py

<!-- 任務流程：producer 負責生成並分發任務，而 worker 負責接收並執行這些任務 -->
    APP_ENV=DEV python -m crawler.worker
    APP_ENV=DEV python -m crawler.project_104.producer_category_104




# 建立專案的 Docker image

####  docker image build / push
    docker build -f Dockerfile -t benitorhuang/crawler_jobs:0.0.1 .
    docker push benitorhuang/crawler_jobs:0.0.1



# docker compose 容器網路連線
<!-- 建立 docker image 連線通道 -->
### 測試 docker compose : rabbitmq、 worker 和 producer 的網路連線。
docker compose -f rabbitmq-network.yml up -d
docker compose -f docker-compose-worker-network.yml up -d
docker compose -f docker-compose-producer-network.yml up -d



#### 建立 network
    docker network create my_network

# 建立 mysql service / 啟動 / 關閉 / 上傳資料
    docker compose -f mysql.yml up -d
    docker compose -f mysql.yml down
    APP_ENV=DEV python -m crawler.database.test_upload_data_to_mysql
    APP_ENV=DEV python -m crawler.database.test_upload_duplicate_data


####   worker/producer  <啟動 / 關閉>
    APP_ENV=DEV celery -A crawler.worker worker --loglevel=info
    APP_ENV=DEV python -m crawler.project_104.producer_category_104
    APP_ENV=DEV python -m crawler.project_104.producer_urls_104
    APP_ENV=DEV python -m crawler.project_104.producer_jobs_104


    docker compose -f docker-compose-worker-network.yml up -d
    docker compose -f docker-compose-worker-network.yml down
    docker compose -f docker-compose-producer-network.yml up -d
    docker compose -f docker-compose-producer-network.yml down
    
#### 加上環境版本 DOCKER_IMAGE_VERSION=***
    DOCKER_IMAGE_VERSION=0.0.1 docker compose -f docker-compose-worker-network-version.yml up -d
    DOCKER_IMAGE_VERSION=0.0.1 docker compose -f docker-compose-worker-network-version.yml down
    DOCKER_IMAGE_VERSION=0.0.3 docker compose -f docker-compose-producer-network-version.yml up -d
    DOCKER_IMAGE_VERSION=0.0.3 docker compose -f docker-compose-producer-network-version.yml down



#### 啟動 scheduler

    DOCKER_IMAGE_VERSION=0.0.4 docker compose -f docker-compose-scheduler-network-version.yml up -d

#### 關閉 scheduler

    DOCKER_IMAGE_VERSION=0.0.4 docker compose -f docker-compose-scheduler-network-version.yml down

#### 查看 log

    docker logs container_name

#### 下載 taiwan_stock_price.csv

    wget https://github.com/FinMind/FinMindBook/releases/download/data/taiwan_stock_price.csv

#### 上傳 taiwan_stock_price.csv

    pipenv run python crawler/upload_taiwan_stock_price_to_mysql.py

#### login
    gcloud auth application-default login

#### set GCP project
    gcloud config set project high-transit-465916-a6

#### 上傳台股股價到 BigQuery
    pipenv run python crawler/upload_taiwan_stock_price_to_bigquery.py

#### 輸入 Secret Manager
    pipenv run python crawler/print_secret_manager.py



================================================
FILE: docker-compose-producer-network.yml
================================================
# version: '3.0'  # 使用 Docker Compose 的版本 3.0，適合大部分部署場景

services:
  producer_104_jobs:  # 定義一個服務，名稱為 crawler_twse
    build: .  # Build the image from the local Dockerfile
    # image: benitorhuang/crawler_jobs:0.0.1  # 使用的映像檔名稱與標籤（版本）
    
    hostname: "crawler_104"  # 設定 hostname = twse
    command: python -m crawler.project_104.producer_104_jobs
    # restart: always  # 若容器停止或崩潰，自動重新啟動
    environment:
      - TZ=Asia/Taipei  # 設定時區為台北（UTC+8）
    networks:
      - my_network  # 將此服務連接到 my_network 網路

networks:
  my_network:
    # 加入已經存在的網路
    external: true



================================================
FILE: docker-compose-worker-network.yml
================================================
# version: '3.0'  # 使用 Docker Compose 的版本 3.0，適合大部分部署場景

services:
  crawler_104:  # 定義一個服務，名稱為 crawler_twse
    build: .  # Build the image from the local Dockerfile
    # image: benitorhuang/crawler_jobs:0.0.1 

    hostname: "crawler_104_category"
    command: celery -A crawler.worker worker --loglevel=info --hostname=%h -Q jobs_104  
    # 啟動容器後執行的命令，這裡是啟動 Celery worker，指定 app 為 crawler.worker，設定日誌等級為 info，
    # 使用主機名稱當作 worker 名稱（%h），並將此 worker 加入名為 "twse" 的任務佇列 (queue)

    restart: always  # 若容器停止或崩潰，自動重新啟動
    environment:
      - TZ=Asia/Taipei  # 設定時區為台北（UTC+8）
    networks:
      - my_network  # 將此服務連接到 my_network 網路

networks:
  my_network:
    # 加入已經存在的網路
    external: true



================================================
FILE: Dockerfile
================================================
# Stage 1: Builder
FROM python:3.13-slim-bullseye AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt uv.lock .
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && /root/.local/bin/uv pip install -r requirements.txt --system


# Stage 2: Runner
FROM python:3.13-slim-bullseye AS runner

WORKDIR /app

# Copy only the installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the application code
COPY . .

ENV PYTHONPATH="/app"
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

CMD ["/bin/bash"]


================================================
FILE: genenv.py
================================================
import os
from configparser import ConfigParser
import structlog
import sys

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

def generate_env_file():
    config_path = "local.ini"

    if not os.path.exists(config_path):
        logger.critical("local.ini not found. Please ensure it exists in the project root.", path=config_path)
        sys.exit(1)

    local_config = ConfigParser()
    try:
        local_config.read(config_path)
    except Exception as e:
        logger.critical("Failed to read local.ini configuration file.", path=config_path, error=e, exc_info=True)
        sys.exit(1)

    # Determine which section to use based on APP_ENV environment variable
    app_env = os.environ.get("APP_ENV", "").upper()

    selected_section_name = "DEFAULT" # Default fallback
    if app_env and app_env in local_config:
        selected_section_name = app_env
    elif "DEFAULT" not in local_config:
        logger.critical("Neither APP_ENV specified section nor 'DEFAULT' section found in local.ini.", app_env=app_env)
        sys.exit(1)

    section = local_config[selected_section_name]
    logger.info("Using configuration section.", section_name=selected_section_name)

    env_content = ""
    for key, value in section.items():
        env_content += f"{key.upper()}={value}\n"

    env_file_path = ".env"
    try:
        with open(env_file_path, "w", encoding="utf8") as env_file:
            env_file.write(env_content)
        logger.info(".env file generated successfully.", path=env_file_path, section_used=selected_section_name)
    except Exception as e:
        logger.critical("Failed to write .env file.", path=env_file_path, error=e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    generate_env_file()


================================================
FILE: local.ini
================================================
# dev 環境
[DEV]
# Worker
WORKER_ACCOUNT = worker
WORKER_PASSWORD = worker
# RabbitMQ
RABBITMQ_HOST = 127.0.0.1
RABBITMQ_PORT = 5672
# MySQL
MYSQL_DATABASE = crawler_db
MYSQL_HOST = 127.0.0.1
MYSQL_PORT = 3306
MYSQL_ACCOUNT = root
MYSQL_ROOT_PASSWORD = root_password
MYSQL_PASSWORD = root_password
# Logging
LOG_LEVEL = INFO
# Producer Batching
PRODUCER_BATCH_SIZE = 100
PRODUCER_DISPATCH_INTERVAL_SECONDS = 1.0
# URL Crawler General Settings
URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = 20
URL_CRAWLER_UPLOAD_BATCH_SIZE = 30
URL_CRAWLER_SLEEP_MIN_SECONDS = 0.5
URL_CRAWLER_SLEEP_MAX_SECONDS = 1.5


# Docker 環境
[DOCKER]
# Worker
WORKER_ACCOUNT = worker
WORKER_PASSWORD = worker
# RabbitMQ
RABBITMQ_HOST = rabbitmq
RABBITMQ_PORT = 5672
# MySQL
MYSQL_DATABASE = crawler_db
MYSQL_HOST = crawler_jobs_mysql
MYSQL_PORT = 3306
MYSQL_ACCOUNT = root
MYSQL_ROOT_PASSWORD = root_password
MYSQL_PASSWORD = root_password
# Logging
LOG_LEVEL = INFO
# Producer Batching
PRODUCER_BATCH_SIZE = 100
PRODUCER_DISPATCH_INTERVAL_SECONDS = 1.0
# URL Crawler General Settings
URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = 20
URL_CRAWLER_UPLOAD_BATCH_SIZE = 30
URL_CRAWLER_SLEEP_MIN_SECONDS = 0.5
URL_CRAWLER_SLEEP_MAX_SECONDS = 1.5


# Prod 環境
[PRODUCTION]
# Worker
WORKER_ACCOUNT = worker
WORKER_PASSWORD = worker
# RabbitMQ
RABBITMQ_HOST = rabbitmq
RABBITMQ_PORT = 5672
# MySQL
MYSQL_DATABASE = crawler_db
MYSQL_HOST = 127.0.0.1
MYSQL_PORT = 3306
MYSQL_ACCOUNT = root
MYSQL_ROOT_PASSWORD = root_password
MYSQL_PASSWORD = root_password
# Logging
LOG_LEVEL = INFO
# Producer Batching
PRODUCER_BATCH_SIZE = 100
PRODUCER_DISPATCH_INTERVAL_SECONDS = 1.0
# URL Crawler General Settings
URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = 20
URL_CRAWLER_UPLOAD_BATCH_SIZE = 30
URL_CRAWLER_SLEEP_MIN_SECONDS = 0.5
URL_CRAWLER_SLEEP_MAX_SECONDS = 1.5



================================================
FILE: mysql-network.yml
================================================
version: '3.8'

services:
  crawler_jobs_mysql:
    image: mysql:8.0
    # 設定 mysql 使用原生認證的密碼 hash
    command: mysqld --default-authentication-plugin=mysql_native_password
    environment:
      MYSQL_DATABASE: ${MYSQL_DATABASE}
      MYSQL_ACCOUNT: ${MYSQL_ACCOUNT}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
    ports:
      - "3306:3306"

    restart: always
    volumes:
      - mysql_data:/var/lib/mysql
    networks:
      - my_network



  crawler_jobs_phpmyadmin:
    image: phpmyadmin/phpmyadmin
    links: 
          - crawler_jobs_mysql:db
    restart: always
    ports:
      - "8080:80"
    networks:
      - my_network

networks:
  my_network:
    external: true

volumes:
  mysql_data:


================================================
FILE: pyproject.toml
================================================
[project]
name = "crawler-jobs"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "celery>=5.5.3",
    "ruff>=0.12.5",
    "structlog>=25.4.0",
    "python-dotenv",
    "requests>=2.32.4",
    "pymysql==1.1.0",
    "tenacity>=8.2.3",
    "pydantic>=2.0.0",
    "sqlalchemy>=2.0.41",
    "gitingest>=0.1.5",
    "pandas>=2.3.1",
]



================================================
FILE: rabbitmq-network.yml
================================================
version: '3'
services:

  rabbitmq:
    # 使用 RabbitMQ 官方管理版的輕量 Alpine 版本映像檔
    image: 'rabbitmq:3.12-management-alpine'
    restart: always  # 若容器停止或崩潰，自動重新啟動
    ports: 
      - '5672:5672'       # 對外開放 RabbitMQ 的 AMQP 通訊埠（應用程式通訊埠）
      - '15672:15672'     # 對外開放 RabbitMQ 的管理介面（Web UI）埠口
    environment:
      RABBITMQ_DEFAULT_USER: "worker"       # 預設使用者名稱設定為 worker
      RABBITMQ_DEFAULT_PASS: "worker"       # 預設密碼設定為 worker
      RABBITMQ_DEFAULT_VHOST: "/"            # 預設虛擬主機 (Virtual Host)，用於隔離不同環境的訊息隊列
    networks:
      - my_network                           # 將服務加入名為 my_network 的自訂網路

  flower:
    # 使用 Flower 映像來監控 Celery 的任務佇列狀況
    image: mher/flower:0.9.5
    command: ["flower", "--broker=amqp://worker:worker@rabbitmq", "--port=5555"]  
    restart: always  # 若容器停止或崩潰，自動重新啟動
    # 啟動 Flower，設定 RabbitMQ 為 broker，並監聽 5555 埠口
    ports: 
      - 5555:5555                           # 映射 Flower 的監控介面埠口到宿主機
    depends_on:
      - rabbitmq                           # 確保 RabbitMQ 先啟動後，Flower 再啟動
    networks:
      - my_network                         # 將服務加入 my_network 網路

networks:
  my_network:
    # 加入已經存在的網路
    external: true


================================================
FILE: requirements.txt
================================================
# This file was autogenerated by uv via the following command:
#    uv pip compile pyproject.toml -o requirements.txt
amqp==5.3.1
    # via kombu
annotated-types==0.7.0
    # via pydantic
anyio==4.9.0
    # via
    #   httpx
    #   starlette
    #   watchfiles
billiard==4.2.1
    # via celery
celery==5.5.3
    # via crawler-jobs (pyproject.toml)
certifi==2025.7.14
    # via
    #   httpcore
    #   httpx
    #   requests
    #   sentry-sdk
charset-normalizer==3.4.2
    # via requests
click==8.2.1
    # via
    #   celery
    #   click-didyoumean
    #   click-plugins
    #   click-repl
    #   gitingest
    #   rich-toolkit
    #   typer
    #   uvicorn
click-didyoumean==0.3.1
    # via celery
click-plugins==1.1.1.2
    # via celery
click-repl==0.3.0
    # via celery
deprecated==1.2.18
    # via limits
dnspython==2.7.0
    # via email-validator
email-validator==2.2.0
    # via
    #   fastapi
    #   pydantic
fastapi==0.116.1
    # via gitingest
fastapi-cli==0.0.8
    # via fastapi
fastapi-cloud-cli==0.1.5
    # via fastapi-cli
gitingest==0.1.5
    # via crawler-jobs (pyproject.toml)
greenlet==3.2.3
    # via sqlalchemy
h11==0.16.0
    # via
    #   httpcore
    #   uvicorn
httpcore==1.0.9
    # via httpx
httptools==0.6.4
    # via uvicorn
httpx==0.28.1
    # via
    #   fastapi
    #   fastapi-cloud-cli
idna==3.10
    # via
    #   anyio
    #   email-validator
    #   httpx
    #   requests
jinja2==3.1.6
    # via fastapi
kombu==5.5.4
    # via celery
limits==5.4.0
    # via slowapi
markdown-it-py==3.0.0
    # via rich
markupsafe==3.0.2
    # via jinja2
mdurl==0.1.2
    # via markdown-it-py
numpy==2.3.2
    # via pandas
packaging==25.0
    # via
    #   kombu
    #   limits
pandas==2.3.1
    # via crawler-jobs (pyproject.toml)
pathspec==0.12.1
    # via gitingest
prompt-toolkit==3.0.51
    # via click-repl
pydantic==2.11.7
    # via
    #   crawler-jobs (pyproject.toml)
    #   fastapi
    #   fastapi-cloud-cli
    #   gitingest
pydantic-core==2.33.2
    # via pydantic
pygments==2.19.2
    # via rich
pymysql==1.1.0
    # via crawler-jobs (pyproject.toml)
python-dateutil==2.9.0.post0
    # via
    #   celery
    #   pandas
python-dotenv==1.1.1
    # via
    #   crawler-jobs (pyproject.toml)
    #   gitingest
    #   uvicorn
python-multipart==0.0.20
    # via fastapi
pytz==2025.2
    # via pandas
pyyaml==6.0.2
    # via uvicorn
regex==2024.11.6
    # via tiktoken
requests==2.32.4
    # via
    #   crawler-jobs (pyproject.toml)
    #   tiktoken
rich==14.1.0
    # via
    #   rich-toolkit
    #   typer
rich-toolkit==0.14.9
    # via
    #   fastapi-cli
    #   fastapi-cloud-cli
rignore==0.6.4
    # via fastapi-cloud-cli
ruff==0.12.5
    # via crawler-jobs (pyproject.toml)
sentry-sdk==2.33.2
    # via fastapi-cloud-cli
shellingham==1.5.4
    # via typer
six==1.17.0
    # via python-dateutil
slowapi==0.1.9
    # via gitingest
sniffio==1.3.1
    # via anyio
sqlalchemy==2.0.41
    # via crawler-jobs (pyproject.toml)
starlette==0.47.2
    # via
    #   fastapi
    #   gitingest
structlog==25.4.0
    # via crawler-jobs (pyproject.toml)
tenacity==9.1.2
    # via crawler-jobs (pyproject.toml)
tiktoken==0.9.0
    # via gitingest
tomli==2.2.1
    # via gitingest
typer==0.16.0
    # via
    #   fastapi-cli
    #   fastapi-cloud-cli
typing-extensions==4.14.1
    # via
    #   fastapi
    #   limits
    #   pydantic
    #   pydantic-core
    #   rich-toolkit
    #   sqlalchemy
    #   typer
    #   typing-inspection
typing-inspection==0.4.1
    # via pydantic
tzdata==2025.2
    # via
    #   kombu
    #   pandas
urllib3==2.5.0
    # via
    #   requests
    #   sentry-sdk
uvicorn==0.35.0
    # via
    #   fastapi
    #   fastapi-cli
    #   fastapi-cloud-cli
    #   gitingest
uvloop==0.21.0
    # via uvicorn
vine==5.1.0
    # via
    #   amqp
    #   celery
    #   kombu
watchfiles==1.1.0
    # via uvicorn
wcwidth==0.2.13
    # via prompt-toolkit
websockets==15.0.1
    # via uvicorn
wrapt==1.17.2
    # via deprecated
cryptography



================================================
FILE: uv.lock
================================================
# This file was autogenerated by uv via the following command:
#    uv pip compile requirements.txt -o uv.lock
amqp==5.3.1
    # via
    #   -r requirements.txt
    #   kombu
annotated-types==0.7.0
    # via
    #   -r requirements.txt
    #   pydantic
anyio==4.9.0
    # via
    #   -r requirements.txt
    #   httpx
    #   starlette
    #   watchfiles
billiard==4.2.1
    # via
    #   -r requirements.txt
    #   celery
celery==5.5.3
    # via -r requirements.txt
certifi==2025.7.14
    # via
    #   -r requirements.txt
    #   httpcore
    #   httpx
    #   requests
    #   sentry-sdk
cffi==1.17.1
    # via cryptography
charset-normalizer==3.4.2
    # via
    #   -r requirements.txt
    #   requests
click==8.2.1
    # via
    #   -r requirements.txt
    #   celery
    #   click-didyoumean
    #   click-plugins
    #   click-repl
    #   gitingest
    #   rich-toolkit
    #   typer
    #   uvicorn
click-didyoumean==0.3.1
    # via
    #   -r requirements.txt
    #   celery
click-plugins==1.1.1.2
    # via
    #   -r requirements.txt
    #   celery
click-repl==0.3.0
    # via
    #   -r requirements.txt
    #   celery
cryptography==45.0.5
    # via -r requirements.txt
deprecated==1.2.18
    # via
    #   -r requirements.txt
    #   limits
dnspython==2.7.0
    # via
    #   -r requirements.txt
    #   email-validator
email-validator==2.2.0
    # via
    #   -r requirements.txt
    #   fastapi
    #   pydantic
fastapi==0.116.1
    # via
    #   -r requirements.txt
    #   gitingest
fastapi-cli==0.0.8
    # via
    #   -r requirements.txt
    #   fastapi
fastapi-cloud-cli==0.1.5
    # via
    #   -r requirements.txt
    #   fastapi-cli
gitingest==0.1.5
    # via -r requirements.txt
greenlet==3.2.3
    # via
    #   -r requirements.txt
    #   sqlalchemy
h11==0.16.0
    # via
    #   -r requirements.txt
    #   httpcore
    #   uvicorn
httpcore==1.0.9
    # via
    #   -r requirements.txt
    #   httpx
httptools==0.6.4
    # via
    #   -r requirements.txt
    #   uvicorn
httpx==0.28.1
    # via
    #   -r requirements.txt
    #   fastapi
    #   fastapi-cloud-cli
idna==3.10
    # via
    #   -r requirements.txt
    #   anyio
    #   email-validator
    #   httpx
    #   requests
jinja2==3.1.6
    # via
    #   -r requirements.txt
    #   fastapi
kombu==5.5.4
    # via
    #   -r requirements.txt
    #   celery
limits==5.4.0
    # via
    #   -r requirements.txt
    #   slowapi
markdown-it-py==3.0.0
    # via
    #   -r requirements.txt
    #   rich
markupsafe==3.0.2
    # via
    #   -r requirements.txt
    #   jinja2
mdurl==0.1.2
    # via
    #   -r requirements.txt
    #   markdown-it-py
numpy==2.3.2
    # via
    #   -r requirements.txt
    #   pandas
packaging==25.0
    # via
    #   -r requirements.txt
    #   kombu
    #   limits
pandas==2.3.1
    # via -r requirements.txt
pathspec==0.12.1
    # via
    #   -r requirements.txt
    #   gitingest
prompt-toolkit==3.0.51
    # via
    #   -r requirements.txt
    #   click-repl
pycparser==2.22
    # via cffi
pydantic==2.11.7
    # via
    #   -r requirements.txt
    #   fastapi
    #   fastapi-cloud-cli
    #   gitingest
pydantic-core==2.33.2
    # via
    #   -r requirements.txt
    #   pydantic
pygments==2.19.2
    # via
    #   -r requirements.txt
    #   rich
pymysql==1.1.0
    # via -r requirements.txt
python-dateutil==2.9.0.post0
    # via
    #   -r requirements.txt
    #   celery
    #   pandas
python-dotenv==1.1.1
    # via
    #   -r requirements.txt
    #   gitingest
    #   uvicorn
python-multipart==0.0.20
    # via
    #   -r requirements.txt
    #   fastapi
pytz==2025.2
    # via
    #   -r requirements.txt
    #   pandas
pyyaml==6.0.2
    # via
    #   -r requirements.txt
    #   uvicorn
regex==2024.11.6
    # via
    #   -r requirements.txt
    #   tiktoken
requests==2.32.4
    # via
    #   -r requirements.txt
    #   tiktoken
rich==14.1.0
    # via
    #   -r requirements.txt
    #   rich-toolkit
    #   typer
rich-toolkit==0.14.9
    # via
    #   -r requirements.txt
    #   fastapi-cli
    #   fastapi-cloud-cli
rignore==0.6.4
    # via
    #   -r requirements.txt
    #   fastapi-cloud-cli
ruff==0.12.5
    # via -r requirements.txt
sentry-sdk==2.33.2
    # via
    #   -r requirements.txt
    #   fastapi-cloud-cli
shellingham==1.5.4
    # via
    #   -r requirements.txt
    #   typer
six==1.17.0
    # via
    #   -r requirements.txt
    #   python-dateutil
slowapi==0.1.9
    # via
    #   -r requirements.txt
    #   gitingest
sniffio==1.3.1
    # via
    #   -r requirements.txt
    #   anyio
sqlalchemy==2.0.41
    # via -r requirements.txt
starlette==0.47.2
    # via
    #   -r requirements.txt
    #   fastapi
    #   gitingest
structlog==25.4.0
    # via -r requirements.txt
tenacity==9.1.2
    # via -r requirements.txt
tiktoken==0.9.0
    # via
    #   -r requirements.txt
    #   gitingest
tomli==2.2.1
    # via
    #   -r requirements.txt
    #   gitingest
typer==0.16.0
    # via
    #   -r requirements.txt
    #   fastapi-cli
    #   fastapi-cloud-cli
typing-extensions==4.14.1
    # via
    #   -r requirements.txt
    #   fastapi
    #   limits
    #   pydantic
    #   pydantic-core
    #   rich-toolkit
    #   sqlalchemy
    #   typer
    #   typing-inspection
typing-inspection==0.4.1
    # via
    #   -r requirements.txt
    #   pydantic
tzdata==2025.2
    # via
    #   -r requirements.txt
    #   kombu
    #   pandas
urllib3==2.5.0
    # via
    #   -r requirements.txt
    #   requests
    #   sentry-sdk
uvicorn==0.35.0
    # via
    #   -r requirements.txt
    #   fastapi
    #   fastapi-cli
    #   fastapi-cloud-cli
    #   gitingest
uvloop==0.21.0
    # via
    #   -r requirements.txt
    #   uvicorn
vine==5.1.0
    # via
    #   -r requirements.txt
    #   amqp
    #   celery
    #   kombu
watchfiles==1.1.0
    # via
    #   -r requirements.txt
    #   uvicorn
wcwidth==0.2.13
    # via
    #   -r requirements.txt
    #   prompt-toolkit
websockets==15.0.1
    # via
    #   -r requirements.txt
    #   uvicorn
wrapt==1.17.2
    # via
    #   -r requirements.txt
    #   deprecated



================================================
FILE: .python-version
================================================
3.13



================================================
FILE: crawler/__init__.py
================================================




================================================
FILE: crawler/check_crawler_config.py
================================================
import structlog

from .logging_config import configure_logging
from .config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
)

configure_logging()

logger = structlog.get_logger(__name__) # Corrected: add __name__

logger.info(
    "RabbitMQ configuration check.", # Improved log message
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account="***masked***", # Masked sensitive info
    worker_password="***masked***", # Masked sensitive info
)


================================================
FILE: crawler/config.py
================================================
import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', 'local.ini')

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# Determine which section to use based on APP_ENV environment variable
# Default to 'DOCKER' if APP_ENV is not set or invalid
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。")
    app_env = "DOCKER"

config_section = config[app_env]

WORKER_ACCOUNT = config_section.get("WORKER_ACCOUNT")
WORKER_PASSWORD = config_section.get("WORKER_PASSWORD")

RABBITMQ_HOST = config_section.get("RABBITMQ_HOST")
RABBITMQ_PORT = int(config_section.get("RABBITMQ_PORT"))

MYSQL_HOST = config_section.get("MYSQL_HOST")
MYSQL_PORT = int(config_section.get("MYSQL_PORT"))
MYSQL_ACCOUNT = config_section.get("MYSQL_ACCOUNT")
MYSQL_ROOT_PASSWORD = config_section.get("MYSQL_ROOT_PASSWORD")
MYSQL_PASSWORD = config_section.get("MYSQL_PASSWORD")
MYSQL_DATABASE = config_section.get("MYSQL_DATABASE")
LOG_LEVEL = config_section.get("LOG_LEVEL", "INFO").upper()

PRODUCER_BATCH_SIZE = int(config_section.get("PRODUCER_BATCH_SIZE", "100"))
PRODUCER_DISPATCH_INTERVAL_SECONDS = float(config_section.get("PRODUCER_DISPATCH_INTERVAL_SECONDS", "1.0"))

URL_CRAWLER_REQUEST_TIMEOUT_SECONDS = int(config_section.get("URL_CRAWLER_REQUEST_TIMEOUT_SECONDS", "20"))
URL_CRAWLER_UPLOAD_BATCH_SIZE = int(config_section.get("URL_CRAWLER_UPLOAD_BATCH_SIZE", "30"))
URL_CRAWLER_SLEEP_MIN_SECONDS = float(config_section.get("URL_CRAWLER_SLEEP_MIN_SECONDS", "0.5"))
URL_CRAWLER_SLEEP_MAX_SECONDS = float(config_section.get("URL_CRAWLER_SLEEP_MAX_SECONDS", "1.5"))



================================================
FILE: crawler/logging_config.py
================================================
import logging
import structlog
import sys

# 從集中的設定模組導入日誌級別
from crawler.config import LOG_LEVEL

def configure_logging():
    """
    配置應用程式的日誌系統，整合 structlog 和標準 logging。
    日誌級別從 crawler.config 獲取。
    """
    # 映射日誌級別字串到 logging 模組的常數
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    numeric_log_level = log_level_map.get(LOG_LEVEL, logging.INFO)

    # 1. 配置 structlog 的處理器鏈
    #    - add_logger_name: 添加 logger 名稱
    #    - add_log_level: 添加日誌級別
    #    - ProcessorFormatter.wrap_for_formatter: 讓 structlog 的事件能被標準 logging 的 formatter 處理
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 2. 配置標準 logging 的 formatter 和 handler
    #    - 使用 structlog.dev.ConsoleRenderer 讓日誌在控制台輸出時更美觀
    #    - foreign_pre_chain 確保來自標準 logging 的日誌也能被 structlog 的處理器處理
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
        foreign_pre_chain=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
        ],
        # 這裡的 logger_factory 和 wrapper_class 應該被移除，因為它們不屬於 Formatter 的參數
        # logger_factory=structlog.stdlib.LoggerFactory(),
        # wrapper_class=structlog.stdlib.BoundLogger,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # 3. 配置 root logger
    #    - 移除 logging.basicConfig，避免重複配置
    #    - 設定 root logger 的級別為從 config 讀取的值
    #    - 添加 structlog 處理後的 handler
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_log_level)

    logger = structlog.get_logger(__name__)
    logger.info("日誌系統配置完成", level=LOG_LEVEL)


================================================
FILE: crawler/worker.py
================================================
from celery import Celery
import structlog

from crawler.logging_config import configure_logging
from crawler.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_ACCOUNT,
    WORKER_PASSWORD,
)

configure_logging()
logger = structlog.get_logger(__name__)

logger.info(
    "RabbitMQ configuration",
    rabbitmq_host=RABBITMQ_HOST,
    rabbitmq_port=RABBITMQ_PORT,
    worker_account="***masked***",
    worker_password="***masked***",
)

app = Celery(
    "task",
    include=[
        "crawler.project_104.task_category_104",
        "crawler.project_104.task_jobs_104",
        "crawler.project_104.task_urls_104",
    ],
    # Configure broker connection settings for robustness
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_connection_timeout=30,
)

# Set the broker URL using app.conf
app.conf.broker_url = f"pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"

app.conf.task_routes = {
    'crawler.project_104.task_104_jobs.fetch_104_data': {'queue': 'jobs_104'},
    'crawler.project_104.task_urls_104.crawl_and_store_category_urls': {'queue': 'urls_104'},
    # 如果有其他任務，可以在這裡添加更多路由
}


================================================
FILE: crawler/database/connection.py
================================================
import logging
import structlog
from contextlib import contextmanager

from tenacity import retry, stop_after_attempt, wait_exponential, before_log, RetryError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 直接從集中的設定模組導入，不再重複讀取 .ini
from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)
from crawler.database.models import Base

# --- 核心設定 ---
logger = structlog.get_logger(__name__)
metadata = Base.metadata
_engine = None  # 使用單例模式確保 Engine 只被建立一次

# SessionLocal 將在 get_session 中與 engine 綁定
SessionLocal = sessionmaker(autocommit=False, autoflush=False)


# --- 核心功能 ---

@contextmanager
def get_session():
    """
    提供一個資料庫 Session 的上下文管理器。
    它能自動處理 commit、rollback 和 session 關閉。
    """
    engine = get_engine()  # 確保 engine 已被初始化
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        logger.error("Session 發生錯誤，執行回滾 (rollback)", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


def get_engine():
    """
    獲取 SQLAlchemy 引擎實例，帶有連接重試機制。
    這是一個單例，確保在應用程式生命週期中只創建一次引擎。
    """
    global _engine
    if _engine is None:
        try:
            _engine = _connect_with_retry()
        except RetryError as e:
            logger.critical("資料庫連接在多次重試後失敗，應用程式無法啟動。", error=e, exc_info=True)
            raise RuntimeError("資料庫連接失敗，請檢查資料庫服務是否正常。") from e
        except Exception as e:
            logger.critical("創建資料庫引擎時發生未預期的錯誤。", error=e, exc_info=True)
            raise RuntimeError("創建資料庫引擎時發生致命錯誤。") from e
    return _engine


@retry(
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    before=before_log(logger, logging.INFO),
    reraise=True,
)
def _connect_with_retry():
    """
    （內部函式）執行實際的資料庫連接，由 tenacity 提供重試能力。
    """
    logger.info(f"正在嘗試連接到資料庫: {MYSQL_DATABASE}@{MYSQL_HOST}:{MYSQL_PORT}")

    # 假設資料庫已存在，直接連接
    # 使用 pymysql 驅動，並設定 utf8mb4 字符集
    db_url = (
        f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
    )

    engine = create_engine(
        db_url,
        pool_recycle=3600,  # 每小時回收一次連接，防止連接被 MySQL 伺服器中斷
        echo=False,
        connect_args={'connect_timeout': 10},
        isolation_level="READ COMMITTED",
    )

    # 測試連接，如果失敗會觸發 tenacity 重試
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("資料庫引擎創建成功，連接測試通過。")
    return engine


def initialize_database():
    """
    初始化資料庫，根據 models.py 中的定義創建所有資料表。
    此函式應在應用程式啟動時或透過專門的腳本手動調用。
    """
    logger.info("正在初始化資料庫，檢查並創建所有資料表...")
    try:
        engine = get_engine()
        metadata.create_all(engine)
        logger.info("資料庫資料表初始化完成。")
    except Exception as e:
        logger.critical("初始化資料庫資料表失敗。", error=e, exc_info=True)
        raise



================================================
FILE: crawler/database/get_category_ids.py
================================================
import structlog
import pandas as pd
import os

from crawler.database.connection import get_session, initialize_database # Import get_session
from crawler.database.models import CategorySource # Import the model
from crawler.logging_config import configure_logging # Import configure_logging

configure_logging() # Configure logging at the script level
logger = structlog.get_logger(__name__)

def get_source_category_ids():
    """
    從資料庫獲取所有職務分類的 ID 和名稱，並以 Pandas DataFrame 形式返回。
    """
    try:
        with get_session() as session: # Use get_session context manager
            # 使用 SQLAlchemy ORM 查詢資料
            categories = session.query(CategorySource).all()
            
            # 將 ORM 物件轉換為字典列表，然後再轉換為 DataFrame
            data = [
                {
                    "parent_source_id": cat.parent_source_id,
                    "source_category_id": cat.source_category_id,
                    "source_category_name": cat.source_category_name
                }
                for cat in categories
            ]
            df = pd.DataFrame(data)
            logger.info("Successfully fetched source category IDs.", count=len(df))
            return df
    except Exception as e:
        logger.error("Error fetching source_category_ids with ORM.", error=e, exc_info=True)
        return pd.DataFrame() # 在錯誤時返回空的 DataFrame

if __name__ == "__main__":
    # Set APP_ENV for local testing
    os.environ["APP_ENV"] = "DEV"
    
    # 確保資料庫在本地測試時被初始化
    initialize_database() 
    
    ids_df = get_source_category_ids()
    logger.info("Source Category IDs fetched.", dataframe_head=ids_df.head().to_dict('records'))
    # 如果需要完整輸出，可以使用 print(ids_df.to_string())


================================================
FILE: crawler/database/models.py
================================================
from sqlalchemy import Column, Integer, String, Text, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone # Import timezone
import enum
from typing import Optional
from pydantic import BaseModel, Field # Import Field

Base = declarative_base()

class SourcePlatform(str, enum.Enum):
    """資料來源平台。用於在資料庫中標識數據的來源。"""
    PLATFORM_104 = "platform_104"
    PLATFORM_1111 = "platform_1111"
    PLATFORM_CAKERESUME = "platform_cakeresume"
    PLATFORM_YES123 = "platform_yes123"

class JobStatus(str, enum.Enum):
    """職缺或 URL 的活躍狀態。"""
    ACTIVE = "active"
    INACTIVE = "inactive"

class CrawlStatus(str, enum.Enum):
    """職缺詳情頁的抓取狀態。"""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"

class SalaryType(str, enum.Enum):
    """標準化的薪資給付週期。"""
    MONTHLY = "MONTHLY"
    HOURLY = "HOURLY"
    YEARLY = "YEARLY"
    DAILY = "DAILY"
    BY_CASE = "BY_CASE"
    NEGOTIABLE = "NEGOTIABLE"

class JobType(str, enum.Enum):
    """標準化的工作類型。"""
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    CONTRACT = "CONTRACT"
    INTERNSHIP = "INTERNSHIP"
    TEMPORARY = "TEMPORARY"

# SQLAlchemy Models
class CategorySource(Base):
    __tablename__ = "tb_category_source"
    id = Column(Integer, primary_key=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False)
    source_category_id = Column(String(255), nullable=False)
    source_category_name = Column(String(255), nullable=False)
    parent_source_id = Column(String(255))

class Url(Base):
    __tablename__ = "tb_urls"
    source_url = Column(String(512), primary_key=True)
    source = Column(Enum(SourcePlatform), nullable=False, index=True)
    status = Column(Enum(JobStatus), nullable=False, index=True, default=JobStatus.ACTIVE)
    details_crawl_status = Column(Enum(CrawlStatus), nullable=False, index=True, default=CrawlStatus.PENDING)
    crawled_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False) # Use timezone-aware datetime
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False) # Use timezone-aware datetime and onupdate
    details_crawled_at = Column(DateTime)

class Job(Base):
    __tablename__ = "tb_jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_platform = Column(Enum(SourcePlatform), nullable=False, index=True)
    source_job_id = Column(String(255), index=True, nullable=False)
    url = Column(String(512), index=True, nullable=False)
    status = Column(Enum(JobStatus), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    job_type = Column(Enum(JobType)) # Changed to Enum(JobType)
    location_text = Column(String(255))
    posted_at = Column(DateTime)
    salary_text = Column(String(255))
    salary_min = Column(Integer)
    salary_max = Column(Integer)
    salary_type = Column(Enum(SalaryType)) # Changed to Enum(SalaryType)
    experience_required_text = Column(String(255))
    education_required_text = Column(String(255))
    company_source_id = Column(String(255))
    company_name = Column(String(255))
    company_url = Column(String(512))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False) # Use timezone-aware datetime
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False) # Use timezone-aware datetime and onupdate

# Pydantic Models
class CategorySourcePydantic(BaseModel):
    id: Optional[int] = None
    source_platform: SourcePlatform
    source_category_id: str
    source_category_name: str
    parent_source_id: Optional[str] = None

    class Config:
        from_attributes = True

class UrlPydantic(BaseModel):
    source_url: str
    source: SourcePlatform
    status: JobStatus = JobStatus.ACTIVE
    details_crawl_status: CrawlStatus = CrawlStatus.PENDING
    crawled_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) # Use default_factory
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) # Use default_factory
    details_crawled_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class JobPydantic(BaseModel):
    id: Optional[int] = None
    source_platform: SourcePlatform
    source_job_id: str
    url: str
    status: JobStatus
    title: str
    description: Optional[str] = None
    job_type: Optional[JobType] = None # Changed to Optional[JobType]
    location_text: Optional[str] = None
    posted_at: Optional[datetime] = None
    salary_text: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_type: Optional[SalaryType] = None # Changed to Optional[SalaryType]
    experience_required_text: Optional[str] = None
    education_required_text: Optional[str] = None
    company_source_id: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) # Use default_factory
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) # Use default_factory

    class Config:
        from_attributes = True



================================================
FILE: crawler/database/pandas_sql_config.py
================================================
import pandas as pd
import structlog
from sqlalchemy import create_engine

from crawler.logging_config import configure_logging
from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

configure_logging()
logger = structlog.get_logger(__name__)

def main():
    """
    示範如何透過 Pandas 直接連線到資料庫並讀取資料。
    """
    # 建立資料庫連接 URL
    # 使用 mysql+mysqlconnector 驅動
    db_url = (
        f"mysql+mysqlconnector://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 嘗試從資料庫讀取一個範例資料表
        # 請替換 'tb_category_source' 為你實際想要查詢的資料表名稱
        # 如果資料庫中沒有 'tb_category_source'，請替換為其他存在的資料表
        table_name = "tb_category_source"
        logger.info("Attempting to read data from database using Pandas.", table=table_name)
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", engine)

        logger.info("Successfully read data from database.", table=table_name, rows_read=len(df))
        logger.info("DataFrame head:", dataframe_head=df.head().to_dict('records'))

    except Exception as e:
        logger.error("An error occurred during database connection or query.", error=e, exc_info=True)

    finally:
        # 關閉引擎連接池
        engine.dispose()
        logger.info("Database engine disposed.")

if __name__ == "__main__":
    # python -m crawler.database.pandas_sql_config
    main()



================================================
FILE: crawler/database/pandas_sql_demo.py
================================================
import pandas as pd
import structlog
from sqlalchemy import create_engine

from crawler.logging_config import configure_logging
# from crawler.config import ( # 暫時不從 config 匯入，直接硬編碼用於測試
#     MYSQL_HOST,
#     MYSQL_PORT,
#     MYSQL_ACCOUNT,
#     MYSQL_PASSWORD,
#     MYSQL_DATABASE,
# )

configure_logging()
logger = structlog.get_logger(__name__)

def main():
    """
    示範如何透過 Pandas 直接連線到資料庫並讀取資料。
    """
    # --- 僅用於本次測試的硬編碼連線資訊 ---
    # 在實際應用中，這些值應從 crawler.config 匯入
    test_mysql_host = "127.0.0.1"
    test_mysql_port = 3306
    test_mysql_account = "root"
    test_mysql_password = "root_password"
    test_mysql_database = "crawler_db"
    # ---------------------------------------

    # 建立資料庫連接 URL
    # 使用 mysql+mysqlconnector 驅動
    db_url = (
        f"mysql+mysqlconnector://{test_mysql_account}:{test_mysql_password}@"
        f"{test_mysql_host}:{test_mysql_port}/{test_mysql_database}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 嘗試從資料庫讀取一個範例資料表
        # 請替換 'tb_category_source' 為你實際想要查詢的資料表名稱
        # 如果資料庫中沒有 'tb_category_source'，請替換為其他存在的資料表
        table_name = "tb_category_source"
        logger.info("Attempting to read data from database using Pandas (hardcoded for test).", table=table_name)
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", engine)

        logger.info("Successfully read data from database.", table=table_name, rows_read=len(df))
        logger.info("DataFrame head:", dataframe_head=df.head().to_dict('records'))

    except Exception as e:
        logger.error("An error occurred during database connection or query.", error=e, exc_info=True)

    finally:
        # 關閉引擎連接池
        engine.dispose()
        logger.info("Database engine disposed.")

if __name__ == "__main__":
    # python -m crawler.database.pandas_sql_demo
    main()



================================================
FILE: crawler/database/repository.py
================================================
import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert

from crawler.database.connection import get_session

from crawler.database.models import (
    CategorySource,
    Url,
    Job,
    SourcePlatform,
    JobStatus,
    CrawlStatus,
    JobPydantic,
    CategorySourcePydantic,
)

logger = structlog.get_logger(__name__)


def sync_source_categories(
    platform: SourcePlatform, flattened_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    將抓取到的職務分類數據同步到資料庫。
    執行 UPSERT 操作，如果分類已存在則更新，否則插入。
    """
    if not flattened_data:
        logger.info("No flattened data to sync for categories.", platform=platform.value)
        return {"total": 0, "affected": 0}

    with get_session() as session:
        stmt = insert(CategorySource).values(flattened_data)
        update_dict = {
            "source_category_name": stmt.inserted.source_category_name,
            "parent_source_id": stmt.inserted.parent_source_id,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.info(
            "Categories synced successfully.",
            platform=platform.value,
            total_categories=len(flattened_data),
        )
        return {"total": len(flattened_data), "affected": 0}


def get_source_categories(
    platform: SourcePlatform, source_ids: Optional[List[str]] = None
) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台和可選的 source_ids 的職務分類。
    返回 CategorySourcePydantic 實例列表，以便在 Session 關閉後安全使用。
    """
    with get_session() as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        if source_ids:
            stmt = stmt.where(CategorySource.source_category_id.in_(source_ids))
        
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug("Fetched source categories.", platform=platform.value, count=len(categories), source_ids=source_ids)
        return categories

def get_all_categories_for_platform(platform: SourcePlatform) -> List[CategorySourcePydantic]:
    """
    從資料庫獲取指定平台的所有職務分類。
    返回 CategorySourcePydantic 實例列表。
    """
    with get_session() as session:
        stmt = select(CategorySource).where(CategorySource.source_platform == platform)
        categories = [
            CategorySourcePydantic.model_validate(cat)
            for cat in session.scalars(stmt).all()
        ]
        logger.debug("Fetched all categories for platform.", platform=platform.value, count=len(categories))
        return categories

def upsert_urls(platform: SourcePlatform, urls: List[str]) -> None:
    """
    Synchronizes a list of URLs for a given platform with the database.
    Performs an UPSERT operation. URLs are marked as ACTIVE and PENDING.
    """
    if not urls:
        logger.info("No URLs to upsert.", platform=platform.value)
        return

    now = datetime.now(timezone.utc)
    url_models_to_upsert = [
        {
            "source_url": url,
            "source": platform,
            "status": JobStatus.ACTIVE,
            "details_crawl_status": CrawlStatus.PENDING,
            "crawled_at": now,
            "updated_at": now,
        }
        for url in urls
    ]

    with get_session() as session:
        stmt = insert(Url).values(url_models_to_upsert)
        update_dict = {
            "status": stmt.inserted.status,
            "updated_at": stmt.inserted.updated_at,
            "details_crawl_status": stmt.inserted.details_crawl_status,
        }
        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.info("URLs upserted successfully.", platform=platform.value, count=len(urls))


def get_unprocessed_urls(platform: SourcePlatform, limit: int) -> List[str]: # Changed return type to List[str]
    """
    從資料庫獲取指定平台未處理的 URL 列表。
    返回 URL 字串列表。
    """
    with get_session() as session:
        statement = (
            select(Url.source_url) # Select only the source_url column
            .where(Url.source == platform, Url.details_crawl_status == CrawlStatus.PENDING)
            .limit(limit)
        )
        urls = list(session.scalars(statement).all())
        logger.debug("Fetched unprocessed URLs.", platform=platform.value, count=len(urls), limit=limit)
        return urls


def upsert_jobs(jobs: List[JobPydantic]) -> None:
    """
    將 Job 對象列表同步到資料庫。
    執行 UPSERT 操作，如果職位已存在則更新，否則插入。
    """
    if not jobs:
        logger.info("No jobs to upsert.", count=0)
        return

    now = datetime.now(timezone.utc)
    job_dicts_to_upsert = [
        {
            **job.model_dump(exclude_none=False),
            "updated_at": now,
            "created_at": job.created_at or now,
        }
        for job in jobs
    ]

    with get_session() as session:
        stmt = insert(Job).values(job_dicts_to_upsert)

        update_cols = {
            column.name: getattr(stmt.inserted, column.name)
            for column in Job.__table__.columns
            if not column.primary_key
        }

        final_stmt = stmt.on_duplicate_key_update(**update_cols)
        session.execute(final_stmt)
        logger.info("Jobs upserted successfully.", count=len(job_dicts_to_upsert))


def mark_urls_as_crawled(processed_urls: Dict[CrawlStatus, List[str]]) -> None:
    """
    根據處理狀態標記 URL 為已爬取。
    """
    if not processed_urls:
        logger.info("No URLs to mark as crawled.")
        return

    now = datetime.now(timezone.utc)
    with get_session() as session:
        for status, urls in processed_urls.items():
            if urls:
                stmt = (
                    update(Url)
                    .where(Url.source_url.in_(urls))
                    .values(details_crawl_status=status, details_crawled_at=now)
                )
                session.execute(stmt)
                logger.info("URLs marked as crawled.", status=status.value, count=len(urls))


================================================
FILE: crawler/database/temp_count_db.py
================================================
import structlog
import os
from sqlalchemy import text

from crawler.database.connection import get_session # Use get_session
from crawler.logging_config import configure_logging # Import configure_logging

configure_logging() # Call configure_logging at the beginning
logger = structlog.get_logger(__name__)

# Set APP_ENV for local testing (if not already set by environment)
os.environ.setdefault("APP_ENV", "DEV") # Use setdefault to avoid overwriting if already set

logger.info("Starting database count check.")

try:
    with get_session() as session:
        category_count = session.execute(text('SELECT COUNT(*) FROM tb_category_source')).scalar()
        url_count = session.execute(text('SELECT COUNT(*) FROM tb_urls')).scalar()

        logger.info("Database counts.", category_count=category_count, url_count=url_count)
except Exception as e:
    logger.error("An error occurred while counting database records.", error=e, exc_info=True)


================================================
FILE: crawler/database/test_upload_data_to_mysql.py
================================================
import pandas as pd
import requests
import structlog
from datetime import datetime

from ..worker import app
from crawler.database.connection import get_session
from crawler.logging_config import configure_logging
from crawler.finmind.config import ( # Changed import path
    FINMIND_API_BASE_URL,
    FINMIND_START_DATE,
    FINMIND_END_DATE,
)

configure_logging()
logger = structlog.get_logger(__name__)

def upload_data_to_mysql(df: pd.DataFrame):
    if df.empty:
        logger.info("DataFrame is empty, skipping upload to MySQL.")
        return

    try:
        with get_session() as session:
            df.to_sql(
                "TaiwanStockPrice",
                con=session.connection(),
                if_exists="append",
                index=False,
            )
            logger.info("Data uploaded to MySQL successfully.", table="TaiwanStockPrice", rows_uploaded=len(df))
    except Exception as e:
        logger.error("Failed to upload data to MySQL.", error=e, exc_info=True)
        raise

@app.task()
def crawler_finmind(stock_id: str):
    logger.info("Starting FinMind data crawl.", stock_id=stock_id)

    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": FINMIND_START_DATE,
        "end_date": FINMIND_END_DATE,
    }

    try:
        resp = requests.get(FINMIND_API_BASE_URL, params=parameter, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if resp.status_code == 200:
            df = pd.DataFrame(data.get("data", []))
            logger.info("FinMind API data fetched.", stock_id=stock_id, rows_fetched=len(df))
            upload_data_to_mysql(df)
        else:
            logger.error("FinMind API returned an error.", status_code=resp.status_code, message=data.get("msg"), stock_id=stock_id)
    except requests.exceptions.RequestException as e:
        logger.error("Network or API request error.", error=e, stock_id=stock_id, exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred during FinMind crawl.", error=e, stock_id=stock_id, exc_info=True)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        logger.info("Usage: python -m crawler.database.test_upload_data_to_mysql <stock_id>")
        sys.exit(1)

    stock_id_for_test = sys.argv[1]
    logger.info("Dispatching crawler_finmind task for local testing.", stock_id=stock_id_for_test)
    crawler_finmind.delay(stock_id_for_test)



================================================
FILE: crawler/database/test_upload_duplicate_data.py
================================================
import pandas as pd
import structlog
from sqlalchemy import (
    Column,
    Date,
    Float,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.mysql import insert

from crawler.logging_config import configure_logging
from crawler.database.connection import get_session, initialize_database # Import get_session and initialize_database

configure_logging()
logger = structlog.get_logger(__name__)

# 定義資料表結構，對應到 MySQL 中的 test_duplicate 表
metadata = MetaData()
stock_price_table = Table(
    "test_duplicate",  # 資料表名稱
    metadata,
    Column("stock_id", String(50), primary_key=True),  # 主鍵 stock_id 欄位
    Column("date", Date, primary_key=True),
    Column("price", Float),
)

# 建立 DataFrame，模擬要寫入的資料
df = pd.DataFrame(
    [
        # 模擬 5 筆重複資料
        {"stock_id": "2330", "date": "2025-06-25", "price": 1000},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1001},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1002},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1003},
        {"stock_id": "2330", "date": "2025-06-25", "price": 1004},
    ]
)

if __name__ == "__main__":
    # 確保資料庫表在測試前被建立
    initialize_database()

    logger.info("Starting test for duplicate data upload with UPSERT.")

    try:
        with get_session() as session:
            # 使用 bulk insert with on_duplicate_key_update
            # 將 DataFrame 轉換為字典列表，以便 insert 語句處理
            insert_stmt = insert(stock_price_table).values(df.to_dict(orient="records"))

            # 定義在主鍵重複時要更新的欄位。這裡只更新 'price' 欄位。
            on_duplicate_update_dict = {
                "price": insert_stmt.inserted.price
            }

            final_stmt = insert_stmt.on_duplicate_key_update(**on_duplicate_update_dict)
            session.execute(final_stmt)
            # session.commit() 由 get_session 上下文管理器自動處理

        logger.info("Data upserted successfully.", rows_processed=len(df))

        # 從資料庫讀取資料並列印
        with get_session() as session:
            # pd.read_sql 可以直接使用 session 的 connection
            read_df = pd.read_sql("SELECT * FROM test_duplicate", con=session.connection())
            logger.info("Data read from database.", dataframe_content=read_df.to_dict(orient='records'))

    except Exception as e:
        logger.error("An error occurred during duplicate data test.", error=e, exc_info=True)



================================================
FILE: crawler/finmind/config.py
================================================
# crawler/finmind/config.py
import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

# 讀取專案根目錄下的 local.ini
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'local.ini')

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# 根據 APP_ENV 選擇區塊
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。")
    app_env = "DOCKER"

config_section = config[app_env]

# FinMind 相關設定
FINMIND_API_BASE_URL = config_section.get("FINMIND_API_BASE_URL", "https://api.finmindtrade.com/api/v4/data")
FINMIND_START_DATE = config_section.get("FINMIND_START_DATE", "2024-01-01")
FINMIND_END_DATE = config_section.get("FINMIND_END_DATE", "2025-06-17")



================================================
FILE: crawler/project_104/config_104.py
================================================
# crawler/project_104/config.py
import os
import configparser
import structlog

logger = structlog.get_logger(__name__)

# 讀取專案根目錄下的 local.ini
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'local.ini')

try:
    config.read(config_path)
except Exception as e:
    logger.critical(f"無法讀取 local.ini 設定檔: {e}", exc_info=True)
    raise RuntimeError("無法讀取設定檔。") from e

# 根據 APP_ENV 選擇區塊
app_env = os.environ.get("APP_ENV", "DOCKER").upper()
if app_env not in config:
    logger.warning(f"環境變數 APP_ENV={app_env} 無效或未找到對應區塊，預設使用 [DOCKER] 設定。")
    app_env = "DOCKER"

config_section = config[app_env]

# 104 平台相關設定
JOB_CAT_URL_104 = config_section.get("JOB_CAT_URL_104", "https://static.104.com.tw/category-tool/json/JobCat.json")
JOB_API_BASE_URL_104 = config_section.get("JOB_API_BASE_URL_104", "https://www.104.com.tw/job/ajax/content/")
WEB_NAME_104 = config_section.get("WEB_NAME_104", "104_人力銀行")

HEADERS_104 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Referer': 'https://www.104.com.tw/jobs/search',
}

HEADERS_104_JOB_API = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Referer': 'https://www.104.com.tw/job/'
}

HEADERS_104_URL_CRAWLER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "https://www.104.com.tw/",
}

URL_CRAWLER_BASE_URL_104 = config_section.get("URL_CRAWLER_BASE_URL_104", "https://www.104.com.tw/jobs/search/api/jobs")
URL_CRAWLER_PAGE_SIZE_104 = int(config_section.get("URL_CRAWLER_PAGE_SIZE_104", "20"))
URL_CRAWLER_ORDER_BY_104 = int(config_section.get("URL_CRAWLER_ORDER_BY_104", "16")) # 16 (最近更新)
URL_PRODUCER_CATEGORY_LIMIT = int(config_section.get("URL_PRODUCER_CATEGORY_LIMIT", "1")) # Default to 1 for local testing. Set to 0 for no limit.



================================================
FILE: crawler/project_104/local_fetch_104_url_data.py
================================================
import requests
import sys
from requests.exceptions import HTTPError, JSONDecodeError
import structlog

from crawler.worker import app
from crawler.logging_config import configure_logging # Import configure_logging
from crawler.config import JOB_API_BASE_URL_104 # Import the base URL from config

configure_logging() # Call configure_logging at the beginning
logger = structlog.get_logger(__name__)

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def get_job_api_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'referer': 'https://www.104.com.tw/'
    }

    job_id = url.split('/')[-1].split('?')[0]
    # Use the configured base URL
    url_api = f'{JOB_API_BASE_URL_104}{job_id}'

    try:
        response = requests.get(url_api, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (HTTPError, JSONDecodeError) as err:
        logger.error("Failed to fetch job API data", url=url_api, error=err) # Improved log message
        return {}

    job_data = data.get('data', {})
    if not job_data or job_data.get('custSwitch', {}) == "off":
        logger.info("Job content does not exist or is closed", job_id=job_id) # Improved log message
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

    logger.info("Extracted job information", job_id=job_id, extracted_info=extracted_info) # Improved log message
    return extracted_info # Return the extracted info

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info("Usage: python local_fetch_104_url_data.py <job_url>") # Updated usage message
        sys.exit(1)

    job_url = sys.argv[1]
    logger.info("Dispatching job API data task", job_url=job_url)
    get_job_api_data.delay(job_url)



================================================
FILE: crawler/project_104/producer_category_104.py
================================================
from .task_category_104 import fetch_url_data_104
import structlog
import time # Added import time

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import JOB_CAT_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

time.sleep(5) # Added a 5-second delay
fetch_url_data_104.delay(JOB_CAT_URL_104)
logger.info("send task_category_104 url", url=JOB_CAT_URL_104)


================================================
FILE: crawler/project_104/producer_jobs_104.py
================================================
import structlog
import time

from crawler.project_104.task_jobs_104 import fetch_url_data_104
from crawler.database.repository import get_unprocessed_urls
from crawler.database.models import SourcePlatform
from crawler.database.connection import initialize_database
from crawler.logging_config import configure_logging
from crawler.config import PRODUCER_BATCH_SIZE, PRODUCER_DISPATCH_INTERVAL_SECONDS

configure_logging()
logger = structlog.get_logger(__name__)

def dispatch_job_urls():
    logger.info("開始從資料庫讀取未處理的職缺 URL 並分發任務...")

    while True:
        # 從資料庫獲取一批未處理的 URL (現在是字串列表)
        urls_to_process = get_unprocessed_urls(SourcePlatform.PLATFORM_104, PRODUCER_BATCH_SIZE)

        if not urls_to_process:
            logger.info("所有未處理的職缺 URL 已分發完畢。")
            break

        for url_str in urls_to_process: # Iterate over URL strings directly
            logger.info("分發職缺 URL 任務", url=url_str)
            fetch_url_data_104.delay(url_str)

        logger.info("已分發一批職缺 URL", count=len(urls_to_process))
        time.sleep(PRODUCER_DISPATCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    initialize_database()
    dispatch_job_urls()



================================================
FILE: crawler/project_104/producer_urls_104.py
================================================
from crawler.database.repository import get_all_categories_for_platform
from crawler.project_104.task_urls_104 import crawl_and_store_category_urls
from crawler.database.models import SourcePlatform
import structlog
from typing import Dict, List

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import URL_PRODUCER_CATEGORY_LIMIT # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

def dispatch_urls_for_all_categories() -> None:
    """
    分發所有 104 職務類別的 URL 抓取任務。

    從資料庫中獲取所有 104 平台的類別，並為每個類別分發一個 Celery 任務，
    由 `crawl_and_store_category_urls` 任務負責實際的 URL 抓取。

    :return: 無。
    :rtype: None
    """
    logger.info("Starting URL task distribution for all 104 categories.")

    all_104_categories = get_all_categories_for_platform(SourcePlatform.PLATFORM_104)

    if all_104_categories:
        logger.info("Found categories for PLATFORM_104.", count=len(all_104_categories))
        root_categories = [
            cat for cat in all_104_categories if cat.parent_source_id is None
        ]

        if root_categories:
            logger.info("Found root categories for PLATFORM_104.", count=len(root_categories))
            
            categories_to_dispatch = root_categories
            if URL_PRODUCER_CATEGORY_LIMIT > 0:
                categories_to_dispatch = root_categories[:URL_PRODUCER_CATEGORY_LIMIT]
                logger.info("Applying category limit for dispatch.", limit=URL_PRODUCER_CATEGORY_LIMIT, actual_count=len(categories_to_dispatch))

            for category_info in categories_to_dispatch:
                category_id: str = category_info.source_category_id
                logger.info("分發 URL 抓取任務", category_id=category_id)
                crawl_and_store_category_urls.delay(category_id)
        else:
            logger.info("No root categories found for PLATFORM_104.")
    else:
        logger.info("No categories found for PLATFORM_104.")

if __name__ == "__main__":
    dispatch_urls_for_all_categories()


================================================
FILE: crawler/project_104/single_url_api_data_104.py
================================================
import requests
import sys
import structlog
import json

from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException, JSONDecodeError

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import HEADERS_104_JOB_API, JOB_API_BASE_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

def fetch_url_data_104(url: str) -> dict:
    """
    從 104 職缺 API 抓取單一 URL 的資料。
    """
    job_id = url.split('/')[-1].split('?')[0]
    url_api = f'{JOB_API_BASE_URL_104}{job_id}'

    logger.info("Fetching data for single URL.", url=url, job_id=job_id, api_url=url_api)

    try:
        response = requests.get(url_api, headers=HEADERS_104_JOB_API, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched data.", job_id=job_id, data_keys=list(data.keys()))
        return data
    except (HTTPError, ConnectionError, Timeout, RequestException) as e:
        logger.error("Network or request error occurred.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}
    except JSONDecodeError as e:
        logger.error("Failed to decode JSON response.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}
    except Exception as e:
        logger.error("An unexpected error occurred.", url=url, api_url=url_api, error=e, exc_info=True)
        return {}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info("Usage: python -m crawler.project_104.single_url_api_data_104 <job_url>")
        sys.exit(1)

    job_url = sys.argv[1]
    data = fetch_url_data_104(job_url)
    if data:
        logger.info("Fetched data content (sample).", job_url=job_url, data_sample=json.dumps(data, indent=2, ensure_ascii=False)[:500])
    else:
        logger.warning("No data fetched for the given URL.", job_url=job_url)



================================================
FILE: crawler/project_104/single_url_api_data_104.txt
================================================
{'data': {'corpImageRight': {'corpImageRight': {'imageUrl': '', 'link': ''}}, 'header': {'corpImageTop': {'imageUrl': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/custintroduce/image2.jpg?v=20241107162243', 'link': ''}, 'jobName': '軟體自動化測試工程師(新竹)', 'appearDate': '2025/07/29', 'custName': '全景軟體股份有限公司', 'custUrl': 'https://www.104.com.tw/company/7hzjbag', 'analysisType': 1, 'analysisUrl': '//www.104.com.tw/jobs/apply/analysis/2ews9', 'isSaved': False, 'isFollowed': False, 'isApplied': False, 'applyDate': '', 'userApplyCount': 0, 'hrBehaviorPR': 0.016831555955287163}, 'contact': {'hrName': '温先生', 'email': '', 'visit': '', 'phone': [], 'other': '', 'reply': ''}, 'environmentPic': {'environmentPic': [{'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_892440736021303088.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_892440736021303088.jpg?v=20241107162243', 'description': '25周年運動會'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_964488071933812501.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_964488071933812501.jpg?v=20241107162243', 'description': 'Lobby-1'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_964488071933812502.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_964488071933812502.jpg?v=20241107162243', 'description': 'Lobby-2'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_892440736021303089.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_892440736021303089.jpg?v=20241107162243', 'description': '員工交誼廳'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_892440736021303090.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_892440736021303090.jpg?v=20241107162243', 'description': '尾牙活動'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_892440736021303091.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_892440736021303091.jpg?v=20241107162243', 'description': '社團活動-羽球社'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_893506157025877465.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_893506157025877465.jpg?v=20241107162243', 'description': '春聯DIY'}, {'thumbnailLink': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/s_893506157025877466.jpg?v=20241107162243', 'link': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/env/l_893506157025877466.jpg?v=20241107162243', 'description': '員工旅遊'}], 'corpImageBottom': {'imageUrl': '', 'link': ''}}, 'condition': {'acceptRole': {'role': [{'code': 2, 'description': '應屆畢業生'}, {'code': 64, 'description': '原住民'}], 'disRole': {'needHandicapCompendium': False, 'disability': []}}, 'workExp': '不拘', 'edu': '大學以上', 'major': ['資訊工程相關', '資訊管理相關'], 'language': [{'code': 1, 'language': '英文', 'ability': {'listening': '中等', 'speaking': '中等', 'reading': '中等', 'writing': '中等'}}], 'localLanguage': [], 'specialty': [], 'skill': [], 'certificate': [], 'driverLicense': [], 'other': '1.熟悉 Windows及Linux系統。\n2.具備自動化測試程式開發相關經驗者佳。(JUnit/Selenium/Sikulix/Jmeter/Postman等)\n3.具備RESTful API測試經驗、基本SQL指令操作者佳。\n4.具備DevOps相關經驗者佳。\n5.規劃產品測試相關流程，評估測試結果。\n6.有密碼學或網路安全相關經驗者佳。\n\n人格特質：\n1.發想靈活，具邏輯分析能力。\n2.具掌握時程的能力，有效控管、細心、負責任。\n3.具解決問題及應變能力，有耐心及抗壓性。\n4.具團隊合作精神，能接受指導及良好的溝通能力。\n5.熱情正面，積極追求新知與接受考驗。'}, 'welfare': {'tag': [], 'welfare': '【努力工作也要充電休息】\n· 新人入職當天，即享有特休假配套福利\n· 全年彈性放假不用補班，別人要上班，我們悠閒吃早午餐\n· 給薪活力假，充電完畢，活力滿滿回崗位\n· 彈性上下班時間，讓你更能兼顧家庭生活\n· 年度休假日數優於勞基法\n\n【優於同業的各項獎勵】\n· 年終獎金、績效獎金、員工分紅\n· 介紹獎金、專利獎金\n· 生日禮金、佳節禮金\n· 完善的調薪制度\n每位員工皆是全景軟體的重要資產，我們鼓勵員工努力的付出，並期許能與全景互相成長。\n\n【員工照顧】\n· 年度健檢及團保，我們在乎每一位同仁的健康\n· 員工婚喪喜慶補助、生育津貼等，你的家人就是我們的家人\n\n【在全景的生活多采多姿】\n· 小當家社、燃脂社、壘球社、桌遊社等多元社團活動\n· 定期下午茶、零食點心，在上班時刻撫慰你的身心靈\n· 電影包場欣賞，不用人擠人排隊買票\n· 國內外旅遊補助，想去哪就去哪\n· 聖誕節派對、年度尾牙等活動，讓我們一起共度重要節慶\n\n【各項技能的訓練及提升】\n· 新進同仁訓：職前訓練、制訂個別培訓計畫\n· 在職同仁訓：e-learning線上學習平台、專業技術分享、多樣化課程\n· 各階主管訓：激發領導統御力，提升跨部門合作效率\n', 'legalTag': []}, 'jobDetail': {'jobDescription': '1.建立測試環境，各項產品運作架構熟悉。\n2.負責系統自動化測試系統及相關系統操作(Jenkins, Git, VM, Docker)。\n3.自動化程式撰寫Selenium, Python, Postman, Script等。\n4.產品相關測試(壓力、負載、效能)，測試規範建立、改善流程。', 'jobCategory': [{'code': '2007001004', 'description': '軟體工程師'}, {'code': '2007001006', 'description': 'Internet程式設計師'}], 'salary': '月薪35,000~50,000元', 'salaryMin': 35000, 'salaryMax': 50000, 'salaryType': 50, 'jobType': 1, 'workType': [], 'addressNo': '6001006001', 'addressRegion': '新竹市', 'addressArea': '新竹市', 'addressDetail': '新竹科學園區園區二路48號2樓', 'industryArea': '新竹科學園區', 'longitude': '121.0067597', 'latitude': '24.7739304', 'manageResp': '不需負擔管理責任', 'businessTrip': '無需出差外派', 'workPeriod': '日班，09:00~18:00', 'vacationPolicy': '週休二日', 'startWorkingDay': '不限', 'hireType': 0, 'delegatedRecruit': '', 'needEmp': '不限', 'landmark': '', 'remoteWork': None}, 'switch': 'on', 'custLogo': 'https://static.104.com.tw/b_profile/cust_picture/9000/16325089000/logo.png?v=20241107162243', 'postalCode': '300', 'closeDate': '2021-04-08', 'industry': '電腦軟體服務業', 'custNo': '16325089000', 'reportUrl': 'https://www.104.com.tw/feedback?category=2&custName=%E5%85%A8%E6%99%AF%E8%BB%9F%E9%AB%94%E8%82%A1%E4%BB%BD%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8&jobName=%E8%BB%9F%E9%AB%94%E8%87%AA%E5%8B%95%E5%8C%96%E6%B8%AC%E8%A9%A6%E5%B7%A5%E7%A8%8B%E5%B8%AB%28%E6%96%B0%E7%AB%B9%29', 'industryNo': '1001001002', 'employees': '170人', 'chinaCorp': False, 'interactionRecord': {'lastProcessedResumeAtTime': None, 'lastCustReplyTimestamp': 1751353490, 'nowTimestamp': 1753789855}}, 'metadata': {'enableHTML': False, 'hiddenBanner': False, 'seo': {'noindex': False}}}


================================================
FILE: crawler/project_104/task_category_104.py
================================================
import json
import requests
import structlog

from crawler.worker import app
# from crawler.logging_config import configure_logging # Removed this import
from crawler.database.models import SourcePlatform
from crawler.database.repository import get_source_categories, sync_source_categories
from crawler.project_104.config_104 import HEADERS_104, JOB_CAT_URL_104 # Changed import path

# configure_logging() # Removed this call
logger = structlog.get_logger(__name__)

def flatten_jobcat_recursive(node_list, parent_no=None):
    """
    Recursively flattens the category tree using a generator.
    """
    for node in node_list:
        yield {
            "parent_source_id": parent_no,
            "source_category_id": node.get("no"),
            "source_category_name": node.get("des"),
        }
        if "n" in node and node.get("n"):
            yield from flatten_jobcat_recursive(
                node_list=node["n"],
                parent_no=node.get("no"),
            )


@app.task()
def fetch_url_data_104(url_JobCat):
    logger.info("Fetching category data", url=url_JobCat)

    try:
        existing_categories = get_source_categories(SourcePlatform.PLATFORM_104)

        response_jobcat = requests.get(url_JobCat, headers=HEADERS_104, timeout=10)
        response_jobcat.raise_for_status()
        jobcat_data = response_jobcat.json()
        flattened_data = list(flatten_jobcat_recursive(jobcat_data))

        if not existing_categories:
            logger.info("Database is empty. Performing initial bulk sync.")
            sync_source_categories(SourcePlatform.PLATFORM_104, flattened_data)
            return

        api_categories_set = {
            (d["source_category_id"], d["source_category_name"], d["parent_source_id"])
            for d in flattened_data if d.get("parent_source_id")
        }
        db_categories_set = {
            (category.source_category_id, category.source_category_name, category.parent_source_id)
            for category in existing_categories
        }

        categories_to_sync_set = api_categories_set - db_categories_set

        if categories_to_sync_set:
            categories_to_sync = [
                {
                    "source_category_id": cat_id,
                    "source_category_name": name,
                    "parent_source_id": parent_id,
                    "source_platform": SourcePlatform.PLATFORM_104.value,
                }
                for cat_id, name, parent_id in categories_to_sync_set
            ]
            logger.info("Found new or updated categories to sync.", count=len(categories_to_sync))
            sync_source_categories(SourcePlatform.PLATFORM_104, categories_to_sync)
        else:
            logger.info("No new or updated categories to sync.")

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data from URL.", url=url_JobCat, error=e, exc_info=True)
    except json.JSONDecodeError as e:
        logger.error("Error decoding JSON from URL.", url=url_JobCat, error=e, exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred.", error=e, exc_info=True)

# if __name__ == "__main__":
#     logger.info("Dispatching fetch_url_data_104 task for local testing.")
#     fetch_url_data_104.delay(JOB_CAT_URL_104)


================================================
FILE: crawler/project_104/task_jobs_104.py
================================================
import json
import requests
import structlog
from crawler.worker import app
from crawler.database.models import SourcePlatform, JobPydantic, JobStatus
from crawler.database.repository import upsert_jobs, mark_urls_as_crawled

from typing import Optional
import re
from datetime import datetime
from crawler.database.models import SalaryType, CrawlStatus

from crawler.logging_config import configure_logging
from crawler.project_104.config_104 import HEADERS_104_JOB_API, JOB_API_BASE_URL_104 # Changed import path

configure_logging()
logger = structlog.get_logger(__name__)

def parse_salary(salary_text: str) -> (Optional[int], Optional[int], Optional[SalaryType]):
    salary_min, salary_max, salary_type = None, None, None
    text = salary_text.replace(",", "").lower()

    # 月薪
    match_monthly = re.search(r'月薪([0-9]+)(?:[至~])([0-9]+)元', text) or re.search(r'月薪([0-9]+)元以上', text)
    if match_monthly:
        salary_type = SalaryType.MONTHLY
        salary_min = int(match_monthly.group(1))
        if len(match_monthly.groups()) > 1 and match_monthly.group(2):
            salary_max = int(match_monthly.group(2))
        return salary_min, salary_max, salary_type

    # 年薪
    match_yearly = re.search(r'年薪([0-9]+)萬(?:[至~])([0-9]+)萬', text) or re.search(r'年薪([0-9]+)萬以上', text)
    if match_yearly:
        salary_type = SalaryType.YEARLY
        salary_min = int(match_yearly.group(1)) * 10000
        if len(match_yearly.groups()) > 1 and match_yearly.group(2):
            salary_max = int(match_yearly.group(2)) * 10000
        return salary_min, salary_max, salary_type

    # 時薪
    match_hourly = re.search(r'時薪([0-9]+)元', text)
    if match_hourly:
        salary_type = SalaryType.HOURLY
        salary_min = int(match_hourly.group(1))
        salary_max = int(match_hourly.group(1))
        return salary_min, salary_max, salary_type

    # 日薪
    match_daily = re.search(r'日薪([0-9]+)元', text)
    if match_daily:
        salary_type = SalaryType.DAILY
        salary_min = int(match_daily.group(1))
        salary_max = int(match_daily.group(1))
        return salary_min, salary_max, salary_type

    # 論件計酬
    if "論件計酬" in text:
        salary_type = SalaryType.BY_CASE
        return None, None, salary_type

    # 面議
    if "面議" in text:
        salary_type = SalaryType.NEGOTIABLE
        return None, None, salary_type

    return salary_min, salary_max, salary_type

@app.task()
def fetch_url_data_104(url: str) -> Optional[JobPydantic]:
    try:
        job_id = url.split('/')[-1].split('?')[0]
        if not job_id:
            logger.error("Failed to extract job_id from URL.", url=url)
            return None

        api_url = f'{JOB_API_BASE_URL_104}{job_id}'

        logger.info("Fetching job data.", job_id=job_id, source_url=api_url)

        response = requests.get(api_url, headers=HEADERS_104_JOB_API, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error("Network error when requesting API.", url=api_url, error=e, exc_info=True)
        return None
    except json.JSONDecodeError:
        logger.error("Failed to parse JSON response.", url=api_url, exc_info=True)
        return None

    job_data = data.get('data')
    if not job_data or job_data.get('switch') == "off":
        logger.warning("Job content does not exist or is closed.", job_id=job_id)
        return None

    try:
        header = job_data.get('header', {})
        job_detail = job_data.get('jobDetail', {})
        condition = job_data.get('condition', {})

        job_addr_region = job_detail.get('addressRegion', '')
        job_address_detail = job_detail.get('addressDetail', '')
        location_text = (job_addr_region + job_address_detail).strip()
        if not location_text:
            location_text = None

        posted_at = None
        appear_date_str = header.get('appearDate')
        if appear_date_str:
            try:
                posted_at = datetime.strptime(appear_date_str, '%Y/%m/%d')
            except ValueError:
                logger.warning("Could not parse posted_at date format.", appear_date=appear_date_str, job_id=job_id)

        salary_min, salary_max, salary_type = parse_salary(job_detail.get('salary', ''))

        job_pydantic_data = JobPydantic(
            source_platform=SourcePlatform.PLATFORM_104,
            source_job_id=job_id,
            url=url,
            status=JobStatus.ACTIVE,
            title=header.get('jobName'),
            description=job_detail.get('jobDescription'),
            job_type=job_detail.get('jobType'),
            location_text=location_text,
            posted_at=posted_at,
            salary_text=job_detail.get('salary'),
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
            experience_required_text=condition.get('workExp'),
            education_required_text=condition.get('edu'),
            company_source_id=header.get('custNo'),
            company_name=header.get('custName'),
            company_url=header.get('custUrl'),
        )

        upsert_jobs([job_pydantic_data])
        logger.info("Successfully parsed and upserted job.", job_title=job_pydantic_data.title, job_id=job_id)
        mark_urls_as_crawled({CrawlStatus.COMPLETED: [url]})
        return job_pydantic_data.model_dump()

    except (AttributeError, KeyError) as e:
        logger.error("Missing key fields when parsing data.", error=e, job_id=job_id, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return {}
    except Exception as e:
        logger.error("Unexpected error when processing job data.", error=e, job_id=job_id, exc_info=True)
        mark_urls_as_crawled({CrawlStatus.FAILED: [url]})
        return {}

# if __name__ == "__main__":
#     from crawler.database.connection import initialize_database
#     from crawler.database.repository import get_unprocessed_urls

#     initialize_database()
#     logger.info("Local testing task_jobs_104. Fetching unprocessed URLs from database.")

#     urls_to_test = get_unprocessed_urls(SourcePlatform.PLATFORM_104, 5)

#     if urls_to_test:
#         for url_obj in urls_to_test:
#             logger.info("Dispatching test URL task.", url=url_obj.source_url)
#             fetch_url_data_104.delay(url_obj.source_url)
#     else:
#         logger.info("No unprocessed URLs available for testing. Please run task_urls_104 first to populate data.")



================================================
FILE: crawler/project_104/task_urls_104.py
================================================
import random
import time
import structlog
import sys
from collections import deque

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from crawler.worker import app
from crawler.database.models import SourcePlatform
from crawler.database.repository import upsert_urls
from crawler.logging_config import configure_logging
from crawler.config import (
    URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
    URL_CRAWLER_UPLOAD_BATCH_SIZE,
    URL_CRAWLER_SLEEP_MIN_SECONDS,
    URL_CRAWLER_SLEEP_MAX_SECONDS,
)
from crawler.project_104.config_104 import (
    URL_CRAWLER_BASE_URL_104,
    URL_CRAWLER_PAGE_SIZE_104,
    URL_CRAWLER_ORDER_BY_104,
    HEADERS_104_URL_CRAWLER,
)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

configure_logging()
logger = structlog.get_logger(__name__)

@app.task
def crawl_and_store_category_urls(job_category_code: str) -> None:
    """
    Celery 任務：遍歷指定職缺類別的所有頁面，抓取職缺網址，並將其儲存到資料庫。
    """
    global_job_url_set = set()
    current_batch_urls = []
    recent_counts = deque(maxlen=4)

    current_page = 1
    logger.info("Task started: crawling job category URLs.", job_category_code=job_category_code)

    while True:
        if current_page % 5 == 1:
            logger.info("Current page being processed.", page=current_page, job_category_code=job_category_code)

        params = {
            'jobsource': 'index_s',
            'page': current_page,
            'pagesize': URL_CRAWLER_PAGE_SIZE_104,
            'order': URL_CRAWLER_ORDER_BY_104,
            'jobcat': job_category_code,
            'mode': 's',
            'searchJobs': '1',
        }

        try:
            response = requests.get(
                URL_CRAWLER_BASE_URL_104,
                headers=HEADERS_104_URL_CRAWLER,
                params=params,
                timeout=URL_CRAWLER_REQUEST_TIMEOUT_SECONDS,
                verify=False
            )
            response.raise_for_status()
            api_data = response.json()

            api_job_urls = api_data.get('data')
            if not isinstance(api_job_urls, list):
                logger.error(
                    "API response 'data' format is incorrect or missing.",
                    page=current_page,
                    job_category_code=job_category_code,
                    api_data_type=type(api_job_urls),
                    api_data_sample=str(api_job_urls)[:100]
                )
                break

            for job_url_item in api_job_urls:
                job_link = job_url_item.get('link', {}).get('job')
                if job_link:
                    if job_link not in global_job_url_set:
                        global_job_url_set.add(job_link)
                        current_batch_urls.append(job_link)

            if len(current_batch_urls) >= URL_CRAWLER_UPLOAD_BATCH_SIZE:
                logger.info("Batch upload size reached. Starting upload.", count=len(current_batch_urls), job_category_code=job_category_code)
                upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
                current_batch_urls.clear()

        except requests.exceptions.HTTPError as http_err:
            logger.error("HTTP error occurred.", error=str(http_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.ConnectionError as conn_err:
            logger.error("Connection error occurred.", error=str(conn_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.Timeout as timeout_err:
            logger.error("Request timeout error occurred.", error=str(timeout_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except requests.exceptions.RequestException as req_err:
            logger.error("Unknown request error occurred.", error=str(req_err), page=current_page, job_category_code=job_category_code, exc_info=True)
            break
        except ValueError:
            logger.error("Failed to decode JSON response.", page=current_page, job_category_code=job_category_code, exc_info=True)
        except Exception as e:
            logger.error("An unexpected error occurred while fetching page.", error=str(e), page=current_page, job_category_code=job_category_code, exc_info=True)

        total_jobs = len(global_job_url_set)
        recent_counts.append(total_jobs)
        if len(recent_counts) == recent_counts.maxlen and len(set(recent_counts)) == 1:
            logger.info("No new data found consecutively. Ending task early.", max_len=recent_counts.maxlen, job_category_code=job_category_code)
            break

        time.sleep(random.uniform(URL_CRAWLER_SLEEP_MIN_SECONDS, URL_CRAWLER_SLEEP_MAX_SECONDS))

        current_page += 1

    if current_batch_urls:
        logger.info("Task completed. Storing remaining raw job URLs to database.", count=len(current_batch_urls), job_category_code=job_category_code)
        upsert_urls(SourcePlatform.PLATFORM_104, current_batch_urls)
    else:
        logger.info("Task completed. No URLs collected, skipping database storage.", job_category_code=job_category_code)

    logger.info("Task execution finished.", job_category_code=job_category_code)

# if __name__ == '__main__':
#     import sys
#     if len(sys.argv) < 2:
#         logger.info("Usage: python -m crawler.project_104.task_urls_104 <job_category_code>")
#         sys.exit(1)

#     JOBCAT_CODE_FOR_TEST = sys.argv[1]
#     logger.info("Dispatching crawl_and_store_category_urls task for local testing.", job_category_code=JOBCAT_CODE_FOR_TEST)
#     crawl_and_store_category_urls.delay(JOBCAT_CODE_FOR_TEST)


================================================
FILE: docs/development_manual.md
================================================
# Crawler System 開發手冊

## 1. 總體哲學 (Philosophy)

本專案所有開發與重構工作，應遵循以下核心哲學：

- **清晰性 (Clarity)**：程式碼首先是寫給人看的，其次才是給機器執行的。優先選擇清晰、易於理解的寫法，避免過度炫技或使用晦澀的語法。
- **單一職責 (Single Responsibility)**：每個模組、每個類別、每個函式都應該只有一個明確的職責。這使得程式碼更容易測試、重用和維護。
- **穩定性 (Robustness)**：應用程式應具備容錯能力。對於外部依賴（如資料庫、訊息佇列），必須有適當的重試和錯誤處理機制。
- **配置外部化 (Externalized Configuration)**：程式碼本身不應包含任何環境特定的設定（如密碼、主機位址）。所有設定都應透過外部設定檔管理。

---

## 2. 環境設定 (Environment Setup)

### 2.1. 必要工具

- **Python**: 版本定義於 `.python-version`。
- **uv**: 用於管理 Python 虛擬環境和套件。
- **Docker & Docker Compose**: 用於啟動外部服務（MySQL, RabbitMQ）。

### 2.2. 初始化步驟

1.  **啟動基礎服務**:
    ```bash
    # 啟動 MySQL 和 RabbitMQ 服務
    docker-compose -f mysql-network.yml up -d
    docker-compose -f rabbitmq-network.yml up -d
    ```

2.  **建立虛擬環境並安裝依賴**:
    ```bash
    # 建立 .venv 虛擬環境
    uv venv

    # 啟用虛擬環境
    source .venv/bin/activate

    # 安裝專案依賴
    uv pip install -r requirements.txt
    ```

3.  **設定環境變數**:
    複製 `local.ini.example` (如果有的話) 為 `local.ini`，並根據本地開發需求修改。`APP_ENV` 環境變數用於切換不同的設定區塊。

---

## 3. 設定檔管理 (Configuration)

- **`local.ini`**: 這是唯一的設定來源 (Single Source of Truth)。它被分為不同的區塊，例如 `[DEV]`, `[DOCKER]`, `[PROD]`。
- **`crawler/config.py`**: 這是讀取 `local.ini` 的唯一模組。專案中任何其他地方需要設定值時，都應該**直接從 `crawler.config` 匯入**，而不是自己重新讀取 `.ini` 檔案。
- **`APP_ENV` 環境變數**: 這個環境變數決定了 `config.py` 要讀取 `local.ini` 中的哪一個區塊。預設為 `DOCKER`。

---

## 4. 核心編碼原則 (Core Coding Principles)

### 4.1. 單一職責原則 (SRP)

- **模組層級**: `crawler/database/connection.py` 只負責資料庫連接，`crawler/config.py` 只負責設定讀取。
- **函式層級**: 一個函式只做一件事情。例如，`get_engine` 只負責取得引擎，而不應該包含建立資料庫的邏輯。

### 4.2. DRY (Don't Repeat Yourself)

- **避免重複程式碼**: 如果一段邏輯在兩個以上的地方出現，就應該將它抽像成一個函式或類別。
- **範例**: 專案中所有讀取設定的地方都應從 `crawler.config` 匯入，這就是 DRY 的體現。

### 4.3. 日誌記錄 (Logging)

- **使用 `structlog`**: 全面使用 `structlog` 進行結構化日誌記錄，而不是 `print()`。
- **日誌級別**:
    - `logger.debug()`: 用於開發時的詳細除錯資訊。
    - `logger.info()`: 用於記錄關鍵的業務流程節點（例如「服務啟動」、「收到新任務」）。
    - `logger.warning()`: 用於記錄可預期的、但需要注意的異常情況（例如「設定檔缺少某個非關鍵值，使用預設值」）。
    - `logger.error()`: 用於記錄發生了錯誤，但應用程式仍可繼續運行的情況（例如「處理單一任務失敗，但 worker 會繼續接收下一個任務」）。
    - `logger.critical()`: 用於記錄導致應用程式無法繼續運行的致命錯誤（例如「資料庫連接失敗」）。
- **包含上下文**: 在記錄日誌時，盡可能帶上關鍵的上下文資訊，例如 `logger.info("任務處理完成", task_id=123, duration_ms=500)`。

---

## 5. 資料庫互動 (Database Interaction)

本專案**強制使用 Pydantic 模型**來定義和處理所有與資料庫互動的資料結構。這確保了資料的類型安全、一致性以及自動驗證。

-   **使用 `get_session`**: 所有對資料庫的讀寫操作，都必須透過 `crawler.database.connection.get_session` 的上下文管理器來完成。
    ```python
    from crawler.database.connection import get_session
    from crawler.database.models import MyDataPydantic # 假設這是你的 Pydantic 模型

    with get_session() as session:
        # 從資料庫讀取資料後，應立即轉換為 Pydantic 模型
        orm_object = session.query(MyORMModel).first()
        if orm_object:
            pydantic_instance = MyDataPydantic.model_validate(orm_object)
            # 現在你可以安全地使用 pydantic_instance

        # 寫入資料庫時，應使用 Pydantic 模型定義的資料
        new_data = MyDataPydantic(field1="value1", field2="value2")
        session.add(MyORMModel(**new_data.model_dump())) # 將 Pydantic 轉換為 ORM 可接受的格式
        # session.commit() 和 session.rollback() 會由 get_session 自動處理
    ```
-   **禁止直接使用 `engine.execute()`**: 除非是像 `initialize_database` 這樣的一次性管理腳本，否則業務邏輯中應避免直接使用 `engine`。
-   **Pydantic 的優勢**: 
    -   **類型安全**: 明確定義資料類型，減少運行時錯誤。
    -   **資料驗證**: 自動驗證輸入資料是否符合預期結構和類型。
    -   **清晰的資料結構**: 讓程式碼更易於理解和維護。
    -   **與 API 整合**: Pydantic 模型可以輕鬆地用於定義 RESTful API 的請求和響應模型。

### 5.1. 透過 Pandas 直接連線資料庫 (僅限讀取或特定用途)

在某些特定場景下，例如進行資料分析或快速查詢時，你可能希望直接透過 Pandas 連線到資料庫。此時，你可以使用 `sqlalchemy` 的 `create_engine` 搭配專案的設定來建立連接。

**注意**：這種方式通常用於讀取資料，對於寫入操作，仍建議使用 `get_session` 和 ORM，並透過 Pydantic 模型來確保事務的完整性和一致性。

1.  **安裝必要套件**: 
    ```bash
    uv pip install pandas sqlalchemy mysql-connector-python
    ```

2.  **範例程式碼**: 
    ```python
    import pandas as pd
    from sqlalchemy import create_engine
    from crawler.config import (
        MYSQL_HOST,
        MYSQL_PORT,
        MYSQL_ACCOUNT,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
    )

    # 建立資料庫連接 URL
    # 使用 mysql+mysqlconnector 驅動
    db_url = (
        f"mysql+mysqlconnector://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    # 建立 SQLAlchemy 引擎
    engine = create_engine(db_url)

    try:
        # 使用 pandas 讀取資料表
        # 替換 'your_table_name' 為你實際的資料表名稱
        df = pd.read_sql("SELECT * FROM your_table_name", engine)

        # 顯示 DataFrame 的前幾行
        print(df.head())

    except Exception as e:
        print(f"連線或查詢時發生錯誤: {e}")

    finally:
        # 關閉引擎連接池
        engine.dispose()
    ```

---

## 6. 執行爬蟲 (Running the Crawler)

為了確保 Python 的 `import` 路徑正確，應從專案根目錄使用 `-m` 參數來執行模組。

- **啟動 Producer**:
  ```bash
  python -m crawler.project_104.producer_category_104
  ```
- **啟動 Worker**:
  ```bash
  APP_ENV=DEV celery -A crawler.worker worker --loglevel=info
  ```

---

## 7. 程式碼風格與檢查 (Linting & Formatting)

為了確保程式碼的一致性、可讀性和品質，本專案強制執行自動化的程式碼風格檢查和格式化。

### 7.1. 為什麼需要程式碼風格與檢查？

-   **提高可讀性**：統一的風格讓所有開發者更容易閱讀和理解程式碼。
-   **減少錯誤**：Linter 可以捕捉潛在的錯誤、不一致的行為和不良的程式碼實踐。
-   **加速開發**：減少程式碼審查中關於風格的討論，讓開發者專注於業務邏輯。
-   **自動化**：透過工具自動執行，減少人工干預。

### 7.2. 推薦工具

-   **`ruff` (Linter & Formatter)**: 一個極速的 Python Linter 和 Formatter，旨在取代 `Flake8`, `isort`, `pylint`, `black` 等多個工具，提供統一的程式碼檢查和格式化體驗。

### 7.3. 安裝與使用

請確保你的虛擬環境已啟用。

1.  **安裝工具**:
    ```bash
    uv pip install ruff
    ```

2.  **配置 `ruff`**:
    `ruff` 的配置通常放在 `pyproject.toml` 中。請確保 `pyproject.toml` 中包含以下或類似的配置：
    ```toml
    [tool.ruff]
    line-length = 120
    select = ["E", "F", "W", "I", "N", "D", "UP", "ANN", "ASYNC", "B", "C4", "DTZ", "ERA", "ISC", "ICN", "PIE", "PT", "RSE", "RET", "SIM", "TID", "ARG", "PLC", "PLE", "PLR", "PLW", "TRY", "PERF"]
    ignore = [] # ruff format 會處理行長度，所以不需要忽略 E501

    [tool.ruff.per-file-ignores]
    "__init__.py" = ["F401"] # 忽略 __init__.py 中未使用的 import 警告
    "tests/*" = ["S101"] # 忽略測試檔案中的 assert 警告
    ```
    （**注意**：上述 `select` 和 `ignore` 列表僅為範例，應根據專案實際需求進行調整。）

3.  **執行檢查與格式化**: 

    -   **格式化 (使用 `ruff`)**:
        ```bash
        ruff format .
        ```
        這會自動格式化專案中的所有 Python 檔案。

    -   **檢查 (使用 `ruff`)**:
        ```bash
        ruff check .
        ```
        這會檢查程式碼中的潛在問題。如果發現問題，`ruff` 會提供建議。

    -   **自動修復 (使用 `ruff`)**:
        ```bash
        ruff check . --fix
        ```
        `ruff` 可以自動修復大部分簡單的問題。

### 7.4. 開發流程整合

強烈建議在提交程式碼前執行 `ruff format .` 和 `ruff check . --fix`。

未來可以考慮整合 `pre-commit` hooks 或 CI/CD 流程，在程式碼提交或推送到遠端倉庫時自動執行這些檢查，以確保程式碼品質。



================================================
FILE: docs/project_104_local_test_plan.md
================================================
# Project 104 本地測試計畫

本文件旨在提供 `project_104` 相關 Producer 和 Task 的本地測試步驟，確保任務分發、Worker 執行及資料庫寫入流程正常運作。

## 測試前準備

1.  **確保所有 Docker 服務已啟動**：
    ```bash
    docker compose -f mysql-network.yml up -d
    docker compose -f rabbitmq-network.yml up -d
    ```

2.  **確保 `local.ini` 配置正確**：
    在 `local.ini` 的 `[DEV]` 區塊中，確保 `RABBITMQ_HOST` 和 `MYSQL_HOST` 都設定為 `127.0.0.1`，以便本地 Python 腳本能連接到 Docker 容器。

3.  **配置測試資料量限制 (可選)**：
    為了加快本地測試速度，你可以在 `local.ini` 的 `[DEV]` 區塊中配置以下參數，限制 Producer 分發的任務數量：
    *   `URL_PRODUCER_CATEGORY_LIMIT`: 限制 `producer_urls_104` 分發的類別數量。預設為 0 (不限制)。設定為正整數表示限制數量。
    *   `PRODUCER_BATCH_SIZE`: 限制 `producer_jobs_104` 每次從資料庫讀取並分發的職缺 URL 數量。
    ```ini
    [DEV]
    # ... 其他設定 ...
    URL_PRODUCER_CATEGORY_LIMIT = 2  # 例如，只分發 2 個類別的 URL 抓取任務
    PRODUCER_BATCH_SIZE = 10       # 例如，每次只處理 10 個職缺 URL
    # ... 其他設定 ...
    ```

4.  **啟動 Celery Worker**：
    在一個**獨立的終端視窗**中，啟動 Celery Worker。讓此視窗保持開啟，以便觀察 Worker 的日誌輸出。
    ```bash
    APP_ENV=DEV celery -A crawler.worker worker --loglevel=info
    ```

## 測試步驟

### 測試 `producer_category_104` (抓取職務類別)

1.  **執行 Producer**：
    在另一個終端視窗中執行以下命令：
    ```bash
    APP_ENV=DEV python -m crawler.project_104.producer_category_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `fetch_url_data_104` 任務被接收、執行，以及日誌中顯示類別資料被同步到資料庫的訊息。

3.  **驗證資料庫**：
    使用以下命令檢查 `tb_category_source` 表中是否有新的資料：
    ```bash
    APP_ENV=DEV python -m crawler.database.pandas_sql_config
    # 或者使用 temp_count_db.py 檢查數量
    APP_ENV=DEV python -m crawler.database.temp_count_db
    ```

### 測試 `producer_urls_104` (抓取職缺 URL)

1.  **執行 Producer**：
    在一個新的終端視窗中執行以下命令：
    ```bash
    APP_ENV=DEV python -m crawler.project_104.producer_urls_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `crawl_and_store_category_urls` 任務被接收、執行，以及日誌中顯示 URL 被儲存到資料庫的訊息。

3.  **驗證資料庫**：
    使用以下命令檢查 `tb_urls` 表中是否有新的 URL 資料：
    ```bash
    APP_ENV=DEV python -m crawler.database.pandas_sql_config
    # 或者使用 temp_count_db.py 檢查數量
    APP_ENV=DEV python -m crawler.database.temp_count_db
    ```

### 測試 `producer_jobs_104` (抓取職缺詳情)

**注意**：此測試依賴於 `producer_urls_104` 已經將足夠的未處理 URL 寫入 `tb_urls` 表。

1.  **執行 Producer**：
    在一個新的終端視窗中執行以下命令：
    ```bash
    APP_ENV=DEV python -m crawler.project_104.producer_jobs_104
    ```

2.  **觀察 Worker 日誌**：
    回到 Celery Worker 的終端視窗，觀察是否有 `fetch_url_data_104` 任務被接收、執行，以及日誌中顯示職缺詳情被儲存到資料庫的訊息。

3.  **驗證資料庫**：
    *   檢查 `tb_jobs` 表中是否有新的職缺詳情資料。
    *   檢查 `tb_urls` 表中對應的 URL 的 `details_crawl_status` 是否已更新為 `COMPLETED`。

## Celery Worker 命名與監控

### Worker 命名

Celery Worker 的標準命名格式是 `celery@hostname`。如果你希望自定義 Worker 的顯示名稱，可以使用 `-n` 或 `--hostname` 參數。

例如，為 `project_104` 專門啟動一個 Worker，並給它一個識別名稱：
```bash
APP_ENV=DEV celery -A crawler.worker worker -n project_104_worker@%h --loglevel=info
```

**關於 `"project_104.{{.Task.Slot}}"` 這樣的動態命名**：
Celery Worker 的 `--hostname` 參數用於設定 Worker 實例的靜態名稱，不直接支援這種基於任務槽位的動態模板。這種模式通常與容器編排工具（如 Docker Swarm, Kubernetes）在部署多個 Worker 實例時，為每個容器或 Pod 動態生成主機名有關。在 Celery 層面，你主要透過**隊列 (Queues)** 來控制 Worker 處理哪些任務。

如果你想讓一個 Worker 專門處理 `project_104` 的任務，最常見且推薦的做法是讓它監聽 `project_104` 相關的隊列（例如 `urls_104`, `jobs_104`）。

例如，啟動一個只監聽 `urls_104` 和 `jobs_104` 隊列的 Worker：
```bash
APP_ENV=DEV celery -A crawler.worker worker -Q urls_104,jobs_104 --loglevel=info
```

### 監控任務與 Worker 狀態

1.  **Celery Inspect 命令**：
    這些命令可以直接在終端中執行，用於查詢 Worker 的狀態和任務資訊。
    ```bash
    # 顯示所有活躍的 Worker
    APP_ENV=DEV celery -A crawler.worker inspect active_queues

    # 顯示所有活躍的任務（正在執行的任務）
    APP_ENV=DEV celery -A crawler.worker inspect active

    # 顯示所有已註冊的任務（Worker 知道的任務）
    APP_ENV=DEV celery -A crawler.worker inspect registered

    # 顯示所有排隊等待執行的任務
    APP_ENV=DEV celery -A crawler.worker inspect scheduled

    # 顯示所有被 Worker 預留但尚未執行的任務
    APP_ENV=DEV celery -A crawler.worker inspect reserved

    # 顯示 Worker 的統計資訊
    APP_ENV=DEV celery -A crawler.worker inspect stats
    ```

2.  **Flower UI**：
    Flower 是一個基於 Web 的 Celery 監控工具，提供更直觀的介面來查看 Worker、任務、隊列等狀態。你已經在 `rabbitmq-network.yml` 中配置了 Flower。
    *   確保 Flower 容器正在運行：
        ```bash
        docker compose -f rabbitmq-network.yml up -d
        ```
    *   在瀏覽器中訪問：`http://localhost:5555` (如果你的 Flower 端口映射是 5555)。
    *   在 Flower 介面中，你可以看到活躍的 Worker、任務的狀態、隊列的訊息數量等。


