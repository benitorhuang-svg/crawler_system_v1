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
     python -m crawler.worker
     python -m crawler.project_104.producer_category_104




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
    python -m crawler.database.test_upload_data_to_mysql
    python -m crawler.database.test_upload_duplicate_data


####   worker/producer  <啟動 / 關閉>
    celery -A crawler.worker worker --loglevel=info

    ENV=DEV python genenv.py
    python -m crawler.project_104.task_category_104
    python -m crawler.project_104.task_urls_104
    python -m crawler.project_104.task_jobs_104


    docker compose -f docker-compose-worker-network.yml up -d
    docker compose -f docker-compose-worker-network.yml down
    docker compose -f docker-compose-producer-network.yml up -d
    docker compose -f docker-compose-producer-network.yml down
    
#### 加上環境版本 DOCKER_IMAGE_VERSION=***
    DOCKER_IMAGE_VERSION=0.0.1 docker compose -f docker-compose-worker-network-version.yml up -d
    DOCKER_IMAGE_VERSION=0.0.1 docker compose -f docker-compose-worker-network-version.yml down
    DOCKER_IMAGE_VERSION=0.0.3 docker compose -f docker-compose-producer-network-version.yml up -d
    DOCKER_IMAGE_VERSION=0.0.3 docker compose -f docker-compose-producer-network-version.yml down

