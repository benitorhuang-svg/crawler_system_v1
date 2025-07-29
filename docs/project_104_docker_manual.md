# 專案 104 Docker 化操作手冊

本文件旨在說明如何將 `crawler_jobs` 專案打包成 Docker 映像檔，並如何使用 Docker Compose 管理整個爬蟲系統。

## 1. 前置準備 (Prerequisites)

請確保您的系統已安裝 Docker 和 Docker Compose (或 Docker CLI 內建的 `compose` 外掛)。

## 2. 環境設定 (Environment Setup)

### 2.1. 產生環境變數檔案 (`.env`)

專案使用 `local.ini` 來管理不同環境的設定。在 Docker 環境中，我們需要將 `local.ini` 中的 `[DOCKER]` 區塊設定轉換為 Docker Compose 可讀取的 `.env` 檔案。

在專案根目錄下執行：

```bash
APP_ENV=DOCKER python genenv.py
```

這將會根據 `local.ini` 中的 `[DOCKER]` 區塊產生一個 `.env` 檔案。

### 2.2. 建立共用網路

為了讓所有 Docker 容器能夠互相通訊，我們需要建立一個共用的 Docker 網路。

```bash
docker network create my_network || true
```
`|| true` 確保即使網路已存在也不會報錯。

## 3. Docker 映像檔建置 (Build)

我們使用多階段建置 (Multi-stage Build) 的 `Dockerfile` 來優化映像檔的大小和建置效率。

### 3.1. 建置指令

在專案的根目錄下，執行以下指令來建置映像檔：

```bash
# -f 指定 Dockerfile 的路徑
# -t 為映像檔命名並加上標籤 (tag)，格式為 <your-dockerhub-username>/<image-name>:<version>
# . 表示建置上下文 (build context) 為當前目錄
docker build -f Dockerfile -t benitorhuang/crawler_jobs:0.0.2 .
```

### 3.2. 驗證建置結果

建置完成後，你可以使用以下指令來查看本機的所有映像檔，確認 `benitorhuang/crawler_jobs:0.0.2` 是否已成功建立。

```bash
docker images
```

## 4. 啟動與管理服務 (Docker Compose)

我們使用 Docker Compose 來同時管理多個服務 (MySQL, RabbitMQ, Worker, Producer)。

### 4.1. 啟動核心服務 (MySQL, RabbitMQ, Flower)

首先，啟動資料庫和訊息佇列服務。這些服務通常會長時間運行。

```bash
# 啟動 MySQL 和 phpMyAdmin
docker compose -f mysql-network.yml up -d

# 啟動 RabbitMQ 和 Flower (Celery 監控工具)
docker compose -f rabbitmq-network.yml up -d
```
`-d` 參數表示在背景以分離模式 (detached mode) 執行。

### 4.2. 啟動應用服務 (Worker, Producer)

接下來，啟動我們的爬蟲應用服務。

#### 4.2.1. 啟動 Worker

Worker 是長時間運行的背景服務，它會持續監聽並處理來自 RabbitMQ 的任務。

```bash
# 啟動 Worker 服務
docker compose -f docker-compose-worker-network.yml up -d
```
**注意**：Worker 容器會監聽 `jobs_104` 佇列。

#### 4.2.2. 執行 Producer

Producer 的職責是讀取資料庫中的 URL 並分派任務。它是一個短時間執行的腳本，執行完畢後容器就會停止。

```bash
# 執行 Producer 服務
docker compose -f docker-compose-producer-network.yml up
```
Producer 會將任務發送到 `jobs_104` 佇列。

### 4.3. 查看服務日誌

你可以使用以下指令來查看特定服務的日誌輸出：

```bash
# 查看 Worker 服務的日誌
docker logs -f crawler_system-crawler_104-1

# 查看 Producer 服務的日誌 (如果它還在運行或剛結束)
docker logs -f crawler_system-producer_104_jobs-1

# 查看 RabbitMQ 服務的日誌
docker logs -f crawler_system-rabbitmq-1
```
**提示**：`crawler_system-` 是 Docker Compose 預設的專案名稱前綴。`crawler_104-1` 和 `producer_104_jobs-1` 是服務名稱加上實例編號。

### 4.4. 停止所有服務

當你完成測試或開發後，可以使用以下指令停止所有由 Docker Compose 啟動的服務：

```bash
# 停止並移除所有服務容器、網路和卷 (如果沒有被其他服務使用)
docker compose -f mysql-network.yml -f rabbitmq-network.yml -f docker-compose-worker-network.yml -f docker-compose-producer-network.yml down
```
**注意**：`down` 命令會停止並移除容器。如果你想保留容器，只停止它們，可以使用 `stop` 命令。

## 5. 推送至 Docker Hub (Optional)

如果你希望將此映像檔分享給團隊或部署到其他環境，可以將其推送到 Docker Hub。

```bash
# 首先登入 Docker Hub
docker login

# 推送映像檔
docker push benitorhuang/crawler_jobs:0.0.2
```
