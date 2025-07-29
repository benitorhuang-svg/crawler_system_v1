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