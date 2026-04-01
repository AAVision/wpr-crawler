FROM python:3.12-slim

WORKDIR /opt/dagster

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster dependency resolution and installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy and install Python dependencies using uv
COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

# Install Playwright browser (needed for JS-rendered search pages)
RUN playwright install-deps && playwright install chromium

# Copy project files
COPY dagster_project/ ./dagster_project/
COPY scrapy_project/ ./scrapy_project/
COPY transformer/ ./transformer/
COPY scripts/ ./scripts/
COPY config.yaml .

# Set Python path
ENV PYTHONPATH="/opt/dagster:${PYTHONPATH}"

# Create logs directory
RUN mkdir -p /opt/dagster/logs

EXPOSE 3000