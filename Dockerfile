FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY main.py .
COPY src ./src
COPY schedulejob ./schedulejob
COPY PROJECT_SETUP_ARCHITECTURE.md .
COPY README.md .

EXPOSE 8000

CMD ["uvicorn", "src.webapp.app:get_app", "--host", "0.0.0.0", "--port", "8000", "--factory", "--loop", "asyncio"]
