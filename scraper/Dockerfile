FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests

RUN mkdir -p /app/failed_html \
    && groupadd --system scraper \
    && useradd --system --gid scraper --home-dir /app scraper \
    && chown -R scraper:scraper /app

USER scraper

CMD ["python", "-m", "app.main"]
