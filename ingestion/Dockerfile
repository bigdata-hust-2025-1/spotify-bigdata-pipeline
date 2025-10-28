# ingestion/Dockerfile
FROM python:3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY crawl_spotify.py .
CMD ["python", "crawl_spotify.py"]