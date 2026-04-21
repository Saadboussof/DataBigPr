FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python minio
COPY archiver.py .
CMD ["python", "-u", "archiver.py"]
