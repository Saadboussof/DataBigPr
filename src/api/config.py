import os

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

JWT_SECRET = os.getenv("JWT_SECRET", "taasim-super-secret-key-change-me-for-security")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = 120

# Mock User DB (username -> password, role)
USERS_DB = {
    "rider1": {"password": "rider123", "role": "rider"},
    "admin1": {"password": "admin123", "role": "admin"},
}
