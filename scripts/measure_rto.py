import time
from minio import Minio

# Configure MinIO client for Region 2
region2 = Minio("localhost:9002", access_key="minioadmin", secret_key="minioadmin", secure=False)

# Measure RTO
def measure_rto(bucket_name, object_name):
    start_time = time.time()
    try:
        data = region2.get_object(bucket_name, object_name)
        data.read()  # Ensure data is fully fetched
        end_time = time.time()
        print(f"RTO: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Failed to recover file: {e}")

if __name__ == "__main__":
    measure_rto("test-bucket", "example1.txt")
