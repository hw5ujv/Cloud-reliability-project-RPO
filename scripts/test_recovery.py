from minio import Minio

# Configure MinIO client for Region 2
region2 = Minio("localhost:9002", access_key="minioadmin", secret_key="minioadmin", secure=False)

# Function to test recovery
def test_recovery(bucket_name, object_name):
    try:
        obj = region2.get_object(bucket_name, object_name)
        with open(f"recovered_{object_name}", "wb") as file_data:
            for d in obj.stream(32 * 1024):
                file_data.write(d)
        print(f"Successfully recovered {object_name} from Region 2")
    except Exception as e:
        print(f"Failed to recover {object_name}: {e}")

# Main script
if __name__ == "__main__":
    bucket_name = "test-bucket"
    object_name = "example.txt"

    # Recover the data
    test_recovery(bucket_name, object_name)