import os
import time
import random
from minio import Minio

# MinIO client configurations
region1 = Minio("localhost:9001", access_key="minioadmin", secret_key="minioadmin", secure=False)
region2 = Minio("localhost:9002", access_key="minioadmin", secret_key="minioadmin", secure=False)

# Directories
sample_data_dir = "./sample_data"
bucket_name = "test-bucket"

# Generate a large dataset with varying file sizes
def generate_large_dataset(num_files=100):
    print(f"Generating {num_files} files...")
    os.makedirs(sample_data_dir, exist_ok=True)
    for i in range(num_files):
        filename = f"file_{i+1}.txt"
        size_kb = random.randint(1, 1024)  # Random size between 1 KB and 1 MB
        file_path = os.path.join(sample_data_dir, filename)
        with open(file_path, "w") as f:
            content = f"File Name: {filename}\n" + ("Sample Data\n" * (size_kb * 1024 // 11))
            f.write(content)
        print(f"Generated: {filename} ({size_kb} KB)")

# Upload files to Region 1
def upload_files_to_region1(bucket_name):
    print(f"Uploading files to bucket: {bucket_name} in Region 1")
    if not region1.bucket_exists(bucket_name):
        region1.make_bucket(bucket_name)
    for filename in os.listdir(sample_data_dir):
        file_path = os.path.join(sample_data_dir, filename)
        region1.fput_object(bucket_name, filename, file_path)
        print(f"Uploaded: {filename}")

# Replicate files from Region 1 to Region 2
def replicate_files_to_region2(bucket_name):
    print(f"Replicating files from Region 1 to Region 2")
    # Ensure the bucket exists in Region 2
    if not region2.bucket_exists(bucket_name):
        region2.make_bucket(bucket_name)
    # List objects in Region 1
    objects = region1.list_objects(bucket_name, recursive=True)
    for obj in objects:
        try:
            data = region1.get_object(bucket_name, obj.object_name)
            region2.put_object(bucket_name, obj.object_name, data, length=-1, part_size=10*1024*1024)
            print(f"Replicated: {obj.object_name}")
        except Exception as e:
            print(f"Failed to replicate {obj.object_name}: {e}")

# Simulate Region 1 outage by stopping the container
def simulate_region1_outage():
    print("Simulating Region 1 outage...")
    os.system("docker stop minio-region1")
    print("Region 1 is now offline.")

# Access files from Region 2 and measure recovery time
def access_files_from_region2(bucket_name):
    print("Accessing files from Region 2...")
    start_time = time.time()
    objects = region2.list_objects(bucket_name, recursive=True)
    file_count = 0
    for obj in objects:
        try:
            data = region2.get_object(bucket_name, obj.object_name)
            data.read()  # Read data to ensure it's accessible
            file_count += 1
            print(f"Accessed: {obj.object_name}")
        except Exception as e:
            print(f"Failed to access {obj.object_name}: {e}")
    end_time = time.time()
    recovery_time = end_time - start_time
    print(f"Accessed {file_count} files from Region 2")
    print(f"Recovery Time Objective (RTO): {recovery_time:.2f} seconds")
    return recovery_time

# Main function
def main():
    print("Starting Geo-Redundant Storage Scenario...")

    # Step 1: Generate sample data
    generate_large_dataset(100)

    # Step 2: Upload files to Region 1
    upload_files_to_region1(bucket_name)

    # Step 3: Replicate files to Region 2
    replicate_files_to_region2(bucket_name)

    # Step 4: Simulate Region 1 outage
    simulate_region1_outage()

    # Step 5: Access files from Region 2 and measure RTO
    recovery_time = access_files_from_region2(bucket_name)

    print(f"Scenario Completed. RTO: {recovery_time:.2f} seconds")

if __name__ == "__main__":
    main()
