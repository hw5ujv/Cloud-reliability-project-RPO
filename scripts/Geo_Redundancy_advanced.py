import os
import time
import csv
import random
import shutil
from minio import Minio, S3Error

# Configuration for MinIO Clients
region1 = Minio("localhost:9001", access_key="minioadmin", secret_key="minioadmin", secure=False)
region2 = Minio("localhost:9002", access_key="minioadmin", secret_key="minioadmin", secure=False)

bucket_name = "test-bucket"
sample_data_dir = "./sample_data"

def clean_up_data_dir():
    if os.path.exists(sample_data_dir):
        shutil.rmtree(sample_data_dir)

def generate_large_dataset(num_files=100, min_kb=10, max_kb=1024):
    """Generate a dataset with a range of file sizes."""
    print(f"Generating {num_files} files with sizes between {min_kb}KB and {max_kb}KB...")
    os.makedirs(sample_data_dir, exist_ok=True)
    for i in range(num_files):
        filename = f"file_{i+1}.txt"
        size_kb = random.randint(min_kb, max_kb)
        file_path = os.path.join(sample_data_dir, filename)
        content = ("X" * (size_kb * 1024))
        with open(file_path, "w") as f:
            f.write(content)
    print("Dataset generation completed.")

def upload_files(region_client, bucket_name, directory):
    """Upload generated files to a given bucket in a given region."""
    print(f"Uploading files to bucket: {bucket_name}")
    if not region_client.bucket_exists(bucket_name):
        region_client.make_bucket(bucket_name)
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        region_client.fput_object(bucket_name, filename, file_path)
    print("Upload completed.")

def replicate_files(source_client, target_client, bucket_name):
    """Replicate files from source region to target region."""
    print("Replicating files from Region 1 to Region 2...")
    if not target_client.bucket_exists(bucket_name):
        target_client.make_bucket(bucket_name)
    objects = source_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        data = source_client.get_object(bucket_name, obj.object_name)
        target_client.put_object(bucket_name, obj.object_name, data, length=-1, part_size=10*1024*1024)
    print("Replication completed.")

def measure_access_time(region_client, bucket_name):
    """Measure how long it takes to read all files from a given region once."""
    start_time = time.time()
    objects = list(region_client.list_objects(bucket_name, recursive=True))
    accessible_count = 0
    for obj in objects:
        try:
            data = region_client.get_object(bucket_name, obj.object_name)
            data.read()  # Confirm the object is readable
            accessible_count += 1
        except S3Error as e:
            print(f"Failed to access {obj.object_name}: {e}")
    end_time = time.time()
    duration = end_time - start_time
    return accessible_count, duration

def simulate_outage():
    """Simulate Region 1 outage by stopping the docker container."""
    print("Simulating Region 1 outage...")
    # Adjust the container name as per your docker-compose setup
    os.system("docker stop minio-region1")
    print("Region 1 is now offline.")

def restore_region():
    """Restore Region 1 after the test."""
    print("Restarting Region 1 container...")
    os.system("docker start minio-region1")
    print("Region 1 is back online.")

def measure_rto_from_region2(bucket_name, expected_file_count, poll_interval=5):
    """Measure how long it takes for Region 2 to serve all files after Region 1 goes down."""
    print("Measuring RTO from Region 2...")
    start_time = time.time()
    while True:
        objects = list(region2.list_objects(bucket_name, recursive=True))
        accessible_files = 0
        for obj in objects:
            try:
                data = region2.get_object(bucket_name, obj.object_name)
                data.read()  
                accessible_files += 1
            except:
                pass
        if accessible_files == expected_file_count:
            break
        print(f"Accessible Files: {accessible_files}/{expected_file_count}, waiting {poll_interval}s...")
        time.sleep(poll_interval)
    end_time = time.time()
    rto = end_time - start_time
    print(f"All files accessible from Region 2. RTO: {rto:.2f} seconds")
    return rto

def clear_bucket(region_client, bucket_name):
    """Clean up the bucket to prepare for the next test."""
    if region_client.bucket_exists(bucket_name):
        objects = region_client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            region_client.remove_object(bucket_name, obj.object_name)
        region_client.remove_bucket(bucket_name)

def calculate_rpo(original_file_count, recovered_file_count):
    """Calculate RPO as the percentage of original data that is accessible after failover."""
    data_loss_count = original_file_count - recovered_file_count
    if original_file_count > 0:
        restored_percentage = (recovered_file_count / original_file_count) * 100
    else:
        restored_percentage = 100.0
    print(f"Original file count: {original_file_count}")
    print(f"Recovered (accessible) file count in Region 2: {recovered_file_count}")
    print(f"Data loss (files not recovered): {data_loss_count}")
    print(f"RPO (Data Restored): {restored_percentage:.2f}%")
    return restored_percentage

def log_results_to_csv(filename, scenario_name, num_files, min_kb, max_kb, replication_used, rto, baseline_time, rpo_percentage):
    """Log the results of the scenario to a CSV file."""
    header = [
        "scenario_name", 
        "num_files", 
        "min_file_kb", 
        "max_file_kb", 
        "replication_used", 
        "RTO_seconds", 
        "baseline_access_time_seconds",
        "RPO_data_restored_percentage"
    ]
    row = [
        scenario_name, 
        num_files, 
        min_kb, 
        max_kb, 
        replication_used, 
        rto, 
        baseline_time,
        rpo_percentage
    ]

    file_exists = os.path.exists(filename)
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)

def run_geo_failover_test(num_files=100, min_kb=10, max_kb=1024, use_replication=True):
    """Run one geo-failover test with given parameters."""
    scenario_name = "Geo-Redundancy Failover Test" if use_replication else "Single-Region Outage (No Replication)"
    
    # Clean up from previous runs
    clean_up_data_dir()
    clear_bucket(region1, bucket_name)
    clear_bucket(region2, bucket_name)

    # Step 1: Generate data
    generate_large_dataset(num_files, min_kb, max_kb)

    # Step 2: Upload to region1
    upload_files(region1, bucket_name, sample_data_dir)

    # Record original file count after uploading
    original_objects = list(region1.list_objects(bucket_name, recursive=True))
    original_file_count = len(original_objects)

    # Step 3: Replicate to region2 if replication is on
    if use_replication:
        replicate_files(region1, region2, bucket_name)
    else:
        clear_bucket(region2, bucket_name)

    # Step 4: Measure baseline access time from region1
    baseline_accessible, baseline_time = measure_access_time(region1, bucket_name)
    print(f"Baseline: Accessed {baseline_accessible}/{num_files} files in {baseline_time:.2f}s from Region 1.")

    # Step 5: Simulate outage of region1
    simulate_outage()

    # Step 6: Measure RTO from region2 if replication is used
    if use_replication:
        rto = measure_rto_from_region2(bucket_name, expected_file_count=num_files)
        # After measuring RTO, count how many files Region 2 actually has
        region2_objects = list(region2.list_objects(bucket_name, recursive=True))
        recovered_count = len(region2_objects)
    else:
        # No replication scenario
        start_time = time.time()
        objects = list(region2.list_objects(bucket_name, recursive=True))
        accessible_count = 0
        for obj in objects:
            try:
                data = region2.get_object(bucket_name, obj.object_name)
                data.read()
                accessible_count += 1
            except:
                pass
        if accessible_count == 0:
            rto = -1  # Indicates failure/no access
        else:
            rto = time.time() - start_time
        recovered_count = accessible_count

    # Step 7: Calculate RPO based on how many files are available in Region 2
    rpo_percentage = calculate_rpo(original_file_count, recovered_count)

    # Step 8: Log results
    log_results_to_csv("results.csv", scenario_name, num_files, min_kb, max_kb, use_replication, rto, baseline_time, rpo_percentage)

    # Optional: Restore region1 after testj
    restore_region()

    # Cleanup data on the host machine
    clean_up_data_dir()

    print(f"Test scenario completed. RTO: {rto:.2f}s, RPO: {rpo_percentage:.2f}%. Check results.csv for recorded metrics.\n\n")

def main():
    # Test cases:
    # With replication
    run_geo_failover_test(num_files=100, min_kb=10, max_kb=50, use_replication=True)
    run_geo_failover_test(num_files=500, min_kb=50, max_kb=512, use_replication=True)
    run_geo_failover_test(num_files=1000, min_kb=10, max_kb=1024, use_replication=True)

    # Without replication
    run_geo_failover_test(num_files=100, min_kb=10, max_kb=50, use_replication=False)
    run_geo_failover_test(num_files=500, min_kb=10, max_kb=1024, use_replication=False)

    print("All test scenarios have been executed. Review results.csv for comparisons.")

if __name__ == "__main__":
    main()
