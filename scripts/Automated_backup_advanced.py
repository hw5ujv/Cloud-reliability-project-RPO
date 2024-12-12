import os
import time
import csv
import random
import shutil
from minio import Minio, S3Error

# Configuration for MinIO (single region scenario)
region1 = Minio("localhost:9001", access_key="minioadmin", secret_key="minioadmin", secure=False)

bucket_name = "test-bucket"
sample_data_dir = "./sample_data"
backup_dir = "./backups"

def clean_up_data_dir():
    if os.path.exists(sample_data_dir):
        shutil.rmtree(sample_data_dir)

def clean_up_backup_dir():
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)

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
    """Upload generated files to a given bucket."""
    print(f"Uploading files to bucket: {bucket_name}")
    if not region_client.bucket_exists(bucket_name):
        region_client.make_bucket(bucket_name)
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        region_client.fput_object(bucket_name, filename, file_path)
    print("Upload completed.")

def create_partial_backup(region_client, bucket_name, backup_fraction=0.5):
    """Create a partial backup of a fraction of the files."""
    print(f"Creating a partial backup for bucket: {bucket_name} at fraction: {backup_fraction}")
    os.makedirs(backup_dir, exist_ok=True)
    objects = list(region_client.list_objects(bucket_name, recursive=True))
    random.shuffle(objects)
    backup_count = int(len(objects) * backup_fraction)
    objects_to_backup = objects[:backup_count]

    backed_up_files = 0
    for obj in objects_to_backup:
        try:
            data = region_client.get_object(bucket_name, obj.object_name)
            backup_file_path = os.path.join(backup_dir, obj.object_name)
            with open(backup_file_path, "wb") as backup_file:
                for d in data.stream(32 * 1024):
                    backup_file.write(d)
            backed_up_files += 1
        except Exception as e:
            print(f"Failed to back up {obj.object_name}: {e}")
    print(f"Partial backup completed! {backed_up_files}/{len(objects_to_backup)} files backed up.")

def simulate_partial_failure(region_client, bucket_name, deletion_fraction=0.5):
    """Simulate accidental deletion of a fraction of the files."""
    print(f"Simulating partial failure by deleting {deletion_fraction*100}% of files...")
    objects = list(region_client.list_objects(bucket_name, recursive=True))
    failure_count = int(len(objects) * deletion_fraction)
    objects_to_delete = random.sample(objects, failure_count)

    for obj in objects_to_delete:
        region_client.remove_object(bucket_name, obj.object_name)
    print(f"Deleted {failure_count} files.")

def measure_access_time(region_client, bucket_name):
    """Measure how long it takes to read all files currently in the bucket."""
    start_time = time.time()
    objects = list(region_client.list_objects(bucket_name, recursive=True))
    accessible_count = 0
    for obj in objects:
        try:
            data = region_client.get_object(bucket_name, obj.object_name)
            data.read()
            accessible_count += 1
        except S3Error as e:
            print(f"Failed to access {obj.object_name}: {e}")
    end_time = time.time()
    duration = end_time - start_time
    return accessible_count, duration

def restore_from_backup(region_client, bucket_name):
    """Restore files from backup directory to the bucket."""
    print("Restoring from backup...")
    start_time = time.time()
    for filename in os.listdir(backup_dir):
        file_path = os.path.join(backup_dir, filename)
        region_client.fput_object(bucket_name, filename, file_path)
    end_time = time.time()
    recovery_time = end_time - start_time
    print(f"Restoration completed! Recovery Time (restore phase): {recovery_time:.2f} seconds")
    return recovery_time

def calculate_rpo(region_client, bucket_name, original_file_count):
    # Count how many files are currently in the bucket
    objects_after_recovery = list(region_client.list_objects(bucket_name, recursive=True))
    recovered_count = len(objects_after_recovery)
    data_loss_count = original_file_count - recovered_count

    # Calculate RPO as percentage of data restored
    if original_file_count > 0:
        restored_percentage = (recovered_count / original_file_count) * 100
    else:
        restored_percentage = 100.0  # If no original files, trivial case

    print(f"Original file count: {original_file_count}")
    print(f"Recovered file count: {recovered_count}")
    print(f"Data loss (files not recovered): {data_loss_count}")
    print(f"RPO (Data Restored): {restored_percentage:.2f}%")

    return restored_percentage

def clear_bucket(region_client, bucket_name):
    """Clean up the bucket to prepare for the next test."""
    if region_client.bucket_exists(bucket_name):
        objects = region_client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            region_client.remove_object(bucket_name, obj.object_name)
        region_client.remove_bucket(bucket_name)

def log_results_to_csv(filename, scenario_name, num_files, min_kb, max_kb, backup_fraction, deletion_fraction, rto, baseline_time, rpo_percentage):
    header = [
        "scenario_name", 
        "num_files", 
        "min_file_kb", 
        "max_file_kb", 
        "backup_fraction", 
        "deletion_fraction", 
        "RTO_seconds", 
        "baseline_access_time_seconds",
        "RPO_data_restored_percentage"
    ]
    row = [
        scenario_name, 
        num_files, 
        min_kb, 
        max_kb, 
        backup_fraction, 
        deletion_fraction, 
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

def run_automated_backup_test(num_files=100, min_kb=10, max_kb=1024, backup_fraction=0.75, deletion_fraction=0.5):
    """Run one automated backup test scenario with RPO measurement."""
    scenario_name = "Automated Backup and Restore Scenario"
    
    # Clean up from previous runs
    clean_up_data_dir()
    clean_up_backup_dir()
    clear_bucket(region1, bucket_name)

    # Step 1: Generate data
    generate_large_dataset(num_files, min_kb, max_kb)

    # Step 2: Upload to region1
    upload_files(region1, bucket_name, sample_data_dir)
    original_objects = list(region1.list_objects(bucket_name, recursive=True))
    original_file_count = len(original_objects)

    # Step 3: Measure baseline access time
    baseline_accessible, baseline_time = measure_access_time(region1, bucket_name)
    print(f"Baseline: Accessed {baseline_accessible}/{num_files} files in {baseline_time:.2f}s.")

    # Step 4: Create a partial backup
    create_partial_backup(region1, bucket_name, backup_fraction=backup_fraction)

    # Step 5: Simulate partial failure
    simulate_partial_failure(region1, bucket_name, deletion_fraction=deletion_fraction)

    # Step 6: Restore from backup and measure RTO
    start_rto = time.time()
    restore_from_backup(region1, bucket_name)
    post_restore_accessible, _ = measure_access_time(region1, bucket_name)
    end_rto = time.time()
    rto = end_rto - start_rto
    print(f"RTO measured as the time from start of restore to all accessible: {rto:.2f}s.")

    # Step 7: Calculate RPO
    rpo_percentage = calculate_rpo(region1, bucket_name, original_file_count)

    # Step 8: Log results
    log_results_to_csv("backup_results_r.csv", scenario_name, num_files, min_kb, max_kb, backup_fraction, deletion_fraction, rto, baseline_time, rpo_percentage)

    # Cleanup data on the host machine
    clean_up_data_dir()
    clean_up_backup_dir()

    print(f"Test scenario completed. RTO: {rto:.2f}s, RPO: {rpo_percentage:.2f}%. Check backup_results_r.csv for recorded metrics.\n\n")

def main():
    # Multiple test cases with different parameters

    # 1. Small number of files, high backup fraction, moderate deletion
    run_automated_backup_test(num_files=100, min_kb=10, max_kb=50, backup_fraction=0.75, deletion_fraction=0.5)

    # 2. More files, smaller backup fraction (less coverage), and moderate deletion
    run_automated_backup_test(num_files=500, min_kb=10, max_kb=1024, backup_fraction=0.5, deletion_fraction=0.5)

    # 3. Even more files, very high backup fraction, larger sizes
    run_automated_backup_test(num_files=1000, min_kb=50, max_kb=1024, backup_fraction=0.9, deletion_fraction=0.5)

    # 4. Medium number of files, small backup fraction (test minimal backups)
    run_automated_backup_test(num_files=500, min_kb=10, max_kb=512, backup_fraction=0.25, deletion_fraction=0.5)

    # 5. Smaller dataset but high deletion fraction (80% deletion)
    run_automated_backup_test(num_files=100, min_kb=10, max_kb=100, backup_fraction=0.75, deletion_fraction=0.8)

    print("All backup test scenarios have been executed. Review backup_results.csv for comparisons.")

if __name__ == "__main__":
    main()
