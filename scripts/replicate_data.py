from minio import Minio

# Configure MinIO clients
region1 = Minio("localhost:9001", access_key="minioadmin", secret_key="minioadmin", secure=False)
region2 = Minio("localhost:9002", access_key="minioadmin", secret_key="minioadmin", secure=False)

# Function to replicate data
def replicate_data(bucket_name, object_name):
    # Download the object from Region 1
    obj = region1.get_object(bucket_name, object_name)
    # Upload the object to Region 2
    region2.put_object(bucket_name, object_name, obj, length=-1, part_size=10*1024*1024)
    print(f"Replicated {object_name} from Region 1 to Region 2")

def replicate_files(bucket_name):
    # Ensure the bucket exists in Region 2
    if not region2.bucket_exists(bucket_name):
        region2.make_bucket(bucket_name)

    # List objects in Region 1
    objects = region1.list_objects(bucket_name, recursive=True)

    # Replicate each object
    for obj in objects:
        print(f"Replicating {obj.object_name}...")
        data = region1.get_object(bucket_name, obj.object_name)
        region2.put_object(bucket_name, obj.object_name, data, length=-1, part_size=10*1024*1024)
        print(f"Replicated {obj.object_name} to Region 2")

# Main script
if __name__ == "__main__":
    bucket_name = "test-bucket"
    object_name = "example1.txt"

    # Ensure the bucket exists in Region 2
    if not region2.bucket_exists(bucket_name):
        region2.make_bucket(bucket_name)

    # Replicate the data
    #replicate_data(bucket_name, object_name)

    replicate_files(bucket_name)