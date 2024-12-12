import os

# Directory to store sample data
output_dir = "./sample_data"

# Sample content template
content_template = """
File Name: {filename}
File Size: {filesize} KB
This is a sample file for testing disaster recovery in MinIO.
The file contains repetitive data to fill the required size.
""" + ("Sample Data\n" * 100)

# Function to create a file with specified size
def create_sample_file(filename, size_kb):
    content = content_template.format(filename=filename, filesize=size_kb)
    content *= (size_kb * 1024) // len(content)  # Repeat to match size
    with open(os.path.join(output_dir, filename), "w") as file:
        file.write(content)

# Generate files
def generate_files():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files = [
        ("example1.txt", 1),   # 1 KB
        ("example2.txt", 10),  # 10 KB
        ("example3.txt", 100), # 100 KB
        ("example4.txt", 512), # 512 KB
        ("example5.txt", 1024) # 1 MB
    ]

    for filename, size_kb in files:
        create_sample_file(filename, size_kb)
        print(f"Created {filename} with size {size_kb} KB")

if __name__ == "__main__":
    generate_files()
