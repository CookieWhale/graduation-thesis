import os
import csv
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Configuration
input_file = "logins.txt"
output_file = "rabbit_output_parallel.csv"
split_dir = "splits"
output_dir = "split_outputs"
num_threads = 6  # Must match the number of tokens

rabbit_cmd = "rabbit"

# Assign one token per thread (order must match the file chunks)
tokens = [

]

# Step 1: Split the input file by lines
def split_file(input_file, num_parts):
    os.makedirs(split_dir, exist_ok=True)
    with open(input_file, 'r') as f:
        lines = f.readlines()

    chunk_size = len(lines) // num_parts + (len(lines) % num_parts > 0)
    for i in range(num_parts):
        chunk = lines[i * chunk_size : (i + 1) * chunk_size]
        with open(f"{split_dir}/chunk_{i}.txt", "w") as f_out:
            f_out.writelines(chunk)

# Step 2: Run the rabbit command (each thread uses a different token)
def run_rabbit(chunk_path, index, token):
    os.makedirs(output_dir, exist_ok=True)
    out_csv = f"{output_dir}/out_{index}.csv"
    print(f"[Thread {index}]  Starting rabbit on {chunk_path}...")
    
    cmd = [
        rabbit_cmd,
        "--input-file", chunk_path,
        "--key", token,
        "--csv", out_csv,
        "--incremental"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"[Thread {index}]  Finished. Output saved to {out_csv}")
    except subprocess.CalledProcessError as e:
        print(f"[Thread {index}]  Failed with error: {e}")

# Step 3: Merge multiple CSV files
def merge_csv(output_dir, final_output):
    csv_files = sorted(Path(output_dir).glob("out_*.csv"))
    with open(final_output, "w", newline='') as fout:
        writer = None
        for file in csv_files:
            with open(file, "r", newline='') as fin:
                reader = csv.reader(fin)
                headers = next(reader)
                if writer is None:
                    writer = csv.writer(fout)
                    writer.writerow(headers)
                for row in reader:
                    writer.writerow(row)

# Step 4: Main logic
def main():
    print(" Splitting input file...")
    split_file(input_file, num_threads)

    print(" Running rabbit on each chunk with separate tokens...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            chunk_path = f"{split_dir}/chunk_{i}.txt"
            token = tokens[i]
            futures.append(executor.submit(run_rabbit, chunk_path, i, token))

        # Wait for all threads to complete
        for future in futures:
            future.result()

    print(" Merging CSV files...")
    merge_csv(output_dir, output_file)

    print(f" Done! Final output saved to: {output_file}")

if __name__ == "__main__":
    main()
