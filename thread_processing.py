import pandas as pd
import threading
import time
import platform
import psutil
import subprocess

def process_data(data):
    start = time.time()
    filtered = data[data > 1000]
    sorted_data = filtered.sort_values()
    end = time.time()
    print(f"Processed chunk of size {len(data)} in {round(end - start, 4)} seconds")
    return sorted_data

def worker_threading(data_chunks):
    results = []
    threads = []

    def thread_task(chunk):
        result = process_data(chunk)
        results.append(result)

    for chunk in data_chunks:
        thread = threading.Thread(target=thread_task, args=(chunk,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return pd.concat(results)

def benchmark(data):
    chunks = [data.iloc[i::4] for i in range(4)]  # split into 4 interleaved chunks
    start = time.time()
    _ = worker_threading(chunks)
    end = time.time()
    return end - start

def get_system_info():
    print("\n=== System Info ===")
    print(f"Processor: {platform.processor()}")
    print(f"RAM: {round(psutil.virtual_memory().total / 1e9, 2)} GB")
    try:
        gpu_info = subprocess.check_output(["system_profiler", "SPDisplaysDataType"]).decode()
        for line in gpu_info.split("\n"):
            if "Chipset Model" in line or "VRAM" in line:
                print(line.strip())
    except:
        print("GPU: Unable to detect")

def main():
    file_path = "/Users/evankafzy/Documents/dps-final/train.csv"
    df = pd.read_csv(file_path)
    data = df['trip_duration']

    print("=== Sample Data (trip_duration column) ===")
    print(data.head(10))  # Print first 10 rows
    print()

    splits = {
        "25%": data.sample(frac=0.25, random_state=42),
        "50%": data.sample(frac=0.50, random_state=42),
        "75%": data.sample(frac=0.75, random_state=42),
        "100%": data
    }

    results = []
    for label, split in splits.items():
        print(f"\n--- Running Threaded Benchmark for {label} ---")
        duration = benchmark(split)
        results.append({"Split": label, "Time (s)": round(duration, 4)})

    result_df = pd.DataFrame(results)
    print("\n=== Threading Benchmark Results ===")
    print(result_df)
    get_system_info()

if __name__ == "__main__":
    main()
