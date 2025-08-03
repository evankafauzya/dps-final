import pandas as pd
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

def benchmark(data):
    start = time.time()
    _ = process_data(data)
    end = time.time()
    return end - start

def get_system_info():
    print("=== System Info ===")
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

    splits = {
        "25%": data.sample(frac=0.25, random_state=42),
        "50%": data.sample(frac=0.50, random_state=42),
        "75%": data.sample(frac=0.75, random_state=42),
        "100%": data
    }

    results = []
    for label, split in splits.items():
        duration = benchmark(split)
        results.append({"Split": label, "Time (s)": duration})

    result_df = pd.DataFrame(results)
    print(result_df)
    get_system_info()

if __name__ == "__main__":
    main()