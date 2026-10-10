import os
import requests

# Target download directory and size limit in bytes (2 GB)
DOWNLOAD_DIR = "./raw/batch"
TARGET_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Generate list of year-month combinations (starting from recent months)
years = range(2025, 2022, -1)  # 2025, 2024, 2023...
months = [f"{m:02d}" for m in range(12,0 ,-1)]

downloaded_bytes = 0

for year in years:
    for month in months:
        if downloaded_bytes >= TARGET_SIZE_BYTES:
            break

        file_name = f"yellow_tripdata_{year}-{month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        save_path = os.path.join(DOWNLOAD_DIR, file_name)

        print(f"Downloading {file_name}...")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)

            file_size_mb = os.path.getsize(save_path) / (1024 * 1024)
            total_gb = downloaded_bytes / (1024 * 1024 * 1024)
            print(f"Saved {file_name} ({file_size_mb:.1f} MB). Total downloaded: {total_gb:.2f} GB")
        else:
            print(f"File not found: {file_name} (HTTP {response.status_code})")

    if downloaded_bytes >= TARGET_SIZE_BYTES:
        break

print(f"\nDownload complete! Files saved in: {os.path.abspath(DOWNLOAD_DIR)}")