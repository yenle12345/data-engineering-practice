import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_and_extract(uri, download_path):
    try:
        filename = uri.split("/")[-1]
        zip_path = download_path / filename
        print(f"Tải xuống: {uri}")
        response = requests.get(uri, timeout=10)
        if response.status_code != 200:
            print(f"Không thể tải file: {uri}")
            return
        with open(zip_path, "wb") as f:
            f.write(response.content)
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)
        print(f"Đã giải nén: {filename}")
        zip_path.unlink()
    except Exception as e:
        print(f"Lỗi khi xử lý {uri}: {e}")

def main():
    download_path = Path("downloads")
    download_path.mkdir(exist_ok=True)

    for uri in download_uris:
        download_and_extract(uri, download_path)

if __name__ == "__main__":
    main()
