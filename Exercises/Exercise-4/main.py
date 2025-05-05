import os
import json
import csv
import glob
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
def main():
    data_dir = './data'
    json_files = glob.glob(f'{data_dir}/**/*.json', recursive=True)

    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Lỗi khi đọc JSON từ file: {json_file}")
                continue

        if isinstance(data, dict):
            data = [data]

        flattened_data = [flatten_json(entry) for entry in data]

        if not flattened_data:
            continue

        csv_file = json_file.replace('.json', '.csv')

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
            writer.writeheader()
            writer.writerows(flattened_data)

        print(f"Đã chuyển {json_file} thành {csv_file}")



if __name__ == "__main__":
    main()
