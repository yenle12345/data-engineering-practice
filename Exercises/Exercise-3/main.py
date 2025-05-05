import requests
import gzip
from io import BytesIO

def get_file(base,key):
    url = base + key
    resp = requests.get(url,stream = True)
    resp.raise_for_status()
    return resp
def main():
    s3_base = "https://data.commoncrawl.org/"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    resp_wet = get_file(s3_base,key)
    with gzip.open(BytesIO(resp_wet.content), 'rt', encoding='utf-8') as f: 
        uri_path = f.readline().strip()

    resp_warc = get_file(s3_base,uri_path)
    for line in resp_warc.iter_lines(decode_unicode=True):
        print(line)
        
if __name__ == "__main__":
    main()
