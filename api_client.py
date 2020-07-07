import ast
import json
import requests
import sys

import arrow


API_URL = "https://00ij5gk51d.execute-api.eu-west-1.amazonaws.com/accounts"
PER_PAGE = 100


class APIClient:
    def fetch(self, start=None, end=None):
        args = f"per_page={PER_PAGE}&"
        if start is not None:
            args += f"updated_at_gt={start}&"
        if end is not None:
            args += f"updated_at_lte={end}&"
        
        records = None
        next_page = 1
        while next_page is not None:
            response = self._fetch_page(args, next_page)
            print(f"Fetched page {next_page}: {len(response['data'])} records")
            next_page = response['metadata']['paging']['next_page']
            if records is None:
                records = response["data"]
            else:
                records.append(response["data"])
        
        # print(json.dumps(records, indent=3))
        return records

    
    def _fetch_page(self, args, page):
        args += f"&page={page}"
        response = requests.get(f"{API_URL}?{args}").text
        data = ast.literal_eval(response)
        # print(json.dumps(data, indent=3))
        return data


if __name__ == "__main__":
    client = APIClient()
    end = arrow.utcnow()
    start = end.shift(minutes=-5)
    data = client.fetch(start, end)
    print(data)
    print(start)
    print(end)
