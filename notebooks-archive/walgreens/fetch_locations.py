import requests
import pandas as pd
import time
import random
from tqdm import tqdm

headers = {
    "authority": "www.walgreens.com",
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

def fetch_store_data(session, zipcode, retries=3, base_delay=2):
    params = {
        "requestor": "search",
    }
    json_data = {
        "r": "50",
        "requestType": "dotcom",
        "s": "50",
        "q": f'{zipcode}',
        "zip": f'{zipcode}',
    }
    
    for attempt in range(retries):
        try:
            response = session.post(
                "https://www.walgreens.com/locator/v1/stores/search",
                params=params,
                headers=headers,
                json=json_data,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if "results" in data and data["results"]:
                return [
                    {
                        "store_number": r["storeNumber"],
                        "street": r["store"]["address"]["street"].title(),
                        "city": r["store"]["address"]["city"].title(),
                        "state": r["store"]["address"]["state"],
                        "zip": r["store"]["address"]["zip"],
                        "latitude": r["latitude"],
                        "longitude": r["longitude"],
                    }
                    for r in data["results"]
                ]
            else:
                print(f"No results for {zipcode}. Skipping.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error for {zipcode}: {str(e)}")
            if attempt < retries - 1:
                wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch data for {zipcode} after {retries} attempts.")
                return None
        
        # Add a delay between requests
        time.sleep(base_delay + random.uniform(0, 2))

def main():
    zips = (
        pd.read_json("../_reference/data/zip_code_demographics_esri.json")
        .query("population > 1000")
        .sort_values("population", ascending=False)
        .reset_index(drop=True)
    )
    zips["zipcode"] = zips["zipcode"].astype(str).str.zfill(5)
    sample = zips.head(100).copy()  # Process 100 ZIP codes
    
    response_list = []
    with requests.Session() as session:
        for _, row in tqdm(sample.iterrows(), total=len(sample)):
            result = fetch_store_data(session, row["zipcode"])
            if result:
                response_list.extend(result)
    
    return pd.DataFrame(response_list)

if __name__ == "__main__":
    start_time = time.time()
    result_df = main()
    end_time = time.time()
    
    print(f"\nTotal time taken: {end_time - start_time:.2f} seconds")
    print(f"Number of stores found: {len(result_df)}")
    if not result_df.empty:
        print(f"Number of unique ZIP codes with stores: {result_df['zip'].nunique()}")
        print(result_df.head())  # Print first few rows
    else:
        print("No stores found. Check the logs above for details on each request.")
    
    # Save the results to a CSV file
    if not result_df.empty:
        result_df.to_csv("walgreens_stores.csv", index=False)
        print("Results saved to walgreens_stores.csv")