import streamlit as st
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import math
import time

# API Configuration
BASE_URL = "https://test.baas.batterypool.com/driver/api/"
GET_LIST_URL = f"{BASE_URL}drivers/getListOfDriverIds"
GET_DETAILS_URL = f"{BASE_URL}getDriverDetailsProxy/"
PARTNER_ID = "tp4_id"

st.title("Driver Details Fetcher (3 at a time, abort on 500)")

def fetch_driver_details(driver_id):
    """Fetch details for a single driver."""
    details_url = f"{GET_DETAILS_URL}{driver_id}?partnerId={PARTNER_ID}"
    response = requests.get(details_url)

    # If you want to raise an exception on 500, you could do it here, e.g.:
    # if response.status_code == 500:
    #      raise Exception("Received 500 error from API")

    if response.status_code == 200:
        try:
            data = response.json().get("data", {})
            total_penalty = data.get("totalPenaltyApplicable", "N/A")
        except:
            total_penalty = "Error Parsing"
    elif response.status_code == 500:
        # Identify this response clearly so we know to abort
        total_penalty = "API Error 500"
    else:
        total_penalty = f"API Error {response.status_code}"

    return {"Vehicle ID": driver_id, "Total Penalty": total_penalty}

if st.button("Fetch All Driver Details"):
    start_time = time.time()

    # 1. Fetch the list of driver IDs
    st.write("Fetching list of drivers...")
    response = requests.get(GET_LIST_URL)

    if response.status_code == 200:
        try:
            driver_ids = response.json().get("data", [])
        except Exception as e:
            st.error(f"Error parsing driver list: {e}")
            driver_ids = []
    else:
        st.error(f"Failed to fetch driver list. API Error {response.status_code}")
        driver_ids = []

    st.write(f"Total drivers found: {len(driver_ids)}")

    # 2. Process in batches of 25
    BATCH_SIZE = 4
    num_batches = math.ceil(len(driver_ids) / BATCH_SIZE)
    results = []
    abort_due_to_error = False

    for batch_index in range(num_batches):
        if abort_due_to_error:
            break

        start = batch_index * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_ids = driver_ids[start:end]

        st.write(f"Fetching details for batch {batch_index + 1}/{num_batches}: {batch_ids}")

        # -- Run up to 25 requests concurrently in this batch
        with ThreadPoolExecutor(max_workers=4) as executor:
            batch_results = list(executor.map(fetch_driver_details, batch_ids))

        # Check if any request returned a 500
        if any(br["Total Penalty"] == "API Error 500" for br in batch_results):
            st.error("Aborting due to 500 error in this batch.")
            abort_due_to_error = True
            # Optionally, you might want to store partial results,
            # so we still add this batch's partial results.
            # Or you can skip if you want to discard them.
            results.extend(batch_results)
            break

        # Merge this batch's results into the main results
        results.extend(batch_results)

        # (Optional) Short sleep to avoid bursting the API
        time.sleep(1)

    # 3. Convert results to DataFrame and display
    df = pd.DataFrame(results)
    st.write(df)

    # 4. Show execution time
    end_time = time.time()
    execution_time = end_time - start_time
    st.write(f"Total Execution Time: {execution_time:.2f} seconds")
