import requests
import time

TRINO_URL = "http://localhost:8080/v1/statement"  # Adjust as needed
HEADERS = {
    "X-Trino-User": "benchmark_user",
    "X-Trino-Catalog": "postgresql",
    "X-Trino-Schema": "public"
}

session = requests.Session()

def run_query(query, tag, query_type):
    start_time = time.time()
    # print(f"Running query [{query_type}] tag={tag}...")
    try: 
        data = _send_query(query)
        data, query_id, all_rows = _poll_query_results(data)
        
        end_time = time.time()
        
        _append_query_data(data, all_rows)
        stats = data.get("stats", {})
        result_case_count = all_rows[0][0] if all_rows else 0
        
        total_rows = stats.get("processedRows", 0)
        cpu_time = stats.get("cpuTimeMillis", 0)
        cpu_time_per_million_rows = cpu_time / (total_rows / 1_000_000)

        
        result = {
            "query_id": query_id,
            "query_tag": tag,
            "query_type": query_type,
            "result_case_count": result_case_count,
            "direct_execution_time_sec": end_time - start_time,
            "elapsed_millis": stats.get("elapsedTimeMillis"),
            "cpu_time_millis": cpu_time,
            "cpu_time_per_million_rows": cpu_time_per_million_rows,
            "queued_millis": stats.get("queuedTimeMillis"),
            "peak_memory_mb": round(stats.get("peakMemoryBytes", 0) / 1024**2, 2),
            "total_rows_processed": total_rows,
        }
        
    except Exception as e:
        print(f"❌ Query failed for [{query_type}] tag={tag}: {e}")
        result = {
            "query_id": "FAILED",
            "query_tag": tag,
            "query_type": query_type,
            "result_case_count": 0,
            "direct_execution_time_sec": 0,
            "elapsed_millis": None,
            "cpu_time_millis": None,
            "queued_millis": None,
            "peak_memory_mb": None,
            "total_rows_processed": 0,
            "error": str(e),
        }
    
    return result


def _append_query_data(data, all_rows):
    if "data" in data:
        all_rows.extend(data["data"])


def _send_query(query):
    response = session.post(TRINO_URL, data=query, headers=HEADERS)
    response.raise_for_status()
    
    return response.json()


def _follow_next_page(data):
    response = session.get(data["nextUri"])
    response.raise_for_status()
    
    return response.json() # Update with the next page


def _poll_query_results(data):
    all_rows = []
    query_id = "unknown"
    
    while "nextUri" in data:
        _append_query_data(data, all_rows)
        data = _follow_next_page(data)
    
        if "id" in data:
            query_id = data["id"]
            
        if "error" in data:
            raise Exception(f"Error during execution: {data['error'].get('message')}")
        
    return data,query_id, all_rows
