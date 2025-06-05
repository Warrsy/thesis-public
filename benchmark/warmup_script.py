import os
import run_trino_query
import concurrent.futures

def execute_queries_from_file(sql_dir, filename):
    with open(os.path.join(sql_dir, filename), "r") as file:
        queries = file.read().split("-- QUERY: ")
        results = run_in_parallel(queries)

    return results


def run_in_parallel(queries):
    future_to_query = {}
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for index, q in enumerate(queries[1:]):  # Skip the header
            query_id_line, query_body = q.strip().split("\n", 1)
            query_id = query_id_line.strip()
            query_type = "MATCH_RECOGNIZE" if "MATCH_RECOGNIZE" in query_body.upper() else "REGEX"
                    
            future_to_query[executor.submit(
                    run_trino_query.run_query, 
                        query_body, query_id, query_type)] = (query_id, query_body)
            
            if index == 10:
                break
        
        for future in concurrent.futures.as_completed(future_to_query):
            query_id, _ = future_to_query[future]
            
            try:
                result = future.result()
                results.append(result)
                
            except Exception as exc:
                print(f"❌ Query {query_id} failed with exception: {exc}")

    return results


def run_and_record_queries(queries):
    results = []
    
    for index, q in enumerate(queries[1:]):  # Skip the header
        query_id_line, query_body = q.strip().split("\n", 1)
        query_id = query_id_line.strip()
        query_type = "MATCH_RECOGNIZE" if "MATCH_RECOGNIZE" in query_body.upper() else "REGEX"
                
        result = run_trino_query.run_query(query_body, query_id, query_type)
        results.append(result)
                
        if index == 10:
            break
        
    return results


def find_target_sql_file(model_num, sql_dir):
    for fname in os.listdir(sql_dir):
        if fname.startswith(f"model{model_num}") and fname.endswith("100.sql"):
            return fname
        
    return None


def run_warmup_script():
    print("Running warmup script...")
    for model_num in range(0, 10):
        sql_dir = f"data/queries/model{model_num}"
        
        filename = find_target_sql_file(model_num, sql_dir)
            
        if filename:
            print(f"Processing file: {filename}")
            model_version = filename.split(".")[0]
            
            execute_queries_from_file(sql_dir, filename)
        else:
            print(f"No file starting with model{model_num} and ending with 100.sql found in {sql_dir}")
            
    print("Warmup script completed.")