import pandas as pd
import os
import run_trino_query
import concurrent.futures
import warmup_script

def execute_queries_from_file(sql_dir, filename):
    with open(os.path.join(sql_dir, filename), "r") as file:
        queries = file.read().split("-- QUERY: ")
        results = run_and_record_queries(queries)

    return results


def run_in_parallel(queries):
    future_to_query = {}
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for q in queries[1:]:
            query_id_line, query_body = q.strip().split("\n", 1)
            query_id = query_id_line.strip()
            query_type = "MATCH_RECOGNIZE" if "MATCH_RECOGNIZE" in query_body.upper() else "REGEX"
            
            future_to_query[executor.submit(
                    run_trino_query.run_query, 
                        query_body, query_id, query_type)] = (query_id, query_body)
            
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
        print(f"\rExecuting query {index + 1}/{len(queries) - 1}", end='', flush=True)
        result = run_trino_query.run_query(query_body, query_id, query_type)
        results.append(result)

    print()
    return results

warmup_script.run_warmup_script()

for model_num in range(0, 16):
    sql_dir = f"data/queries/model{model_num}"
    
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql"):
            print(f"Processing file: {filename}")
            
            model_version = filename.split(".")[0]
            output_file = os.path.join(
                'results', f'model{model_num}', f'{model_version}_results.csv')
            
            results = execute_queries_from_file(sql_dir, filename)
            
            output_dir = f'results/model{model_num}'
            os.makedirs(output_dir, exist_ok=True)
            
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
