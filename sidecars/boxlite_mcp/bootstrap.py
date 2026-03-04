import duckdb
import os
import sys
import json

def bootstrap():
    # 1. Environment Parsing
    inputs = json.loads(os.getenv("SWANFLOW_INPUTS", "{}"))
    output_url = os.getenv("SWANFLOW_OUTPUT_URL")
    
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 2. Attach S3 inputs as views
    for table_name, url in inputs.items():
        print(f"[Bootstrap] Attaching {table_name} from S3...")
        con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{url}')")
    
    # 3. Execute User Code
    # The user code is expected to be in a file called 'user_code.py' or passed as a string
    try:
        # We provide the duckdb connection to the user script
        user_globals = {"db": con, "duckdb": duckdb}
        with open("user_code.py", "r") as f:
            exec(f.read(), user_globals)
        
        # 4. Export results
        if output_url:
            print(f"[Bootstrap] Exporting results to S3...")
            # We assume the user script populated a table or view called 'results'
            con.execute(f"COPY (SELECT * FROM results) TO '{output_url}' (FORMAT PARQUET)")
            
        print("[Bootstrap] Execution completed successfully.")
    except Exception as e:
        print(f"[Bootstrap] Error during execution: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    bootstrap()
