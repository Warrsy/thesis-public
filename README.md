# Thesis Scripts

This repository contains all the scripts used for my Bachelor's thesis:  
**A Comparative Study of MATCH_RECOGNIZE and REGEXP-Based SQL Approaches for Process Querying**

## 📁 Repository Structure

```
/benchmark/     # Benchmarking scripts and performance measurement tools

/query_scripts/ # Scripts for generating and translating SQL queries

/data_scripts/  # Scripts for loading and managing data in the database
```

## 📝 Script Descriptions

### /benchmark/
- **benchmark_script.py**: Runs the main benchmarking process, executing SQL queries and saving results.
- **consolidate_data.py**: Aggregates and processes benchmark result CSVs into a single file for analysis.
- **run_trino_query.py**: Handles execution and timing of SQL queries against the Trino server.
- **warmup_script.py**: Executes a set of queries to warm up the database/cache before benchmarking.

### /query_scripts/
- **convert_signal_queries_to_sql.py**: Converts signal queries from CSV to SQL files for benchmarking.
- **match_recognize_query.py**: Builds SQL queries using the MATCH_RECOGNIZE clause.
- **match_recognize_translator.py**: Translates signal queries into MATCH_RECOGNIZE SQL patterns.
- **regex_query.py**: Builds SQL queries using regular expressions.
- **regexp_translator.py**: Translates signal queries into SQL regular expression patterns.

### /data_scripts/
- **load_csv_files_to_db.py**: Loads CSV data files into the PostgreSQL database.

## ⚙️ How to Run
1. **Download the dataset and queries** from [this Google Drive link](https://drive.google.com/drive/folders/1OExouU7yRUBSY-i2_xz8x3qUrv5bEYK4?usp=drive_link).

2. **Create the following directory structure** inside your repository:
```
/data/          # Raw and processed data used in the thesis
  ├── models/   # Folder containing the process model datasets
  └── queries/  # Folder containing translated queries for benchmarking
```
3. Place the downloaded datasets into the `/data/models/` folder, and the queries into `/data/queries/`.

4. Follow the setup instructions in `setup_db.md` to configure the Trino database.

5. Run the data loading script:

```bash
python data_scripts/load_csv_files_to_db.py
```
6. Once the data is loaded, you can run the benchmark:
```bash
python benchmark/benchmark_script.py
```

## 📄 Thesis Document

The full thesis is available at [link].
#### 👩‍🔬 Author

- **Name**: Daniel Hylander

- **Affiliation**: Umeå University / Department of Computing Science

- **Email**: daniel.hylander1@gmail.com
