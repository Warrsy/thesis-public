import pandas as pd
import re
import match_recognize_translator as mrt
import match_recognize_query as mrq
import regexp_translator as rt
import regex_query as rq
import textwrap
import logging
from pathlib import Path

PERCENT_RANGE = range(10, 101, 10)
DATA_DIR = Path(__file__).parent.parent / 'data' / 'queries'
CSV_PATH = DATA_DIR / 'signal_queries.csv'


regexp_patterns = {
    "opening_negation": 
        r"\^\s*NOT\(\s'[^']*?'\s\)|\^\sNOT\(\s'[^']*?'\s\|\s'[^']*?'\s\)\*",
    "closing_negation": 
        r"NOT\(\s'[^']*?'\s\)\*\$|NOT\(\s'[^']*?'\s\|\s'[^']*?'\s\)\*\s*\$",
    "closing_negation_OR": 
        r"\(\(\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\)\*\s\|\s\(\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\)\)\*\s*\$",
    "closing_directly_follows_negation": 
        r"\(\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\*\)\s*\$",
    "in-directly_follows": 
        r"\(\s'[^']*?'\s~>\s'[^']*?'\s\)\*",
    "in-directly_any_follows": 
        r"\(\s'[^']*?'\sANY\*\s'[^']*?'\s\)\*",
    "directly_follows_negation": 
        r"\(\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\*\)\*",
    "no_consecutive": 
        r"\(\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\*\s'[^']*?'\sNOT\(\s'[^']*?'\s\)\*\)\*",
    "In-directly_follows_with_trailing": 
        r"\(\(\s'[^']*?'\sANY\*\s'[^']*?'\sANY\*\)\s\|\s\(\s'[^']*?'\sANY\*\s'[^']*?'\sANY\*\)\)\*"
}

def main():
    logging.basicConfig(level=logging.INFO)
    
    dataFrame = pd.read_csv(CSV_PATH)
    unique_models = dataFrame['model_id'].unique()
    
    logging.info(f"Loaded {len(unique_models)} unique models from {CSV_PATH}")
    
    for model_num, model_id in enumerate(unique_models):
        df_subset = dataFrame[dataFrame['model_id'] == model_id]
        process_model(model_num, model_id, df_subset)


def process_model(model_num: int, model_id: str, df_subset: pd.DataFrame):
    model_dir = DATA_DIR / f"model{model_num}"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    for percent in PERCENT_RANGE:
        process_percent(df_subset, model_num, model_id, percent, model_dir)


def process_percent(df_subset: pd.DataFrame, model_num: int, model_id: str, percent: int, model_dir: Path):
    sql_queries = []
    table_name = f"postgresql.public.model{model_num}_{model_id}_{percent}"
    output_file = model_dir / f"model{model_num}_{model_id}_{percent}.sql"
    
    for query_num, (_, row) in enumerate(df_subset.iterrows()):
        signal_query = row['signal_query']
        match_query = find_matches(signal_query)

        if match_query == '':
            activity = find_activity_in_query(signal_query)
            regex_query, match_recognize_query = create_queries(activity, query_num, table_name)
            
        else:
            tokenized_query_match_recognize = tokenize_signal_query_match_recognize(match_query)
            tokenized_query_regex = tokenize_signal_query_regex(match_query)
            
            patternTranslator = mrt.MatchRecognizeTranslator(tokenized_query_match_recognize)
            regexpTranslator = rt.RegexpTranslator(tokenized_query_regex, regexp_patterns)
            
            patternTranslator.translate()
            regexpTranslator.translate()
            
            pattern = patternTranslator._format_pattern()
            definitions = patternTranslator._format_definitions()
            sequences = regexpTranslator._return_sequences()

            match_recognize_query = mrq.MatchRecognizeQuery(pattern, definitions, query_num, table_name)
            regex_query = rq.RegexQuery(table_name, sequences, query_num)

        sql_queries.append(match_recognize_query)
        sql_queries.append(regex_query)

    with open(output_file, 'w') as f:
        for query in sql_queries:
            f.write(str(query).strip() + "\n\n")


def tokenize_signal_query_regex(match_query):
    patterns = list(regexp_patterns.values())
    combined_pattern = r"(" + r"|".join(patterns) + r")"
    token_pattern = re.compile(combined_pattern, re.VERBOSE)

    tokens = token_pattern.findall(match_query)
    
    return [token.strip() for token in tokens if token.strip()]


def tokenize_signal_query_match_recognize(match_query):
    token_pattern = re.compile(r"""
        (
            \^|\$|\(|\)|\*|\||~>|ANY|NOT|'[^']*'
        )""", re.VERBOSE)
    tokens = token_pattern.findall(match_query)
    
    return [token.strip() for token in tokens if token.strip()]


def find_matches(signal_query):
    match_pattern = re.compile(r"""
        (
            ^MATCHES\s*.*
        )""", re.VERBOSE | re.MULTILINE | re.DOTALL)
    
    if match_pattern.findall(signal_query) == []:
        return ''
    
    return match_pattern.findall(signal_query)[0]


def find_activity_in_query(signal_query):
    activity_pattern = re.compile(r"""
        (
            '.*'
        )""", re.VERBOSE | re.MULTILINE | re.DOTALL)
    
    return activity_pattern.findall(signal_query)[0]


def create_queries(activity, query_num, table_name):
    regex_query = textwrap.dedent(f"""
        -- QUERY: {query_num}
        -- TYPE: regex
        WITH raw_traces AS (
            SELECT
                case_id,
                ARRAY_JOIN(ARRAY_AGG(activity ORDER BY position), ',') AS full_trace
            FROM
                {table_name}
            GROUP BY
                case_id
        )
        SELECT
            COUNT(case_id)
        FROM
            raw_traces rt
        WHERE
            regexp_like(full_trace, {activity})
        """)
        
    match_recognize_query = textwrap.dedent(f"""
        -- QUERY: {query_num}
        -- TYPE: MATCH_RECOGNIZE
        SELECT COUNT(case_id)
        FROM {table_name}
        MATCH_RECOGNIZE (
            PARTITION BY case_id
            ORDER BY position
            ONE ROW PER MATCH
            PATTERN (^ANY* A ANY*$)
            DEFINE A AS activity = {activity})
    """)
    
    return regex_query, match_recognize_query


if __name__ == "__main__":
    main()