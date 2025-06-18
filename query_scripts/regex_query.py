import textwrap

class RegexQuery:
    def __init__(self, table_name, sequences, query_num):
        self.raw_traces = textwrap.dedent(f"""
            -- QUERY: {query_num}
            -- TYPE: regex
            WITH raw_traces AS (
                SELECT
                    case_id,
                    ARRAY_JOIN(ARRAY_AGG(activity ORDER BY position), ',') AS full_trace
                FROM {table_name}
                GROUP BY case_id
            ), """)
            
        self.sequences = sequences

    def build(self):
        query = self.raw_traces
        for sequence in self.sequences:
            query += sequence
            
        return query
    
    def __str__(self):
        return self.build()