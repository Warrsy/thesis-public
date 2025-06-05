
import textwrap

class MatchRecognizeQuery:
    def __init__(self, model_id, pattern, definitions, query_num, table_name):
        self.select_clause = textwrap.dedent(f"""
            -- QUERY: {query_num}
            -- TYPE: MATCH_RECOGNIZE
            SELECT COUNT(case_id)""")
        self.from_clause = textwrap.dedent(f"""
            FROM {table_name}""")
        self.match_recognize_clause = textwrap.dedent(f"""
            MATCH_RECOGNIZE (
                PARTITION BY case_id
                ORDER BY position
                ONE ROW PER MATCH
                PATTERN {pattern}
                DEFINE {definitions}
            )
            """)

    def build(self):
        return f"""
        {self.select_clause}{self.from_clause}{self.match_recognize_clause}
        """
    
    def __str__(self):
        return self.build()