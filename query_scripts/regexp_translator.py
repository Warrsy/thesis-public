import logging
import re
import textwrap

class RegexpTranslator:
    def __init__(self, tokens, regexp_patterns):
        self.tokens = tokens
        self.pos = 0
        self.pattern_parts = []
        self.regexp_patterns = regexp_patterns

    def translate(self):
        while self.pos < len(self.tokens):
            self._parse_next()
            
    def _parse_next(self):
        token = self.tokens[self.pos]

        for name, pattern in self.regexp_patterns.items():
            if re.match(pattern, token):
                if name == "opening_negation":
                    self._parse_opening_not()
                    
                elif name == "closing_negation":
                    self._parse_closing_not()
                    
                elif name == "in-directly_follows" or name == "in-directly_any_follows":
                    self._parse_follows()
                    
                elif name == "directly_follows_negation":
                    self._parse_follows_negation()
                    
                elif name == "no_consecutive":
                    self._parse_no_consecutive()
                    
                elif name == "In-directly_follows_with_trailing":
                    self._parse_follows_trailing()
                    
                elif name == "closing_negation_OR" or name == "closing_directly_follows_negation":
                    self.pos += 1
                    
                else:
                    self.pos += 1  # Default increment if no specific parsing
                return  # Exit after a match is found and processed

        # If no pattern matches the token
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Unknown token: {token}")
        self.pos += 1

    def _parse_follows_trailing(self):
        follows_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        activities = self._extract_activities(follows_tokens)
        
        follow_segment = textwrap.dedent(f"""
            sequences_of_interest AS (
                WITH all_target_sequences AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        regexp_extract(
                            segment_after_first, '(({activities[0]}(,[^,]+)*,{activities[1]}(,[^,]+)*)|({activities[2]}(,[^,]+)*,{activities[3]}(,[^,]+)*))'
                        ) AS sequence
                    FROM initial_segment
                ),
                last_occurrence AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        LENGTH(sequence) AS last_occurrence_position
                    FROM all_target_sequences
                    WHERE LENGTH(sequence) > 0
                )
                SELECT
                    case_id,
                    segment_after_first,
                    last_occurrence_position,
                    substring(
                        segment_after_first FROM last_occurrence_position
                    ) AS segment_after_last_occurrence
                FROM last_occurrence
            )""")
        
        self.pattern_parts.append(follow_segment)
        self.pos += 1

    def _parse_no_consecutive(self):
        follows_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        activities = self._extract_activities(follows_tokens)
        
        follow_segment = textwrap.dedent(f"""
            sequences_of_interest AS (
                WITH all_target_sequences AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        regexp_extract_all(
                            segment_after_first, '{activities[0]}(,[^,]+)*?,{activities[2]}'
                        ) AS sequences
                    FROM initial_segment
                ),
                last_occurrence AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        sequences,
                        regexp_position(
                            segment_after_first, '{activities[0]}(,[^,]+)*?,{activities[2]}',1,
                            CAST(cardinality(sequences) AS INTEGER)
                        ) AS last_occurrence_position
                    FROM all_target_sequences
                    WHERE cardinality(sequences) > 0
                ),
                last_segment AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        last_occurrence_position,
                        substring(
                            segment_after_first FROM last_occurrence_position + LENGTH(sequences[cardinality(sequences)])
                        ) AS segment_after_last_occurrence
                    FROM last_occurrence
                )
                SELECT
                    case_id,
                    segment_after_first,
                    last_occurrence_position,
                    segment_after_last_occurrence
                FROM last_segment
                WHERE segment_after_last_occurrence NOT LIKE '%{activities[1]}%'
            )""")
        
        self.pattern_parts.append(follow_segment)
        self.pos += 1

    def _parse_follows_negation(self):
        follows_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        activities = self._extract_activities(follows_tokens)
        
        follow_segment = textwrap.dedent(f"""
            sequences_of_interest AS (
                SELECT
                    case_id,
                    segment_after_first,
                    '' AS segment_after_last_occurrence
                FROM initial_segment
                WHERE 
                    first_matched_activity IS NOT NULL AND 
                    segment_after_first NOT LIKE '%{activities[1]}%'
            )""")
        
        self.pattern_parts.append(follow_segment)
        self.pos += 1

    def _parse_opening_not(self):
        negation_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        negation_clause = self._create_activity_clause(negation_tokens)

        next_activities = self._find_next_activities()
        negation_clause += '|' + next_activities

        initial_segment = textwrap.dedent(f"""
            initial_segment AS (
                SELECT
                    rt.case_id,
                    SUBSTRING(rt.full_trace FROM first_occurrence.pos) AS segment_after_first,
                    first_occurrence.match AS first_matched_activity
                FROM raw_traces rt
                CROSS JOIN LATERAL ( 
                    SELECT
                        regexp_position(rt.full_trace, '({negation_clause})') AS pos,
                        regexp_extract(rt.full_trace, '({negation_clause})') AS match
                ) AS first_occurrence
                WHERE
                    regexp_like(first_occurrence.match, '{next_activities}') OR first_occurrence.match IS NULL
            ),""")
        
        self.pattern_parts.append(initial_segment)
        
        # print("Not Clause: " + initial_segment)


    def _parse_closing_not(self):
        negation_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        negation_clause = self._create_activity_clause(negation_tokens)

        closing_negation_segment = textwrap.dedent(f"""
            SELECT COUNT(ini.case_id)
            FROM initial_segment ini
            LEFT OUTER JOIN sequences_of_interest soi ON soi.case_id = ini.case_id
            WHERE
                (soi.segment_after_last_occurrence IS NULL AND 
                NOT regexp_like(ini.segment_after_first, '{negation_clause}')
                ) OR
                NOT regexp_like(soi.segment_after_last_occurrence, '{negation_clause}')""")
        
        # print("Closing Negation Segment: " + closing_negation_segment)
        self.pattern_parts.append(closing_negation_segment)

        self.pos += 1 # Skip ')'

    def _parse_follows(self):
        follows_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        activities = self._extract_activities(follows_tokens)
        
        follow_segment = textwrap.dedent(f"""
            sequences_of_interest AS (
                WITH all_target_sequences AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        regexp_extract(
                            segment_after_first, '{activities[0]}(,[^,]+)*,{activities[1]}'
                        ) AS sequence
                    FROM initial_segment
                ),
                last_occurrence AS (
                    SELECT
                        case_id,
                        segment_after_first,
                        LENGTH(sequence) AS last_occurrence_position
                    FROM all_target_sequences
                        WHERE sequence IS NOT NULL
                )
                SELECT
                    case_id,
                    segment_after_first,
                    last_occurrence_position,
                    substring(
                        segment_after_first FROM last_occurrence_position
                    ) AS segment_after_last_occurrence
                FROM last_occurrence
            )""")
        
        self.pattern_parts.append(follow_segment)
        self.pos += 1
        # print("Follow Segment: " + follow_segment)

    def _tokenize_to_operators(self, pattern):
        token_pattern = re.compile(r"""
            (
                \^|\$|\(|\)|\*|\||~>|ANY|NOT|'[^']*'
            )""", re.VERBOSE)
        
        return token_pattern.findall(pattern)

    def _extract_activities(self, tokens):
        activities = []
        for token in tokens:
            if token.startswith("'"):
                activities.append(token.strip("'"))

        return activities

    def _find_next_activities(self):
        self.pos += 1 
        activities = ''
        start_activity = True
        
        next_tokens = self._tokenize_to_operators(self.tokens[self.pos])
        for token in next_tokens:
            if token.startswith("'") and start_activity:
                activities += token.strip("'")
                start_activity = False

            if token == '|':
                start_activity = True
                activities += '|'
                
        return activities

    def _create_activity_clause(self, tokens):
        activities = []
        activity_clause = ''

        for token in tokens:
            if token.startswith("'"): # Is a literal
                activities.append(token)

        for index, activity in enumerate(activities):
            if activity.startswith("'"):
                activity_clause += activity.strip("'")

                if index !=  (len(activities) - 1):
                    activity_clause += '|'

        return activity_clause
    
    def _return_sequences(self):
        return self.pattern_parts