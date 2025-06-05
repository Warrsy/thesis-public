import logging

class MatchRecognizeTranslator:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0
        self.pattern_parts = []
        self.definitions = {}
        self.alias_counter = 0

    def translate(self):
        while self.pos < len(self.tokens):
            self._parse_next()


    def _parse_next(self):
        token = self.tokens[self.pos]
        if (self.pos == 1 and token != '^'):
            self.pattern_parts.append('^ANY*')
            self.pos += 1

        elif (self.pos == len(self.tokens) - 2 and token != '$'):
            self.pattern_parts.append('ANY*$')
            self.pos += 1

        elif token == 'NOT':
            self._parse_not()

        elif token.startswith("'"):
            self._parse_literal()

        elif token == '~>':
            # self._parse_follows()
            self.pattern_parts.append('(ANY)*')
            self.pos += 1

        elif token == 'ANY':
            # self._parse_any()
            self.pattern_parts.append('ANY')
            self.pos += 1

        elif token in ['^', '$', '*', '(', ')', '|']:
            self.pattern_parts.append(token)
            self.pos += 1

        else:
            self.pos += 1
            logging.basicConfig(level=logging.ERROR)
            logging.error(f"Unknown token: {token}")


    def _parse_any(self):
        self.pos += 1
        append_star = False
        if self.tokens[self.pos] == '*':
            append_star = True
            self.pos += 1

        alias = self._next_alias()
        self.definitions[alias] = f'{alias} AS activity != {self.tokens[self.pos]}'
        self.pattern_parts.append(alias)

        if append_star:
            self.pattern_parts.append('*')


    def _parse_follows(self):
        self.pos += 1
        
        alias = self._next_alias()
        self.definitions[alias] = f'{alias} AS activity != {self.tokens[self.pos]}'
        self.pattern_parts.append(alias)


    def _parse_not(self):
        self.pos += 1 # Skip 'NOT'
        activities = []

        if self.tokens[self.pos] == '(':
            self.pos += 1 # Skip '('
        
        while self.tokens[self.pos] != ')':
            if self.tokens[self.pos].startswith("'"):
                activities.append(self.tokens[self.pos])
                self.pos += 1

            elif self.tokens[self.pos] == '|':
                self.pos += 1
                continue

            else:
                self.pos += 1

        self.pos += 1 # Skip ')'

        alias = self._next_alias()
        self.definitions[alias] = self._create_definition(alias, activities)
        self.pattern_parts.append(alias)
        
    
    def _create_definition(self, alias, activities):
        definition = f'{alias} AS '

        for index, activity in enumerate(activities):
            definition += f'activity != {activity}'
            if index < len(activities) - 1:
                definition += ' AND '

        return definition


    def _next_alias(self):
        alias = chr(ord('A') + self.alias_counter)
        self.alias_counter += 1

        return alias
    
    def _parse_literal(self):
        alias = self._next_alias()
        self.definitions[alias] = f'{alias} AS activity = {self.tokens[self.pos]}'
        self.pattern_parts.append(alias)

        self.pos += 1


    def _format_pattern(self):
        pattern_string = ""

        for i, token in enumerate(self.pattern_parts):
            if i > 0 and self.pattern_parts[i] == '(':
                 pattern_string += ' '
                 pattern_string += '('

            elif token in {'(', ')', '*', '^', '$'}:
                pattern_string += token
            
            else:
                if i > 0 and self.pattern_parts[i - 1] not in {'(', '^'}:
                    pattern_string += ' '
                
                pattern_string += token
                
        return pattern_string


    def _format_definitions(self):
        return ', '.join(self.definitions.values())