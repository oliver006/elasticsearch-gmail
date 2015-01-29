class DelegatingEmailParser(object):

    def __init__(self, parsers):
        self.parsers = parsers

    def parse(self, email):
        for parser in self.parsers:
            if parser.canParse(email):
                return parser.parse(email)

        return email
