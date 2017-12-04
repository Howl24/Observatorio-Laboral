#import sys
#sys.path.insert(0, "../../")
from observatorio_laboral.model import CassandraModel


class Term(CassandraModel):

    def __init__(self, dict_name, term, representative, state, idf):
        self.dict_name = dict_name
        self.term = term
        self.representative = representative
        self.state = state
        self.idf = idf

    @classmethod
    def Setup(cls):
        cls.ConnectToDatabase("l4", "l4_terms", setup=True)
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            dict_name       text,
            term            text,
            representative  text,
            state           boolean,
            idf             float,
            PRIMARY KEY ((dict_name), term));
            """.format(cls.table)

        cls.CreateTable(table_creation_cmd)

    @classmethod
    def ConnectToDatabase(cls, keyspace, table, setup=False):
        super().ConnectToDatabase(keyspace, table)
        if setup:
            return

        statements = {}
        statements['insert'] = \
            """
            INSERT INTO {0}
            (dict_name, term, representative, state, idf)
            VALUES
            (?, ?, ?, ?, ?);
            """.format(cls.table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE dict_name = ?
            """.format(cls.table)

        cls.PrepareStatements(statements)

    @classmethod
    def ByRow(cls, row):
        return cls(row.dict_name, row.term, row.representative,
                   row.state, row.idf)

    def ToRow(self):
        return (self.dict_name, self.term, self.representative,
                self.state, self.idf)

#Term.Setup()
