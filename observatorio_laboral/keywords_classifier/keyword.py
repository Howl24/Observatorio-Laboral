from observatorio_laboral.model import CassandraModel


class KeyWord(CassandraModel):

    table = ""

    def __init__(self, category, word, similars_stem=[], similars_no_stem=[]):
        self.category = category
        self.word = word
        self.similars_stem = similars_stem
        self.similars_no_stem = similars_no_stem

    @classmethod
    def Setup(cls):
        cls.ConnectToDatabase("l4", "keywords", setup=True)
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            category text,
            word text,
            similars_stem list<text>,
            similars_no_stem list<text>,
            PRIMARY KEY (category, word));
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
            (category, word, similars_stem, similars_no_stem)
            VALUES
            (?, ?, ?, ?);
            """.format(cls.table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE category = ?;
            """.format(cls.table)

        cls.PrepareStatements(statements)

    @classmethod
    def ByRow(cls, row):
        return cls(row.category, row.word, row.similars_stem, row.similars_no_stem)

    def ToRow(self):
        return (self.category, self.word, self.similars_stem, self.similars_no_stem)
