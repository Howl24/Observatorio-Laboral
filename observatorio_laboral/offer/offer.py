from observatorio_laboral.model import CassandraModel


class Offer(CassandraModel):

    table = ""

    def __init__(self,
                 source, year, month, id,
                 features={}, careers=set()):

        self.source = source
        self.year = year
        self.month = month
        self.id = id
        self.features = features

        if careers is None:
            self.careers = set()
        else:
            self.careers = careers

    @classmethod
    def Setup(cls):
        cls.ConnectToDatabase("l4", "l4_offers", setup=True)
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            source text,
            year int,
            month int,
            id text,
            features map<text, text>,
            careers set<text>,
            PRIMARY KEY ((source, year, month), id));
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
            (source, year, month, id, features, careers)
            VALUES
            (?, ?, ?, ?, ?, ?);
            """.format(cls.table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE source = ?
            AND year = ?
            AND month = ?;
            """.format(cls.table)

        statements['select_all'] = \
            """
            SELECT * FROM {0};
            """.format(cls.table)

        cls.PrepareStatements(statements)

    @classmethod
    def ByRow(cls, row):
        return cls(row.source, row.year, row.month, row.id, row.features, row.careers)

    def ToRow(self):
        return (self.source, self.year, self.month, self.id, self.features, self.careers)

# Method to create tables
# Offer.Setup()
# Offer.ConnectToDatabase('l4', 'l4_offers')
