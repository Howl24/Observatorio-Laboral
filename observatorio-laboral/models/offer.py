from model import CassandraModel

class Offer(CassandraModel):

    def __init__(self,
                 source, year, month, id,
                 features={}, careers=set(), skills={}):
        pass

    @classmethod
    def Setup(cls):
        cls.ConnectToDatabase("l4", "l4_offers")
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
    def ConnectToDatabase(cls, keyspace, table):
        super().ConnectToDatabase(keyspace, table)
        statements = {}
        statements['insert'] = \
                """
                INSERT INTO {0}
                (source, year, month, id, features, careers)
                VALUES
                (?, ?, ?, ?, ?, ?);
                """.format(cls.table)

        cls.PrepareStatements(statements)

#Offer.Setup()
Offer.ConnectToDatabase('l4', 'l4_offers')
