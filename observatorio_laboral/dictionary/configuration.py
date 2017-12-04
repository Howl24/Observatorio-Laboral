#import sys
#sys.path.insert(0, "../../")
from observatorio_laboral.model import CassandraModel


class Configuration(CassandraModel):

    def __init__(self, dict_name, key, value, comment):
        self.dict_name = dict_name
        self.key = key
        self.value = value
        self.comment = comment

    @classmethod
    def Setup(cls):
        cls.ConnectToDatabase("l4", "l4_configuration", setup=True)
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            dict_name       text,
            key             text,
            value           text,
            comment         text,
            PRIMARY KEY ((dict_name), key));
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
            (dict_name, key, value, comment)
            VALUES
            (?, ?, ?, ?);
            """.format(cls.table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE dict_name = ?
            """.format(cls.table)

        statements['select_dictionary_names'] = \
            """
            SELECT DISTINCT dict_name FROM {0};
            """.format(cls.table)

        cls.PrepareStatements(statements)

    @classmethod
    def ByRow(cls, row):
        return cls(row.dict_name, row.key, row.value, row.comment)

    def ToRow(self):
        return (self.dict_name, self.key, self.value, self.comment)

#Configuration.Setup()
