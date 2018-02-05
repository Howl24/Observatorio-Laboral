from observatorio_laboral.model import CassandraModel


class Offer(CassandraModel):
    """
    Class that contains the structure of an job offer.

    Class variables:
        - model_id -> Offer model identifier ("offer")
                      Used in  cassandra model conection

    Attributes:
        - source -> Job Source (symplicity, aptitus, bumeran, etc.)
        - year   -> Published year
        - month  -> Published month
        - career -> Career related to the offer.
                    "Unassigned" if it's already not assigned to one.
        - id     -> hash obtained by some offer features 
                    (title, description, etc.)
        - features -> Dictionary containing the offer features
    """

    model_id = "offer"


    def __init__(self, keyspace, table,
                 source, year, month, # Partition key
                 careers, id,         # Clusters
                 features={}):

        super().__init__(keyspace, table)
        self.source = source
        self.year = year
        self.month = month
        self.career = career
        self.id = id
        self.features = features

    # ----------------------------------------------------------------------
    # Cassandra methods
    @classmethod
    def DefineCreateTableCommand(cls, keyspace, table):
        table_creation_cmd = \
            """
            CREATE TABLE IF NOT EXISTS {0} (
            source text,
            year int,
            month int,
            career text,
            id text,
            features map<text, text>,
            PRIMARY KEY ((source, year, month), career, id));
            """.format(table)

        return table_creation_cmd

    @classmethod
    def DefineStatements(cls, table):
        statements = {}
        statements['insert'] = \
            """
            INSERT INTO {0}
            (source, year, month, id, career, features)
            VALUES
            (?, ?, ?, ?, ?, ?);
            """.format(table)

        statements['select'] = \
            """
            SELECT * FROM {0}
            WHERE source = ?
            AND year = ?
            AND month = ?;
            """.format(table)

        statements['select_by_career'] = \
            """
            SELECT * FROM {0}
            WHERE source = ?
            AND year = ?
            AND month = ?
            AND career = ?;
            """.format(table)

        statements['select_by_id'] = \
            """
            SELECT * FROM {0}
            WHERE source = ?
            AND year = ?
            AND month = ?
            AND career = ?
            AND id = ?;
            """.format(table)

        statements['select_all'] = \
            """
            SELECT * FROM {0};
            """.format(table)

        return statements

    @classmethod
    def ByRow(cls, keyspace, table, row):
        """Match __init__ method."""
        return cls(keyspace = keyspace,
                   table = table,
                   source = row.source,
                   year = row.year,
                   month = row.month,
                   career = row.career,
                   id = row.id,
                   features = row.features)
                   
    def ToRow(self):
        """Match 'insert' statement."""
        return (self.source,
                self.year,
                self.month,
                self.career,
                self.id,
                self.features)
