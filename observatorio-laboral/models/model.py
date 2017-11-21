from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.query import BoundStatement


class CassandraModel():
    """
    Class that contains all the common functions for cassandra conection.

    cluster: Cluster instance

    sessions: Dictionary
        Key     -> Keyspace name
        Value   -> Session instance
    """

    cluster = Cluster()
    sessions = {}
    keyspace = None
    table = None
    prepared_statements = {}

    @classmethod
    def ConnectToDatabase(cls, keyspace, table):
        cls.keyspace = keyspace
        cls.table = table

        if cls.keyspace not in cls.sessions:
            cls.sessions[cls.keyspace] = cls.cluster.connect(cls.keyspace)

        return cls.sessions[keyspace]

    @classmethod
    def CreateTable(cls, table_creation_cmd):
        session = cls.sessions[cls.keyspace]
        session.execute(table_creation_cmd)

    @classmethod
    def PrepareStatements(cls, statements):
        session = cls.sessions[cls.keyspace]
        for key, statement in statements.items():
            cls.prepared_statements[key] = session.prepare(statement)

        return cls.prepared_statements

    @classmethod
    def PrintTable(cls):
        print(cls.table)

    @classmethod
    def Setup(cls):
        if table is None:
            print("Falta crear tablas")



