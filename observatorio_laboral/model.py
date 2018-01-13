from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.query import BoundStatement
from abc import ABC
import logging


class CassandraModel(ABC):
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
        cls.prepared_statements = {}

        if cls.keyspace not in cls.sessions:
            try:
                cls.sessions[cls.keyspace] = cls.cluster.connect(cls.keyspace)
            except NoHostAvailable:
                logging.info("Ejecute el comando 'sudo cassandra -R' " + \
                             "para iniciar el servicio de Casssandra")
                raise NoHostAvailable

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

    @classmethod
    def RunStatement(cls, statement, params=None):
        stmt = cls.prepared_statements[statement]
        result = cls.sessions[cls.keyspace].execute(stmt, params)
        return result

    @classmethod
    def ByRow(cls, row):
        raise NotImplementedError

    @classmethod
    def ToRow(cls):
        raise NotImplementedError

    def Insert(self): 
        row = self.ToRow()
        result = self.__class__.RunStatement('insert', row)

    @classmethod
    def Query(cls, statement, pk, custom=False):
        rows = cls.RunStatement(statement, pk)

        if not rows:
            return []
        else:
            if custom:
                return list(rows)
            else:
                return [cls.ByRow(row) for row in rows]
