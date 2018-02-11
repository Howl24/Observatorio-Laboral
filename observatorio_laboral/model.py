from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from abc import ABC
from abc import abstractmethod
import logging


class CassandraModel(ABC):
    """
    Class that contains all the common functions for cassandra conection.

    Class variables:

        - cluster: Cluster instance shared for all the application

        - sessions: Dictionary
            Key     -> Keyspace name
            Value   -> Session instance

        - model_id : keyword to differentiate between child model classes

        - prepared_statements: Collection of prepared statements
            Structure: Dictionary
            key   -> Keyspace name
            value -> Prepared statements for that keyspace
                Structure : Dictionary
                    key   -> mix between child model class id and
                            command reference
                    value -> prepared statement to use

    Attributes:
        keyspace -> Child model keyspace
        table -> Child model table
    """

    cluster = Cluster()
    sessions = {}
    model_id = ""
    prepared_statements = {}

    @abstractmethod
    def __init__(self, keyspace, table):
        self.keyspace = keyspace
        self.table = table

    # ----------------------------------------------------------------------
    # Database Connection methods

    @classmethod
    def CreateKeyspace(cls, keyspace):
        """Only creates a keyspace, no more magic"""
        session = cls.cluster.connect()
        cmd = """
              CREATE KEYSPACE IF NOT EXISTS {0}
              WITH replication = {{
              'class': 'SimpleStrategy',
              'replication_factor' : '1'
              }};
              """.format(keyspace)

        session.execute(cmd)

    @classmethod
    def ConnectToDatabase(cls, keyspace, table=None, setup=False):
        """
        Method that initialize a connection to cassandra database and
        store it on the class variables.
        """
        keyspace = keyspace.lower()
        keyspace = keyspace.strip()

        if keyspace not in cls._get_keyspaces():
            try:
                cls.sessions[keyspace] = cls.cluster.connect(keyspace)
                cls.prepared_statements[keyspace] = {}
            except NoHostAvailable:
                logging.info("Ejecute el comando 'sudo cassandra -R' " +
                             "para iniciar el servicio de Cassandra")
                raise NoHostAvailable

        if setup:
            table_cmd = cls.DefineCreateTableCommand(keyspace, table)
            cls.sessions[keyspace].execute(table_cmd)
        else:
            # DefineStatements method must be override by child class
            statements = cls.DefineStatements(table)
            cls.PrepareStatements(keyspace, table, statements)

    @classmethod
    def PrepareStatements(cls, keyspace, table, statements):
        session = cls.sessions[keyspace]
        for cmd_key, statement in statements.items():
            key = cls._build_key(keyspace, table, cmd_key)
            cls.prepared_statements[key] = session.prepare(statement)

    # ----------------------------------------------------------------------
    # CQL execution methods

    @classmethod
    def RunStatement(cls, keyspace, statement, params=None):
        """Basic statement execution"""
        result = cls.sessions[keyspace].execute(statement, params)
        return result

    @classmethod
    def RunPreparedStatement(cls, keyspace, table, cmd_key, params=None):
        """Run a prepared statement by it's command key"""
        key = cls._build_key(keyspace, table, cmd_key)

        try:
            result = cls.RunStatement(keyspace,
                                      cls.prepared_statements[key], params)
        except KeyError:
            logging.info("Prepared statement " + key + " not found.")
            return None

        return result

    @classmethod
    def Query(cls, keyspace, table, cmd_key, params):
        rows = cls.RunPreparedStatement(keyspace, table, cmd_key, params)
        if not rows:
            return []
        else:
            return [cls.ByRow(keyspace, table, row) for row in rows]

    def Insert(self):
        row = self.ToRow()
        result = self.__class__.RunPreparedStatement(self.keyspace,
                                                     self.table,
                                                     cmd_key='insert',
                                                     params=row)
        return result

    # ----------------------------------------------------------------------
    # Child class methods to implement

    @classmethod
    @abstractmethod
    def DefineCreateTableCommand(cls, keyspace, table):
        logging.error("Create Table Command not defined!. \
                       Implement DefineCreateTableCommand method.")
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def DefineStatements(cls, table):
        logging.error("Statements not defined!. \
                       Implement DefineStatements method.")
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def ByRow(cls, keyspace, table, row):
        logging.error("By Row method not implemented.")
        raise NotImplementedError

    @abstractmethod
    def ToRow(self):
        logging.error("ToRow method not implemented.")
        raise NotImplementedError

    # ----------------------------------------------------------------------
    # Helper methods

    @classmethod
    def _get_keyspaces(cls):
        """
        Return a list of connected keyspaces
        """
        return list(cls.sessions.keys())

    @classmethod
    def _build_key(cls, keyspace, table, cmd_key):
        return "__".join((cls.model_id, keyspace, table, cmd_key))
