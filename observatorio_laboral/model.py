from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from abc import ABC
from abc import abstractmethod
import logging
import csv


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
    fields = []
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
    def RunStatement(cls, keyspace, statement, params=None, async=False):
        """Basic statement execution"""
        if async:
            result = cls.sessions[keyspace].execute_async(statement, params)
        else:
            result = cls.sessions[keyspace].execute(statement, params)
        return result

    @classmethod
    def RunPreparedStatement(cls, keyspace, table, cmd_key, params=None,
                             async=False):
        """Run a prepared statement by it's command key"""
        key = cls._build_key(keyspace, table, cmd_key)

        try:
            result = cls.RunStatement(keyspace,
                                      cls.prepared_statements[key], params,
                                      async=async)
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
            return [cls.FromNamedTuple(keyspace, table, row) for row in rows]

    def Insert(self):
        row = self.ToTuple()
        result = self.__class__.RunPreparedStatement(self.keyspace,
                                                     self.table,
                                                     cmd_key='insert',
                                                     params=row)
        return result

    @classmethod
    def InsertListAsync(self, obj_list):
        param_list = []
        for obj in obj_list:
            param_list.append(obj)

        future_list = []
        for param in param_list:
            future_resp = self.__class__.RunPreparedStatement(self.keyspace,
                                                              self.table,
                                                              cmd_key="insert",
                                                              params=param,
                                                              async=True)
            future_list.append(future_resp)

        for future_resp in future_list:
            future_resp.result()

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
    def FromNamedTuple(cls, keyspace, table, row):
        logging.error("FromNamedTuple method not implemented.")
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def FromTextNamedTuple(cls, keyspace, table, row):
        logging.error("FromTextNamedTuple method not implemented.")
        raise NotImplementedError

    @abstractmethod
    def ToTuple(self):
        logging.error("ToTuple method not implemented.")
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

    # ----------------------------------------------------------------------
    # Backup Utils

    @classmethod
    def Backup(cls, keyspace, table, filename):
        """ Creates a backup file with all the data in the table"""
        # Prepared Statement instead of Query method
        # to work with lazy memory usage.
        # offers = cls.Query(keyspace, table, "select_all", ())

        rows = cls.RunPreparedStatement(keyspace, table, "select_all", ())

        with open(filename, "w") as csvfile:
            fieldnames = cls.fields
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                    delimiter="|",
                                    quotechar='^', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in rows:
                writer.writerow(row._asdict())

    @classmethod
    def Restore(cls, keyspace, table, filename):
        """ Restore a previous backup file """

        cmd = "TRUNCATE {0}".format(table)
        cls.RunStatement(keyspace, cmd)

        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile,
                                    delimiter="|",
                                    quotechar='^', quoting=csv.QUOTE_ALL)

            if reader.fieldnames != cls.fields:
                logging.error("Incorrect file header")
            else:
                logging.info("Correct file header")
                params_list = []
                for row in reader:
                    obj = cls.FromTextNamedTuple(keyspace, table, row)
                    params = obj.ToTuple()
                    params_list.append(params)

                cls.InsertListAsync()
