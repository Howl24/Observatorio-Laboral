from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.query import BoundStatement

import abc


class CassandraModel(object, metaclass=abc.ABCMeta):

    """
    Class that contains all the common functions for cassandra conection.
    """

    @abc.abstractmethod
    def CreateTables(cls):
        raise NotImplementedError('Users must define CreateTables to use this class')
    

    @abc.abstractmethod
    def ConnectToDatabase(cls):
        raise NotImplementedError('Users must define ConnectToDatabase to use this class')
