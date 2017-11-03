from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.query import BoundStatement

import abc


class CassandraModel(object, metaclass=abc.ABCMeta):
    """
    Class that contains all the common functions for cassandra conection.
    """

    @classmethod
    @abc.abstractmethod
    def CreateTables(cls):
        raise NotImplementedError('Users must define CreateTables to use this class')
    

    @classmethod
    @abc.abstractmethod
    def ConnectToDatabase(cls):
        raise NotImplementedError('Users must define ConnectToDatabase to use this class')


#"""Class Example

#class Foo(CassandraModel):
#
#    def __init__(self):
#        pass
#
#    @classmethod
#    def CreateTables(cls):
#        pass
#
#    @classmethod
#    def ConnectToDatabase(cls):
#        pass

#"""
