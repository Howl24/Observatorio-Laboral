import logging
import ast
from cassandra.cluster import NoHostAvailable
#from observatorio_laboral.offer.offer_controller import OfferController
#from observatorio_laboral.offer.date_range import DateRange
from observatorio_laboral.offer.offer import Offer
from observatorio_laboral.offer import OfferController
from observatorio_laboral.offer import DateRange

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

# Test Connection

keyspace = "l4_test"
all_offers_table = "all_offers"
cleaned_offers_table = "reviewed_offers"
train_offers_table = "train_offers"

def setup():
    # Keyspace creation
    Offer.CreateKeyspace(keyspace)

    # Create Tables
    Offer.ConnectToDatabase(keyspace=keyspace, 
                            table=all_offers_table,
                            setup=True)
    
    Offer.ConnectToDatabase(keyspace=keyspace,
                            table=cleaned_offers_table,
                            setup=True)
    
    Offer.ConnectToDatabase(keyspace=keyspace,
                            table=train_offers_table,
                            setup=True)


def prepare_statements():
    Offer.ConnectToDatabase(keyspace=keyspace, 
                            table=all_offers_table)
    
    Offer.ConnectToDatabase(keyspace=keyspace,
                            table=cleaned_offers_table)
    
    Offer.ConnectToDatabase(keyspace=keyspace,
                            table=train_offers_table)
    
def load_offers():
    Offer.Import(keyspace, all_offers_table,
                 "Backup/Data/data_symplicity_new_offers")

def test_export():
    oc = OfferController('l4_test', '')


    Offer.Export(keyspace, all_offers_table,
                 "Backup/Data/Export/Last.csv", offer_list)

def test_import():
    keyspace = 'l4_test'
    table = 'all_offers'
    Offer.ConnectToDatabase(keyspace, table)
    Offer.Import(keyspace, table,
                 filename='/home/howl24/Data/test_data_aptitus_new_offers.csv')

#    oc = OfferController(keyspace='l4_test',
#                         table='all_offers')
#
#    oc.from_csv("/home/howl24/Data/test_data_aptitus_new_offers")
#

def test_backup():
    keyspace = 'l4_test'
    table = 'all_offers'
    Offer.ConnectToDatabase(keyspace, table)
    Offer.Backup(keyspace, table, "backup_file.csv")

def test_restore():
    keyspace = "l4_restore"
    table = "all_offers"

    Offer.CreateKeyspace(keyspace)
    Offer.ConnectToDatabase(keyspace,
                            table,
                            setup=True)
    Offer.ConnectToDatabase(keyspace,table)
    Offer.Restore(keyspace, table, "backup_file.csv")
                    
if __name__ == "__main__":
    #test_backup()
    test_restore()


    #test_import()
    #setup()
    #prepare_statements()

    #load_offers()
