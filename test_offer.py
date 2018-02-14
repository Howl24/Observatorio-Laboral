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
                    
if __name__ == "__main__":
    test_import()
    #setup()
    #prepare_statements()

    #load_offers()

#date_range = DateRange(1, 2013, 5, 2017)
#source = "symplicity"

#try:
#    offer_controller = OfferController()
#    offers = offer_controller.get_offers(source, date_range)
##    print("Ofertas obtenidas: ", len(offers))
#except NoHostAvailable:
#    logging.info("No se pudo realizar la prueba")
#


# Get text

#print(offer_controller.get_text(offers[0], ["Job Title", "Description"]))
