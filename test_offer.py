import logging
from cassandra.cluster import NoHostAvailable
#from observatorio_laboral.offer.offer_controller import OfferController
#from observatorio_laboral.offer.date_range import DateRange
from observatorio_laboral.offer.offer import Offer

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

# Test Connection

# Setup
keyspace = "l4_test"

# Test Keyspace creation
#Offer.CreateKeyspace(keyspace)
#

all_offers_table = "all_offers"
cleaned_offers_table = "reviewed_offers"
train_offers_table = "train_offers"
#
## Create Tables
#Offer.ConnectToDatabase(keyspace=keyspace, 
#                        table=all_offers_table,
#                        setup=True)
#
#Offer.ConnectToDatabase(keyspace=keyspace,
#                        table=cleaned_offers_table,
#                        setup=True)
#
#Offer.ConnectToDatabase(keyspace=keyspace,
#                        table=all_offers_table,
#                        setup=True)

# Prepare Statements

Offer.ConnectToDatabase(keyspace=keyspace, 
                        table=all_offers_table)

Offer.ConnectToDatabase(keyspace=keyspace,
                        table=cleaned_offers_table)

Offer.ConnectToDatabase(keyspace=keyspace,
                        table=all_offers_table)

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
