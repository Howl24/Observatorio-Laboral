import sys
sys.path.insert(0, "../../")

from offer import Offer
from date_range import DateRange

class OfferController(object):

    def __init__(self):
        Offer.ConnectToDatabase('l4', 'l4_offers')


    def get_offers(self, source=None, date_range=None):
        """
            date_range -> Tuple (min_date, max_date)
            min_date   -> Tuple (month, year)
            max_date   -> Tuple (month, year)
        """

        for date in date_range:
            query_params = (source, date.get_month(), date.get_year())
            offers = Offer.Query('select', query_params)
            print(date)


# INSERTION TEST
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()

rows = session.execute("select * from symplicity.new_offers")

Offer.ConnectToDatabase('l4', 'l4_offers')

source = "symplicity"
for row in rows:
    year = row.year
    month = row.month
    id = row.id
    features = row.features
    careers = row.careers

    offer = Offer(source, year, month, id, features, careers)
    offer.Insert()

#date_range = DateRange(6, 2016, 3, 2017)
#source = "symplicity"
#
#offer_controller = OfferController()
#offer_controller.get_offers(source, date_range)
