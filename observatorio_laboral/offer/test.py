from cassandra.cluster import Cluster
from offer import Offer

# INSERTION TEST
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

# SELECT TESTS
# BY SOURCE AND DATE RANGE
date_range = DateRange(4, 2016, 6, 2016)
source = "symplicity"

offer_controller = OfferController()
offer_controller.get_offers(source, date_range)


