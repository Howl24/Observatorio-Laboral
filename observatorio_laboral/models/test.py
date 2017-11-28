from cassandra.cluster import Cluster
from offer import OfferModel

cluster = Cluster()
session = cluster.connect()

rows = session.execute("select * from symplicity.new_offers")

OfferModel.ConnectToDatabase('l4', 'l4_offers')

source = "symplicity"
for row in rows:
    year = row.year
    month = row.month
    id = row.id
    features = row.features
    careers = row.careers

    offer = OfferModel(source, year, month, id, features, careers)
    offer.Insert()
