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
        """

        selected_offers = []
        for date in date_range:
            query_params = (source, date.get_year(), date.get_month())
            offers = Offer.Query('select', query_params)
            selected_offers += offers

        return selected_offers

date_range = DateRange(4, 2016, 6, 2016)
source = "symplicity"

offer_controller = OfferController()
offer_controller.get_offers(source, date_range)
