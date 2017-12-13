#import sys
#sys.path.insert(0, "../../")

from .offer import Offer
from .date_range import DateRange

class OfferController(object):

    def __init__(self):
        Offer.ConnectToDatabase('l4', 'l4_offers')


    def get_offers(self, source=None, date_range=None, career=None):
        """
            date_range -> Tuple (min_date, max_date)
        """

        selected_offers = []
        for date in date_range:
            query_params = (source, date.get_year(), date.get_month())
            offers = Offer.Query('select', query_params)
            offers = [offer for offer in offers if career in offer.careers]
            selected_offers += offers

        return selected_offers

    def get_text(self, offers, text_fields):
        texts = []
        for offer in offers:
            offer_fields = []
            for field in text_fields:
                try:
                    offer_fields.append(offer.features[field])
                except KeyError:
                    offer_fields.append("")

            texts.append(" ".join(offer_fields))

        return texts

#date_range = DateRange(4, 2016, 6, 2016)
#source = "symplicity"
#
#offer_controller = OfferController()
#offer_controller.get_offers(source, date_range)
