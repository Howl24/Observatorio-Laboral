from .offer import Offer


class OfferController(object):
    keyspace = "l4"
    table = "l4_offers"

    def __init__(self, text_fields):
        self.text_fields = text_fields
        Offer.ConnectToDatabase(self.keyspace, self.table)

    def get_offers(self, source, date_range, career=None):
        """ Get offers by source, date_range and career.
            date_range -> DateRange instance
        """
        selected_offers = []
        for date in date_range:
            query_params = (source, date.get_year(), date.get_month())
            offers = Offer.Query('select', query_params)
            if career:
                offers = [offer for offer in offers if career in offer.careers]
            selected_offers += offers

        return selected_offers

    def get_text(self, offer):
        """ Get text from offer features. """

        fields = []
        for field in self.text_fields:
            try:
                fields.append(offer.features[field])
            except KeyError:
                continue

        return " ".join(fields)
