from .offer import Offer

DELIMITER = ","

class OfferController(object):
    keyspace = "l4"
    table = "l4_offers"

    def __init__(self, text_fields, offers = []):
        self.text_fields = text_fields
        self.offers = offers
        Offer.ConnectToDatabase(self.keyspace, self.table)

    def load_offers(self, source, date_range, career=None):
        """ Get offers by source, date_range and career.
            date_range -> DateRange instance
        """
        self.offers = []
        for date in date_range:
            query_params = (source, date.get_year(), date.get_month())
            query_offers = Offer.Query('select', query_params)
            if career:
                query_offers = [offer for offer in offers if career in offer.careers]
            self.offers += query_offers

    def filter_offers_by_field(self, field, ignore=[]):
        """ Filter offers by a dictionary params """

        new_offer_list = []
        for offer in self.offers:
            if field in offer.features:
                if not ignore:
                    new_offer_list.append(offer)
                else:
                    ig = False
                    for label in offer.features[field].split(DELIMITER):
                        if label in ignore:
                            ig = True

                    if not ig:
                        new_offer_list.append(offer)



        self.offers = new_offer_list

    def filter_offers_by_career(self, career):
        new_offer_list = []
        for offer in self.offers:
            if career in offer.careers:
                new_offer_list.append(offer)

        self.offers = new_offer_list

    def find(self, source, year, month, id):
        for offer in self.offers:
            if offer.source == source and \
               offer.year == year and \
               offer.month == month and \
               offer.id == id:

                return offer

    def get_text(self, offer=None):
        """ Get text from offer features. """

        if not offer:
            return [self.get_text(offer) for offer in self.offers]

        fields = []
        for field in self.text_fields:
            try:
                fields.append(offer.features[field])
            except KeyError:
                continue

        return " ".join(fields)

    def get_field_labels(self, field, offer=None, ignore=[]):
        if not offer:
            return [self.get_field_labels(field, offer, ignore) for offer in self.offers]

        if field in offer.features:
            labels = [label for label in offer.features[field].split(DELIMITER) if label not in ignore]
            return labels

        else:
            return []
