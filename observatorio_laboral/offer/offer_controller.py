from .offer import Offer


class OfferController(object):

    def __init__(self, keyspace, table,
                 offers=[]):
        self.keyspace = keyspace
        self.table = table
        self.offers = offers

        Offer.ConnectToDatabase(self.keyspace, self.table)

    def load_offers(self, source, date_range, career=None):
        """ Get offers by source, date_range and career.
            date_range -> DateRange instance or some iterable
                          with get_year and get_month methods

            career = None load all the offers from a source
                          over a date range
        """
        self.offers = []
        for date in date_range:
            if career == None:
                params = (source, date.get_year(), date.get_month())
                query_offers = Offer.Query(self.keyspace,
                                           self.table,
                                           "select",
                                           params)
            else:
                params = (source, date.get_year(), date.get_month(), career)
                query_offers = Offer.Query(self.keyspace,
                                           self.table,
                                           "select_by_career",
                                           params)

            self.offers += query_offers

    def filter_by_field(self, field, labels=None):
        """ Filter offers by a dictionary params
            Remove offer that don't contain the parameter field
            Remove offer that don't contain any label in the list.
        """
        new_offer_list = []
        for offer in self.offers:
            if field in offer.features:
                if not labels:
                    new_offer_list.append(offer)
                else:
                    offer_labels = offer.get_field_labels(field)
                    for label in labels:
                        if label in offer_labels:
                            new_offer_list.append(offer)
                            break

        self.offers = new_offer_list

    def find(self, source, year, month, id, career=""):
        """ Return an offer from the list."""
        for offer in self.offers:
            if offer.source == source and \
               offer.year == year and \
               offer.month == month and \
               offer.career == career and \
               offer.id == id:

                return offer

    def get_text(self, text_fields):
        """ Group get text shortcut"""
        return [offer.get_text(text_fields) for offer in self.offers]

    def get_field_labels(self, field, labels=None):
        """ Group get_field_labels method shortcut """
        return [offer.get_field_labels(field, labels) for offer in self.offers]
