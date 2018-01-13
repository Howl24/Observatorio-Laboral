from .term import Term
from .configuration import Configuration
from observatorio_laboral.offer.offer_controller import OfferController

from sklearn.feature_extraction.text import TfidfVectorizer

import pickle

TEXT_FIELDS = ["Job Title",
               "Description",
               "Qualification",
               ]

PICKLE_DIR = "./Pickle/"


class Dictionary(object):
    """Data Structure used to represent a document by a collection of words.

    Implements:
        transform

    """

    def __init__(self, name):
        self.name = name
        self.oc = OfferController(text_fields=TEXT_FIELDS)
        pass
        # Configuration.ConnectToDatabase("l4", "l4_configuration")
        # Term.ConnectToDatabase("l4", "l4_terms")
        # self.dict_name = dict_name
        # self.load()
        # self.offer_controller = OfferController()
        # self.term_controller = TermController(self.configurations,
        #                                       self.offer_controller)

    def load(self):
        query_params = (self.dict_name,)
        self.configurations = Configuration.Query('select', query_params)
        self.terms = Term.Query('select', query_params)

    def save(self):
        for configuration in self.configurations:
            configuration.Insert()

        for term in self.terms:
            term.Insert()

    def add_configuration(self, configuration):
        if self.dict_name == configuration.dict_name:
            self.configurations.append(configuration)
        else:
            raise Exception("La configuración no pertenece al diccionario")

    def add_term(self, term):
        if self.dict_name == term.dict_name:
            self.terms.append(term)
        else:
            raise Exception("El término no pertenece al diccionario")

    def _get_configuration(self, key):
        for configuration in self.configurations:
            if configuration.key == key:
                return eval(configuration.value)

        raise Exception("No se encontro la clave de configuración")

    def update_terms(self):
        offers = self.offer_controller.get_offers(
                 self._get_configuration('source'),
                 self._get_configuration('date_range'),
                 self._get_configuration('career')
                )

        terms = self.term_controller.get_terms(
                offers
                )

        print(len(terms))

    def fit(self, offers):
        offer_texts = [self.oc.get_text(offer) for offer in offers]

        tfidf_vect = TfidfVectorizer()
        tfidf_vect.fit(offer_texts)
        pickle.dump(tfidf_vect, open(PICKLE_DIR + self.name + ".p", "wb"))

    def transform(self, offers):
        offer_texts = [self.oc.get_text(offer) for offer in offers]
        tfidf_vect = pickle.load(open(PICKLE_DIR + self.name + ".p", "rb"))
        tfidf = tfidf_vect.transform(offer_texts)
        return tfidf
