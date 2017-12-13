import sys
sys.path.insert(0, "../../")


from term import Term
from term_controller import TermController
from configuration import Configuration
from observatorio_laboral.offer.offer_controller import OfferController
from observatorio_laboral.offer.date_range import DateRange


class Dictionary(object):

    def __init__(self, dict_name):
        Configuration.ConnectToDatabase("l4", "l4_configuration")
        Term.ConnectToDatabase("l4", "l4_terms")
        self.dict_name = dict_name
        self.load()
        self.offer_controller = OfferController()
        self.term_controller = TermController(self.configurations,
                                              self.offer_controller)


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

# Configuracion de diccionario de economia

D_ECO = "Diccionario_Economía"
#d = Dictionary(D_ECO)
#d.add_configuration(Configuration(D_ECO,
#                                  "text_fields",
#                                  "['Description', 'Job Title', 'Qualifications', 'Software']", 
#                                  "campos a utilizar para obtener texto de una oferta"))
#d.save()
#d.add_configuration(Configuration(D_ECO, "career", "'ECONOMÍA'",
#                                  "carrera a utilizar para este diccionario"))
#d.add_configuration(Configuration(D_ECO, "date_range", "DateRange(1, 2013, 12, 2017)",
#                                  "Rango de fecha a utilizar. Evaluar DateRange(value)"))
#
#d.save()

d = Dictionary(D_ECO)
d.update_terms()
