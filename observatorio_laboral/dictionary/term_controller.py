from term import Term

class TermController(object):

    def __init__(self):
        Term.ConnectToDatabase('l4', 'l4_terms')

    def get_terms_by_configuration(self):
        pass




