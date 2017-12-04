from term import Term

class TermController(object):

    def __init__(self):
        Term.ConnectToDatabase('l4', 'l4_terms')


