import sys
sys.path.insert(0, "../../")


from term import Term
from configuration import Configuration


class Dictionary(object):

    def __init__(self, dict_name, configurations=None, terms=None):
        Configuration.ConnectToDatabase("l4", "l4_configuration")
        Term.ConnectToDatabase("l4", "l4_terms")
        self.dict_name = dict_name
        self.load()

    def load(self):
        query_params = (self.dict_name,)
        self.configurations = Configuration.Query('select', query_params)
        self.terms = Term.Query('select', query_params)

#key = "sources"
#value = ["symplicity", "new_aptitus", "new_bumeran"]
#comment = "Listado de fuentes a utilizar"

#conf1 = Configuration("", str(key), str(value), comment)

#try:
#    d = Dictionary("Dict1", [conf1])
#except Exception as ex:
#    print(ex)

d = Dictionary('Dict1')
print(d.configurations)
print(d.terms)

