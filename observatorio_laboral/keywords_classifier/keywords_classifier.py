from .keyword import KeyWord

def readNumber(file, error=None):
    try:
        number = int(file.readline())
    except ValueError:
        return error
    return number

class KeyWordClassifier():
    keyspace = "l4"
    table = "keywords"

    def __init__(self, config_filename):
        self.config_filename = config_filename
        self.configurations = []
        self.keywords = {}

        KeyWord.ConnectToDatabase(self.keyspace, self.table)

    def read_configuration(self):
        with open(self.config_filename, 'r') as file:
            self.source = file.readline().strip()

            keep_reading = True
            while keep_reading:
                config = self.read_category_configuration(file)
                if not config:
                    keep_reading = False
                else:
                    self.configurations.append(config)


    def read_category_configuration(self, file):
        category = file.readline().strip()
        proc_code = readNumber(file, 0)
        num_features = readNumber(file, 0)

        features = []
        for i in range(num_features):
            features.append(file.readline().strip())

        if not category or not proc_code or not features:
            return None

        return (category, proc_code, features)

    def load_keywords(self):
        self.keywords = {}
        for (category, proc_code, features) in self.configurations:
            query_params = (category,)
            print(query_params)
            self.keywords[category] = KeyWord.Query('select', query_params)
