from .keyword import KeyWord
from nltk.stem.snowball import SpanishStemmer
from nltk.corpus import stopwords

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


    def stem_text(self, text):
        newText = ""
        for word in text.split():
            wordStemmed = SpanishStemmer.stem(word)
            newText += wordStemmed + ' '

        return newText.strip()


    def process(self, category, proc_code, features):
        tok = CountVectorizer(stop_words=stopwords.words("spanish")).build_tokenizer()

        feats = []
        for feature in features:
            tokens = tok(feature)
            feats.append(tokens)

        data = " ".join(feats)
        data_stem = self.stem_text(data)
        found = set()
        if proc_code == FIND_WORDS_PROC_CODE:
            for kw in self.keywords[category]:
                for search_stem in kw.similars_stem:
                    if search_stem in data_stem.split():
                        found.add(kw.word)

                for search in kw.similars_no_stem:
                    if search in data.split():
                        found.add(kw.word)

        if proc_code == FIND_PRIORITY_PHRASE_PROC_CODE:
            for kw in self.keywords[category]:
                for search_stem in kw.similars_stem:
                    if search_stem in data_stem.split():
                        found.add(kw.word)

                for seach in kw.similars_no_stem:
                    if seach in data.split():
                        found.add(kw.word)

                # Return the first match
                if found:
                    return found

        return found

    def run(self, data):
        results = []
        for offer in data:
            of_res = {}
            for category, proc_code, features in configurations:
                feats = [feat for feat in offer if feat in features]

                of_res[category] = self.process(category, proc_code, feats)

            results.append(of_res)

        return results
