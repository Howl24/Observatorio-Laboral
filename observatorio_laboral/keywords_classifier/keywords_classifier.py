from .keyword import Keyword
from nltk.stem.snowball import SpanishStemmer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
import re

def readNumber(file, error=None):
    try:
        number = int(file.readline())
    except ValueError:
        return error
    return number


FIND_WORDS_PROC_CODE = 2
FIND_PRIORITY_PHRASE_PROC_CODE = 3
STEMMER = SpanishStemmer()
PUNCTUATIONS = '!"#$%&\'()*+,-./:;<=>?[\\]^_`{|}~'  # @ not included (@risk)

class KeywordClassifier():
    keyspace = "l4"
    table = "keywords"

    def __init__(self, config_filename):
        self.config_filename = config_filename
        self.configurations = []
        self.keywords = {}

        Keyword.ConnectToDatabase(self.keyspace, self.table)

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
            self.keywords[category] = Keyword.Query('select', query_params)


    def stem_text(self, text):
        newText = ""
        for word in text.split():
            wordStemmed = STEMMER.stem(word)
            newText += wordStemmed + ' '

        return newText.strip()


    def process(self, category, proc_code, features):
        tok = CountVectorizer().build_tokenizer()

        data = " ".join(features)
        data = data.lower()


        regex = re.compile('[%s]' % re.escape(PUNCTUATIONS))
        data = regex.sub(' ', data)

        tokens = [word for word in tok(data) if word not in stopwords.words('spanish')]

        data = " ".join(tokens)
        data_stem = self.stem_text(data)
        found = set()
        if proc_code == FIND_WORDS_PROC_CODE:
            for kw in self.keywords[category]:
                for search_stem in kw.similars_stem:
                    if self.contained(search_stem, data_stem):
                        found.add(kw.word)

                for search in kw.similars_no_stem:
                    if self.contained(search, data_stem):
                        found.add(kw.word)

        if proc_code == FIND_PRIORITY_PHRASE_PROC_CODE:
            for kw in self.keywords[category]:
                for search_stem in kw.similars_stem:
                    if self.contained(search_stem, data_stem):
                        found.add(kw.word)

                for search in kw.similars_no_stem:
                    if self.contained(search, data):
                        found.add(kw.word)

                # Return the first match
                if found:
                    return found

        return found

    def contained(self, phrase1, phrase2):
        x = phrase2.split()
        y = phrase1.split()

        l1, l2 = len(x), len(y)

        for i in range(l1):
            if x[i: i+l2] == y:
                return True

        return False


    def run(self, data):
        results = []
        for offer in data:
            result = {}
            for category, proc_code, features in self.configurations:
                feats = []
                for f in features:
                    if f in offer:
                        feats.append(offer[f])
                    #else:
                        #print(f)
                        #print(offer)

                result[category] = self.process(category, proc_code, feats)
                if result[category] == set():
                    result[category].add("Otros/no-menciona")

            results.append(result)

        return results
