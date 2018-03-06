from observatorio_laboral.keyword_classifier import Keyword
import json
from nltk.stem.snowball import SpanishStemmer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
import re


CONFIG_DIR = "observatorio_laboral/keyword_classifier/"
FIND_WORDS_PROC_CODE = 2
FIND_PRIORITY_PHRASE_PROC_CODE = 3
STEMMER = SpanishStemmer()
PUNCTUATIONS = '!"#$%&\'()*+,-./:;<=>?[\\]^_`{|}~'  # @ not included (@risk)


class KeywordClassifier():
    """ WorkFlow:
        - First, it reads a configuration file which contains a list of categories
           and for each one a processor code and an offer feature list where to find the keywords

        - Next, the classifier obtain a list of keywords by category and search them into
          the offer text. There are currently 3 types of search:

            1. FIND_PRIORITY
            2. FIND_WORDS
            3. CAREER
    """

    def __init__(self, keyspace, table, config_filename):
        self.keyspace = keyspace
        self.table = table
        self.config_filename = config_filename
        self.configurations = {}
        self.keywords = {}

        Keyword.ConnectToDatabase(self.keyspace, self.table)

        self._read_configuration()
        self._load_keywords()

    def _read_configuration(self):
        filename = CONFIG_DIR + self.config_filename + ".json"
        self.configurations = json.load(open(filename))['configurations']

    def _load_keywords(self):
        self.keywords = {}
        for configuration in self.configurations:
            category = configuration['category']
            proc_code = configuration['proc_code']
            fields = configuration['fields']
            self.keywords[category] = Keyword.Query(self.keyspace, self.table,
                                                    'select', (category,))

    #---------------------------------------------------------------------------

    def stem_text(self, text):
        newText = ""
        for word in text.split():
            wordStemmed = STEMMER.stem(word)
            newText += wordStemmed + ' '

        return newText.strip()


    def contained(self, phrase1, phrase2):
        x = phrase2.split()
        y = phrase1.split()

        l1, l2 = len(x), len(y)

        for i in range(l1):
            if x[i: i+l2] == y:
                return True

        return False


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


    def run(self, data):
        """Workflow execution shortcut. """
        results = []
        for offer in data:
            # Offer is a feature dictionary

            result = {}
            for configuration in self.configurations:
                category = configuration['category']
                proc_code = configuration['proc_code']
                features = configuration['fields']
                
                offer_features = [offer[feature] for feature in features if feature in offer]
                result[category] = self.process(category, proc_code, offer_features)
               
                if result[category] == set():
                    result[category].add("Otros/no-menciona")

            results.append(result)

        return results
