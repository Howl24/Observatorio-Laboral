from .term import Term
from nltk.tokenize import word_tokenize
from nltk.tag.stanford import StanfordPOSTagger
import os


class TermController(object):

    def __init__(self, configurations=None, offer_controller=None):
        Term.ConnectToDatabase('l4', 'l4_terms')
        self.configurations = configurations
        self.offer_controller = offer_controller

    def get_terms(self, offers):
        text_fields = self._get_configuration('text_fields')
        texts = self.offer_controller.get_text(offers, text_fields)
        texts = self.preprocess(texts)
        tags_by_term = _run_tagger(texts)

        vectorizer = TfidfVectorizer(tokenizer=nltk.word_tokenize,
                                     ngram_range=(1, 4),
                                     min_df=15,
                                     max_df=0.5)

        vectorizer.fit(data)
        all_terms = set(vectorizer.get_feature_names())
        vocab = self.get_vocab(all_terms, all_forms, tags_by_term)
        return vocab

    def get_vocab(self, terms, all_forms, tags_by_term):
        for phrase in terms:
            words = phrase.split()
            # get term formula by length
            forms = all_forms[len(words)]

            for key in forms:
                try:
                    accept = True
                    for idx, word in words:
                        accept = accept and key[idx] == tags_by_term[word]
                except:
                    pass

    def _get_configuration(self, key):
        for configuration in self.configurations:
            if configuration.key == key:
                return eval(configuration.value)

        raise Exception("No se encontro la clave de configuración")

    def preprocess(self, texts):
        punctuations = ['•','/', ')', '-']
        translator = str.maketrans("".join(punctuations),' '*len(punctuations))

        proc_texts = []
        for text in texts:
            text = text.lower()
            text = text.translate(translator)
            proc_texts.append(text)
            
        return proc_texts

    def _run_tagger(self, texts):
        java_path = "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
        os.environ['JAVAHOME'] = java_path

        spanish_postagger = StanfordPOSTagger(
                            'models/spanish.tagger',
                            './stanford-postagger.jar')

        tag_list = ['n', 'v', 'a', 'c', 'd', 'f', 'i', 'p', 'r', 's', 'w', 'z']

        terms_by_tag = {}
        for tag in tag_list:
            terms_by_tag[tag] = set()

        tags_by_term = {}
        
        print("Inicia revision")
        for idx, text in enumerate(data):
            if idx % 100 == 0:
                print(" Avance: ", idx,
                      " Tam. terms.: ",
                      len(terms_by_tag['n']) + len(terms_by_tag['v']))
            try:
                signal.alarm(4)
                words = word_tokenize(text)
                
                new_words = False
                for word in words:
                    if word not in tags_by_term:
                        new_words = True
        
                if new_words: 
                    tag_terms = spanish_postagger.tag(words)
                    for term in tag_terms:
                        word = term[0]
                        tag = term[1][0]
                        
                        terms_by_tag[tag].add(word)
                        tags_by_term[word] = tag
        
            except TimeoutError as ex:
                print(" Excede tiempo en: ", idx)
                
            signal.alarm(0)

        return tags_by_term
