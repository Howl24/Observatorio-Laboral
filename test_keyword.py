from observatorio_laboral.keywords_classifier.keywords_classifier import KeywordClassifier
from observatorio_laboral.keywords_classifier.keyword import Keyword

# Setup
# KeyWord.Setup()

kwc = KeywordClassifier("proc_symplicity.txt")
kwc.read_configuration()
kwc.load_keywords()
kwc.run()


