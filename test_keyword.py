from observatorio_laboral.keywords_classifier.keywords_classifier import KeyWordClassifier
from observatorio_laboral.keywords_classifier.keyword import KeyWord

# Setup
# KeyWord.Setup()

kwc = KeyWordClassifier("proc_symplicity.txt")
kwc.read_configuration()
kwc.load_keywords()
print(kwc.keywords)
