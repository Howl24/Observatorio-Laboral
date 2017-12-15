from observatorio_laboral.offer.offer_controller import OfferController
from sklearn.feature_extraction.text import TfidfVectorizer
from observatorio_laboral.offer.date_range import DateRange
from observatorio_laboral.dictionary.dictionary import Dictionary
from sklearn.naive_bayes import MultinomialNB
from sklearn.naive_bayes import BernoulliNB
from sklearn.svm import SVC
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn import cross_validation
import csv

import nltk 
offer_controller = OfferController()

def read_train_offers(labels):
    offers = offer_controller.get_offers("symplicity",
                                         DateRange(1, 2013, 12,2017),
                                         career = "ECONOMÍA")

    train_offers = []
    for offer in offers:
        offer_labels = offer.features['Areas'].split(",")
        if offer_labels:
            train_offer_labels = []
            for label in labels:
                if label in offer_labels:
                    train_offer_labels.append(label)

            train_offers.append((offer, train_offer_labels))

    return train_offers


labels = ["EI", "F", "TE", "OI", "MC", "P", "EM"]
train_offers = read_train_offers(labels)
predict_offers = offer_controller.get_offers("symplicity",
                                             DateRange(6, 2016, 7, 2017),
                                             career="ECONOMÍA")

dictionary = Dictionary("Diccionario_Economía")
vocab = [term.term for term in dictionary.terms if term.state == True]

vectorizer = TfidfVectorizer(tokenizer=nltk.word_tokenize,
                             ngram_range=(1,4),
                             vocabulary=vocab)

train_texts = []
train_labels = []
cnt = 0
for offer, labels in train_offers:
    text_fields = [offer.features["Job Title"],
                   offer.features["Description"],
                   offer.features["Qualifications"]]

    text = " ".join(text_fields)

    if labels == []:
        pass
    else:
        if "EM" in labels or "P" in labels or "F" in labels:
            cnt+= 1
            train_texts.append(text)
            train_labels.append(labels)

mlb = MultiLabelBinarizer()
train_labels = mlb.fit_transform(train_labels)
train_tfidf = vectorizer.fit_transform(train_texts).toarray()

classifier = OneVsRestClassifier(BernoulliNB())

score = cross_validation.cross_val_score(classifier,
                                         train_tfidf,
                                         train_labels,
                                         cv=10)
print(score)

#predict_texts = []
#for offer in predict_offers:
#    text_fields = [offer.features["Job Title"],
#                   offer.features["Description"],
#                   offer.features["Qualifications"]]
#
#    text = " ".join(text_fields)
#    predict_texts.append(text)
#
#predict_tfidf = vectorizer.transform(predict_texts).toarray()
#cls = classifier.fit(train_tfidf, train_labels)
#predictions = cls.predict(predict_tfidf)
#
#
#score = cross_validation.cross_val_score(classifier,
#                                         train_tfidf,
#                                         train_labels,
#                                         cv=2)
#
#print(score)
#predictions = mlb.inverse_transform(predictions)
#results = zip(predict_texts, predictions)

#with open("foo.txt", 'w') as csvfile:
#    writer = csv.DictWriter(csvfile, fieldnames = ['Texto', 'areas'])
#    writer.writeheader()
#
#    for text, pred in results:
#        writer.writerow({'Texto': text, "areas": pred})
