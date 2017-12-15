from observatorio_laboral.offer.offer_controller import OfferController
from observatorio_laboral.offer.date_range import DateRange
from observatorio_laboral.dictionary.dictionary import Dictionary

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
