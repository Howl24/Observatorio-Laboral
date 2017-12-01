import sys
sys.path.insert(0, "../../")


#from observatorio_laboral.offer import offer
from date_range import DateRange


MONTH_IDX = 0
YEAR_IDX = 1
MONTHS_PER_YEAR = 12


class OfferController(object):

    def __init__(self):
        pass


    def get_offers(self, date_range=None):
        pass

        """
            date_range -> Tuple (min_date, max_date)
            min_date   -> Tuple (month, year)
            max_date   -> Tuple (month, year)
        """

        for date in date_range:
            print(date)


date_range = DateRange(6, 2016, 3, 2017)
offer_controller = OfferController()
offer_controller.get_offers(date_range)
