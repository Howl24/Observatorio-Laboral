import sys
sys.path.insert(0, "../../")


#from observatorio_laboral.offer import offer


MONTH_IDX = 0
YEAR_IDX = 1
MONTHS_PER_YEAR = 12


class OfferController(object):

    def __init__(self):
        pass


    def get_offers(self, date_range=None):

        """
            date_range -> Tuple (min_date, max_date)
            min_date   -> Tuple (month, year)
            max_date   -> Tuple (month, year)
        """

        min_date = date_range[0]
        max_date = date_range[1]

        if date_range:
            min_val = min_date[YEAR_IDX] * MONTHS_PER_YEAR + min_date[MONTH_IDX]
            max_val = max_date[YEAR_IDX] * MONTHS_PER_YEAR + max_date[MONTH_IDX]

            for val in range(min_val, max_val +1):
                print(val)



date_range = ((1, 2016), (5, 2017))

offer_controller = OfferController()
offer_controller.get_offers(date_range)
