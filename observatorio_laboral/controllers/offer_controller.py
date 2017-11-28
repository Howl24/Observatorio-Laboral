from observatorio_laboral.models import offer

MONTH_IDX = 0
YEAR_IDX = 1


class OfferController(Object):

    def __init__(self):
        pass


    def load_offers(self, date_range=None):

        """
            date_range -> Tuple (min_date, max_date)
            min_date   -> Tuple (month, year)
            max_date   -> Tuple (month, year)
        """

        if date_range:
            min_val = min_date[YEAR_IDX] * MONTHS_PER_YEAR + min_date[MONTH_IDX]
            max_val = max_date[YEAR_IDX] * MONTHS_PER_YEAR + max_date[MONTH_IDX]

            for val in range(min_val, max_val +1):
                print(val)


offer_controller = OfferController()

ofer_controller.load_offers()
