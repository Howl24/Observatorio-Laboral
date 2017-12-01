from observatorio_laboral.utils.comparable_mixin import ComparableMixin

MONTHS_PER_YEAR = 12


class Date(ComparableMixin):

    def __init__(self, month=None, year=None, date=None):
        if not month or not year:
            self.months = None
        else:
            self.months = month + year * MONTHS_PER_YEAR

    def set_date(self, date):
        self.months = date.months
        return self

    def next_month(self):
        self.months += 1
        return self

    def prev_month(self):
        self.months -= 1
        return self

    def _cmpkey(self):
        return self.months

    def __str__(self):
        return str(str(self.get_month()) + "/" + str(str(self.get_year())))

    def get_month(self):
        return (self.months - 1) % MONTHS_PER_YEAR + 1

    def get_year(self):
        return self.months // MONTHS_PER_YEAR


class DateRange(object):

    def __init__(self, min_month, min_year, max_month, max_year):
        self.min_date = Date(min_month, min_year)
        self.max_date = Date(max_month, max_year)
        self.current = Date().set_date(self.min_date)
        self.current.prev_month()

    def __iter__(self):
        return self

    def __next__(self):
        self.current.next_month()
        if self.current > self.max_date:
            self.current.set_date(self.min_date)
            self.current.prev_month()
            raise StopIteration
        else:
            return self.current
