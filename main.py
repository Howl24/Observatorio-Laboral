from observatorio_laboral.offer.offer_controller import OfferController
from observatorio_laboral.offer.date_range import DateRange
from processor.control import Processor

class ReportGenerator():


    def __init__(self, report_names, sources, date_range, career):
        self.report_names = report_names
        self.sources = sources
        self.date_range = date_range
        self.career = career
        self.offer_controller = OfferController()

    def build_reports(self):
        reports = []
        for source in self.sources:
            offers = self.offer_controller.get_offers(source,
                                                      self.date_range,
                                                      self.career)

            proc_offers = self.process_offers(source, offers)
            l4_counts = self.count_offers(proc_offers)

            for report_name in self.report_names:
                report = self.build_report(source, report_name, offers, l4_counts)
                reports.append(report)

        self.export_ppt(reports)

    def export_ppt(self, reports):
        print(reports)

    def build_report(self, source, report_name, offers, l4_counts):
        filename = source + "-" + report_name
        if report_name == "perfiles":
            offers_by_label = {}

        return report_name

    def count_offers(self, offers):
        l4_count = {}
        for offer in offers:
            for field in offer.skills:
                if field not in l4_count:
                    l4_count[field] = {}

                for skill in offer.skills[field]:
                    if skill not in l4_count[field]:
                        l4_count[field][skill] = 0

                    l4_count[field][skill] += 1

        return l4_count

    def process_offers(self, source, offers):
        proc_filename = "proc_" + source
        processor = Processor(proc_filename)
        processor.connect_to_database()
        processor.read_configuration()
        processor.get_dictionaries()
        proc_offers = processor.process_offers(offers)

        return proc_offers



dr = DateRange(7, 2016, 6, 2017)
rg = ReportGenerator(['foo', 'bar'],
                     ['symplicity'],
                     dr,
                     "ECONOMÍA")

rg.build_reports()
