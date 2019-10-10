from django.core.management import BaseCommand, CommandParser

from data_processors.persist_lims_data import persist_lims_data


class Command(BaseCommand):
    """
    Command to execute LIMS data rewrite with specified csv (s3) location
    """
    def add_arguments(self, parser: CommandParser):
        parser.add_argument('-csv_bucket_name', '--csv_bucket_name')
        parser.add_argument('-csv_key', '--csv_key')

    def handle(self, *args, **options):
        csv_bucket_name = options['csv_bucket_name']
        csv_key = options['csv_key']

        persist_lims_data(csv_bucket_name, csv_key, True)