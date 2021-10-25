# -*- coding: utf-8 -*-
"""sequence

Meant to run as offline tool to repopulate tables from different.

Options:
    sequence: Populate sequence table from sequencerun table


Usage:
    aws sso login --profile dev && export AWS_PROFILE=dev
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help repopulatetable
    python manage.py repopulatetable sequence
"""
import logging

from django.core.management import BaseCommand, CommandParser
from django.db import connection

from data_portal.models import SequenceStatus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""
The Following is SQL statement for populating sequence.
"""
TRUNCATE_SEQUENCE_TABLE_SQL = """
TRUNCATE TABLE data_portal_sequence;
"""

TAKE_LATEST_INTRUMENT_RUN_ID_DATA_SQL = """
SELECT date_modified,
    flowcell_barcode,
    gds_folder_path,
    gds_volume_name,
    sequencerun.instrument_run_id,
    reagent_barcode,
    run_id,
    sample_sheet_name,
    status
FROM   data_portal.data_portal_sequencerun sequencerun
    INNER JOIN (SELECT instrument_run_id,
                Max(date_modified) AS maxdate
            FROM   data_portal.data_portal_sequencerun
            GROUP  BY instrument_run_id) last_sequencerun
        ON sequencerun.instrument_run_id =
            last_sequencerun.instrument_run_id
            AND sequencerun.date_modified = last_sequencerun.maxdate 
"""

TAKE_INITIAL_INTRUMENT_RUN_ID_DATE_SQL = """
SELECT Min(date_modified) AS mindate
FROM   data_portal.data_portal_sequencerun
WHERE  instrument_run_id = %s
GROUP  BY instrument_run_id 
"""

INSERT_SEQUENCE_TABLE_SQL = """
INSERT INTO data_portal_sequence
    (
        instrument_run_id,
        run_id,
        sample_sheet_name,
        gds_folder_path,
        gds_volume_name,
        reagent_barcode,
        flowcell_barcode,
        status ,
        start_time ,
        end_time
    )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


"""
The following are SQL statement for libraryRun
"""


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('table', help="defines which table to repopulate")

    def handle(self, *args, **options):
        opt_table=options["table"].lower()

        if opt_table =="sequence":
            logger.info("Sequence table selected")
            with connection.cursor() as cursor:
                logger.info("Truncate sequence table")
                cursor.execute(TRUNCATE_SEQUENCE_TABLE_SQL)

                logger.info("Fetch latest data of instrument_run_id")
                cursor.execute(TAKE_LATEST_INTRUMENT_RUN_ID_DATA_SQL)
                latest_data = cursor.fetchall()

                logger.info("Iterate each latest data")
                for row in latest_data:
                    logger.info("Destructuring varibles for each row")
                    end_time, flowcell_barcode, gds_folder_path, \
                    gds_volume_name, instrument_run_id, reagent_barcode, \
                    run_id, sample_sheet_name, status = row

                    logger.info(f"Fetch start_time for {instrument_run_id}")
                    cursor.execute(TAKE_INITIAL_INTRUMENT_RUN_ID_DATE_SQL, \
                                [instrument_run_id])
                    start_time = cursor.fetchone()[0]
                    
                    status = SequenceStatus.from_seq_run_status(status)
                    if status not in [SequenceStatus.SUCCEEDED, SequenceStatus.FAILED]:
                        logger.info("Sequence not finish setting end_time to None")
                        end_time=None
                    
                    logger.info("Insert row to sequence table from data fetched")
                    cursor.execute(INSERT_SEQUENCE_TABLE_SQL, [instrument_run_id,
                        run_id, sample_sheet_name,gds_folder_path,
                        gds_volume_name, reagent_barcode, flowcell_barcode,
                        status, start_time, end_time])
                
                logger.info("Repopulate sequence table complete")

        elif opt_table=="libraryrun":
            logger.info("LibraryRun table selected")

            with connection.cursor() as cursor:




