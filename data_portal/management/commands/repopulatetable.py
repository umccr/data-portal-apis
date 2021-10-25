# -*- coding: utf-8 -*-
"""sequence

Meant to run as offline tool to repopulate tables from different.

Options:
    sequence: Populate sequence table from sequencerun table
    libraryrun: Populate libraryrun table based from sequence, fastq, metadata, and workflow tables


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

QUERY_LATEST_INTRUMENT_RUN_ID_DATA_SQL = """
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

QUERY_INITIAL_INTRUMENT_RUN_ID_DATE_SQL = """
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
TRUNCATE_LIBRARYRUN_TABLE_SQL = """
TRUNCATE TABLE data_portal_libraryrun;
"""

QUERY_SEQUENCE_TABLE_SQL = """
SELECT instrument_run_id,
       run_id
FROM   data_portal.data_portal_sequence 
"""

QUERY_RGLB_LANE_FROM_FASTQLIST_SQL = """
SELECT rglb,
       lane
FROM   data_portal.data_portal_fastqlistrow fastq
       inner join data_portal.data_portal_sequencerun sequencerun
               ON fastq.sequence_run_id = sequencerun.id
WHERE  sequencerun.instrument_run_id = %s
       AND sequencerun.run_id = %s 
"""

QUERY_OVERRIDE_CYCLES_SQL = """
SELECT override_cycles
FROM   data_portal.data_portal_labmetadata
WHERE  library_id = %s
"""

QUERY_WORKFLOW_FROM_LIBRARY_ID_SQL = """
SELECT id
FROM   data_portal.data_portal_workflow
WHERE  ( input LIKE %s
          OR output LIKE %s )
"""

INSERT_LIBRARYRUN_VALUE_SQL = """
INSERT INTO data_portal_libraryrun
    (
        library_id,
        instrument_run_id ,
        run_id,
        lane,
        override_cycles,
        coverage_yield,
        qc_pass ,
        qc_status ,
        valid_for_analysis
    )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id;
"""

DROP_LIBRARYRUN_WORKFLOW_TABLE_SQL = """
Drop TABLE IF EXISTS data_portal_libraryrun_workflows
"""

CREATE_LIBRARYRUN_WORKFLOW_TABLE_SQL = """
CREATE TABLE data_portal_libraryrun_workflows
  (
     id            BIGINT(20) NOT NULL auto_increment,
     libraryrun_id BIGINT(20) NOT NULL,
     workflow_id   BIGINT(20) NOT NULL,
     PRIMARY KEY (id),
     UNIQUE KEY data_portal_libraryrun_w_libraryrun_id_workflow_i_d2b4b128_uniq
        (libraryrun_id, workflow_id),
     KEY data_portal_libraryr_workflow_id_7f31cc94_fk_data_port (workflow_id),
     CONSTRAINT data_portal_libraryr_libraryrun_id_3bcbad1b_fk_data_port FOREIGN
     KEY (libraryrun_id) REFERENCES data_portal_libraryrun (id),
     CONSTRAINT data_portal_libraryr_workflow_id_7f31cc94_fk_data_port FOREIGN
     KEY (workflow_id) REFERENCES data_portal_workflow (id)
  ) 
"""

INSERT_LIBRARYRUN_WORKFLOW_TABLE_SQL = """
INSERT INTO data_portal_libraryrun_workflows
    (
        libraryrun_id,
        workflow_id
    )
VALUES (%s, %s);
"""


class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser):
        parser.add_argument('table', help="defines which table to repopulate")

    def handle(self, *args, **options):
        opt_table = options["table"].lower()

        if opt_table == "sequence":
            logger.info("Sequence table selected")

            with connection.cursor() as cursor:

                # Reset existing data
                logger.info("Truncate sequence table")
                cursor.execute(TRUNCATE_SEQUENCE_TABLE_SQL)

                # Fetch latest data of instrument_run_id
                logger.info("Fetch latest data of instrument_run_id")
                cursor.execute(QUERY_LATEST_INTRUMENT_RUN_ID_DATA_SQL)
                latest_data = cursor.fetchall()

                logger.info("Iterate each latest data")
                for row in latest_data:
                    # Destructuring varibles for each row
                    end_time, flowcell_barcode, gds_folder_path, \
                    gds_volume_name, instrument_run_id, reagent_barcode, \
                    run_id, sample_sheet_name, status = row

                    # Fetch start_time of sequence
                    cursor.execute(QUERY_INITIAL_INTRUMENT_RUN_ID_DATE_SQL,
                                   [instrument_run_id])
                    start_time = cursor.fetchone()[0]

                    # Check if sequence has ended and end_time is eligibled to be recorded
                    status = SequenceStatus.from_seq_run_status(status)
                    if status not in [SequenceStatus.SUCCEEDED, SequenceStatus.FAILED]:
                        logger.info(f"Sequence {instrument_run_id} not finish setting end_time to None")
                        end_time = None

                    logger.info(f"Insert sequence {instrument_run_id} to sequence table from data fetched")
                    cursor.execute(INSERT_SEQUENCE_TABLE_SQL, [instrument_run_id,
                                                               run_id, sample_sheet_name, gds_folder_path,
                                                               gds_volume_name, reagent_barcode, flowcell_barcode,
                                                               status, start_time, end_time])

                logger.info("Repopulate sequence table complete")

        elif opt_table == "libraryrun":
            logger.info("LibraryRun table selected")

            # Default value
            coverage_yield = None
            qc_pass = False
            qc_status = None
            valid_for_analysis = True

            with connection.cursor() as cursor:

                # Associated constrain must be drop before reset libraryrun
                logger.info("Drop data_portal_libraryrun_workflows table")
                cursor.execute(DROP_LIBRARYRUN_WORKFLOW_TABLE_SQL)

                # Reset libraryrun 
                logger.info("Drop data_portal_libraryrun_workflows table")
                cursor.execute(TRUNCATE_LIBRARYRUN_TABLE_SQL)

                # Establish new libraryrun constrain
                logger.info("Create a brand new libraryrun_workflow table")
                cursor.execute(CREATE_LIBRARYRUN_WORKFLOW_TABLE_SQL)

                # Fetch List of intrument_run_id and run_id from sequence
                logger.info("Fetch sequence run")
                cursor.execute(QUERY_SEQUENCE_TABLE_SQL)
                sequence_list = cursor.fetchall()

                for instrument_run_id, run_id in sequence_list:
                    # Grab RGLB and lane from fastqlist
                    cursor.execute(QUERY_RGLB_LANE_FROM_FASTQLIST_SQL, [instrument_run_id, run_id])
                    fastq_list = cursor.fetchall()

                    for library_id, lane in fastq_list:

                        # Find overide_cycles from metadata
                        cursor.execute(QUERY_OVERRIDE_CYCLES_SQL, [library_id])
                        override_cycles = cursor.fetchone()[0]

                        # Insert libraryrun entries to the table
                        logger.info(f"Inserting {instrument_run_id}, {run_id}, {library_id},  {lane} entries to "
                                    f"data_portal_libraryrun table.")
                        cursor.execute(INSERT_LIBRARYRUN_VALUE_SQL, [library_id, instrument_run_id, run_id, lane,
                                                                 override_cycles, coverage_yield, qc_pass, qc_status,
                                                                 valid_for_analysis])
                        inserted_libraryrun_id = cursor.fetchone()[0]

                        # Extract associated workflow with libraryid
                        cursor.execute(QUERY_WORKFLOW_FROM_LIBRARY_ID_SQL, [f"%{library_id}%", f"%{library_id}%"])
                        workflow_id_list = cursor.fetchall()

                        # Insert associated workflow to data_portal_libraryrun_workflow
                        for workflow_id in workflow_id_list:
                            cursor.execute(INSERT_LIBRARYRUN_WORKFLOW_TABLE_SQL, [inserted_libraryrun_id, workflow_id])

                logger.info("LibraryRun successfully repopulated.")
