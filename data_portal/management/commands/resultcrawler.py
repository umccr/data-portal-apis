# -*- coding: utf-8 -*-
"""Analysis Result Index Builder

Crawling process is idempotent. You can crawl the same index Lookup model as many times as you like. It resolves to
the same index condition at point-in-time and, reflects the underlay cloud object "live" state i.e. present or absent.

Concept: Index Lookup Key
ImplNote - Note that we can choose to use any business key for lookup purpose to build the index. In this case, we
choose internal SubjectID (SBJ) for historical reason. We may, however, choose to use other business key like LibraryID.
See `AnalysisResult` model note.

Usage:
    export AWS_PROFILE=umccr-dev-admin
    aws sso login
    make up
    export DJANGO_SETTINGS_MODULE=data_portal.settings.local
    python manage.py migrate
    python manage.py help resultcrawler
    python manage.py resultcrawler --subject_id SBJ02060 --dry --log
    python manage.py resultcrawler --reverse --count 10 --dry --log

Example Use Case: Crawl recent 10 Subjects and build their analysis result index
    python manage.py resultcrawler -l -r -c 10

Example Use Case: Crawl a Subject and build its analysis result index
    python manage.py resultcrawler -l -s SBJ02060

Example Use Case: Crawl all Subjects by Instrument Run ID and build its analysis result index
    python manage.py resultcrawler -l -i 241108_A01052_0238_BH2NFGDSXF
"""
import logging
import re
from datetime import datetime

from django.core.management import BaseCommand

from data_portal.models import S3Object, GDSFile, AnalysisResult, LabMetadata, LIMSRow
from data_portal.models.analysisresult import Lookup, PlatformGeneration, AnalysisMethod

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def is_valid_subject_id(subject_id: str):
    return re.match(r'^SBJ\d{5}$', subject_id)


def is_valid_instrument_run_id(instrument_run_id: str):
    return re.match(r'^\d{6}_A\d{5}_\d{4}_[A-Z0-9]{10}$', instrument_run_id)


class Command(BaseCommand):

    def __init__(self, *args, **options):
        super(Command, self).__init__(*args, **options)
        self.key = None

    def build_index_by_subject(self):
        if self.key is None:
            logger.info("Key is not set, skipping.")
            return

        bcbio_results_by_subject = S3Object.objects.get_subject_results(self.key).all()

        gds_results_by_subject = GDSFile.objects.get_subject_results(self.key).all()
        sash_results_by_subject = S3Object.objects.get_subject_sash_results(self.key).all()

        byob_cttsov2_results_by_subject = S3Object.objects.get_subject_cttsov2_results_from_byob(self.key).all()
        byob_wgts_results_by_subject = S3Object.objects.get_subject_wgts_results_from_byob(self.key).all()
        byob_sash_results_by_subject = S3Object.objects.get_subject_sash_results_from_byob(self.key).all()

        if bcbio_results_by_subject.exists():
            AnalysisResult.objects.create_or_update(
                lookup=Lookup(self.key, PlatformGeneration.ONE),
                s3objects=bcbio_results_by_subject,
            )

        if sash_results_by_subject.exists() or gds_results_by_subject.exists():
            AnalysisResult.objects.create_or_update(
                lookup=Lookup(self.key, PlatformGeneration.TWO),
                s3objects=sash_results_by_subject,
                gdsfiles=gds_results_by_subject,
            )

        if byob_cttsov2_results_by_subject.exists():
            AnalysisResult.objects.create_or_update(
                lookup=Lookup(self.key, PlatformGeneration.THREE, AnalysisMethod.TSO500V2),
                s3objects=byob_cttsov2_results_by_subject,
            )

        if byob_wgts_results_by_subject.exists():
            AnalysisResult.objects.create_or_update(
                lookup=Lookup(self.key, PlatformGeneration.THREE, AnalysisMethod.WGTS),
                s3objects=byob_wgts_results_by_subject,
            )

        if byob_sash_results_by_subject.exists():
            AnalysisResult.objects.create_or_update(
                lookup=Lookup(self.key, PlatformGeneration.THREE, AnalysisMethod.SASH),
                s3objects=byob_sash_results_by_subject,
            )

    def add_arguments(self, parser):
        parser.add_argument('-s', '--subject_id', action='store')
        parser.add_argument('-i', '--instrument_run_id', action='store')
        parser.add_argument('-c', '--count', help="If count, crawl upto count limit", action="store")
        parser.add_argument('-r', '--reverse', help="If reverse, crawl recent first", action="store_true")
        parser.add_argument('-d', '--dry', help="Dry run", action="store_true")
        parser.add_argument('-l', '--log', help="Output to log file", action="store_true")

    def handle(self, *args, **options):
        opt_subject_id = options['subject_id']
        opt_instrument_run_id = options['instrument_run_id']
        opt_count = options['count']
        opt_reverse = options['reverse']
        opt_dry = options['dry']
        opt_log = options['log']

        if opt_count is None:
            opt_count = -1
        else:
            opt_count = int(opt_count)

        if opt_reverse is None:
            opt_reverse = False
        else:
            opt_reverse = bool(opt_reverse)

        if opt_log:
            log_file = logging.FileHandler("resultcrawler-{}.log".format(datetime.now().strftime("%Y%m%d%H%M%S")))
            log_file.setLevel(logging.INFO)
            log_file.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
            logger.addHandler(log_file)

        if opt_subject_id:
            if not is_valid_subject_id(opt_subject_id):
                logger.info("Not a valid Subject ID")
                exit(1)

            logger.info(f"Building index for lookup key: {opt_subject_id}")
            self.key = opt_subject_id
            if opt_dry:
                logger.info(f"DRY RUN: {self.key}")
            else:
                self.build_index_by_subject()

        else:

            subject_set = set()

            if opt_instrument_run_id:
                # subjects by instrument_run_id
                if not is_valid_instrument_run_id(opt_instrument_run_id):
                    logger.info("Not a valid Instrument Run ID")
                    exit(1)

                for s in LIMSRow.objects.filter(illumina_id=opt_instrument_run_id).values('subject_id'):
                    if not s['subject_id']:
                        continue
                    subject_set.add(s['subject_id'])
            else:
                # all subjects
                for s in LabMetadata.objects.order_by().values('subject_id'):
                    if not s['subject_id']:
                        continue
                    subject_set.add(s['subject_id'])
                for s in LIMSRow.objects.order_by().values('subject_id'):
                    if not s['subject_id']:
                        continue
                    subject_set.add(s['subject_id'])

            logger.info(f"Total number of subjects: {len(subject_set)}")

            uin = input("WARNING: Do you want to continue? (y or n): ")
            if uin != 'y':
                logger.info("Abort upon user request")
                exit(0)

            for idx, sbj in list(enumerate(sorted(list(subject_set), reverse=opt_reverse))):
                if idx == opt_count:
                    exit(0)
                if not sbj:
                    continue
                logger.info(f"Building index: {idx}, {sbj}")
                self.key = sbj
                if opt_dry:
                    logger.info(f"DRY RUN: {self.key}")
                else:
                    self.build_index_by_subject()
