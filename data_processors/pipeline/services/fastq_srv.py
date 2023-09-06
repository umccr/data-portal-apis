import logging
from typing import List

from django.db import transaction
from django.db.models import QuerySet

from data_portal.models.sequencerun import SequenceRun
from data_portal.models.fastqlistrow import FastqListRow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@transaction.atomic
def create_or_update_fastq_list_row(fastq_list_row: dict, sequence_run: SequenceRun):
    """
    A fastq list row is a dict that contains the following struct:
    {
        'rgid': "GTGTCGGA.GCTTGCGC.1.210108_A01052_0030_ABCDEFGHIJ.MDX200237_L2100008",
        'rgsm': "MDX200237",
        'rglb': "L2100008",
        'lane': 1,
        'read_1': "gds://fastq-vol/210108_A01052_0030_ABCDEFGHIJ/Y151_I8_I8_Y151/PO/MDX200237_L2100008_S2_L001_R1_001.fastq.gz",
        'read_2': "gds://fastq-vol/210108_A01052_0030_ABCDEFGHIJ/Y151_I8_I8_Y151/PO/MDX200237_L2100008_S2_L001_R2_001.fastq.gz",
    }

    :param fastq_list_row:
    :param sequence_run:
    :return:
    """
    rgid: str = fastq_list_row['rgid']
    rgsm: str = fastq_list_row['rgsm']
    rglb: str = fastq_list_row['rglb']
    lane: int = fastq_list_row['lane']
    read_1: str = fastq_list_row['read_1']
    read_2: str = fastq_list_row['read_2']

    qs = FastqListRow.objects.filter(rgid__iexact=rgid)

    if not qs.exists():
        # create new row
        logger.info(f"Creating new FastqListRow (rgid={rgid})")
        flr = FastqListRow()
        flr.rgid = rgid
        flr.rgsm = rgsm
        flr.rglb = rglb
        flr.lane = lane
        flr.read_1 = read_1
        flr.read_2 = read_2
        flr.sequence_run = sequence_run
    else:
        # update to updatable attributes i.e.
        # Depends on business logic requirement, we may decide a particular attribute is "immutable" across different
        # runs. For now, we update everything if the same rgid is matched / already existed in db.
        logger.info(f"Updating existing FastqListRow (rgid={rgid})")
        flr = qs.get()
        flr.rgsm = rgsm
        flr.rglb = rglb
        flr.lane = lane
        flr.read_1 = read_1
        flr.read_2 = read_2
        flr.sequence_run = sequence_run
    flr.save()


@transaction.atomic
def get_fastq_list_row_by_sequence_run(sqr: SequenceRun):
    """
    Business logic:
    Find FastqListRow records by linked SequenceRun instance
    Note, it is safer to perform lookup filter by specific SequenceRun instance
    as there can be multiple SequenceRun entries of the same instrument_run_id or sequence name (i.e. event replay)

    This is particularly important in a case re-run with "subset" of BCL Convert output only, such as follows.
    https://umccr.slack.com/archives/C8CG6K76W/p1690351136620419
    """
    qs = FastqListRow.objects.filter(sequence_run=sqr)
    if qs.exists():
        fqlr_list = []
        for fqlr in qs.all():
            fqlr_list.append(fqlr.as_dict())
        return fqlr_list
    return None


@transaction.atomic
def get_fastq_list_row_by_rgid(rgid):
    try:
        fastq_list_row = FastqListRow.objects.get(rgid=rgid)
    except FastqListRow.DoesNotExist:
        return None

    return fastq_list_row


@transaction.atomic
def get_fastq_list_row_by_rglb(rglb) -> List[FastqListRow]:
    fqlr_list = list()
    qs: QuerySet = FastqListRow.objects.filter(rglb__exact=rglb)
    if qs.exists():
        for fqlr in qs.all():
            fqlr_list.append(fqlr)
    return fqlr_list


@transaction.atomic
def get_fastq_list_row_by_rglb_and_sequence_run(rglb, sqr: SequenceRun) -> List[FastqListRow]:
    fqlr_list = list()
    qs: QuerySet = FastqListRow.objects.filter(rglb__exact=rglb, sequence_run=sqr)
    if qs.exists():
        for fqlr in qs.all():
            fqlr_list.append(fqlr)
    return fqlr_list


def extract_sample_library_ids(fastq_list_rows: List[FastqListRow]):
    samples = set()
    libraries = set()

    for row in fastq_list_rows:
        libraries.add(row.rglb)
        samples.add(row.rgsm)

    return list(samples), list(libraries)
