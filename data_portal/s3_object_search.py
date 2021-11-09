"""
This file contains search query support for search views based on S3Object table.
This uses and extends the base search query functionality provided in `search_query`.
"""
from typing import Dict, List
from django.db.models import QuerySet

from data_portal.search_query import FilterType, FilterTag, FilterTypeFactory, FilterMethod, SearchQueryHelper, \
    Filter, FilterMethodFactory, FilterTagFactory
from utils import libdt


class S3FilterTag(FilterTag):
    KEY = 'KEY'
    SIZE = 'SIZE'
    LAST_MODIFIED_DATE = 'LAST_MODIFIED_DATE'
    SUBJECT_ID = 'SUBJECT_ID'
    SAMPLE_ID = 'SAMPLE_ID'
    LINKED = 'LINKED'
    SOURCE = 'SOURCE'
    TYPE = 'TYPE'


class S3FilterMethod(FilterMethod):
    LINKED_WITH_LIMS = 'LINKED_WITH_LIMS'


class S3FilterMethodFactory(FilterMethodFactory):
    def __init__(self) -> None:
        super().__init__()

        # Register filter methods for S3 object search, in additional to base methods
        self.register(FilterMethod(S3FilterMethod.LINKED_WITH_LIMS, True))


class S3FilterTagFactory(FilterTagFactory):
    def __init__(self) -> None:
        super().__init__()

        # Register filter tags for S3 object search, in additional to base tags
        self.register(FilterTag(S3FilterTag.KEY, str, ('key',)))
        self.register(FilterTag(S3FilterTag.SIZE, int, ('size',)))
        self.register(FilterTag(S3FilterTag.LAST_MODIFIED_DATE, libdt.parse_last_modified_date, ('last_modified_date',)))
        self.register(FilterTag(
            S3FilterTag.SUBJECT_ID,
            str,
            ('s3lims__lims_row__subject_id', 's3lims__lims_row__external_subject_id')
        ))
        self.register(FilterTag(S3FilterTag.SAMPLE_ID, str, ('s3lims__lims_row__sample_id',)))
        self.register(FilterTag(S3FilterTag.LINKED, lambda c: c.lower() == 'true', None))


# Instantiate factories
filter_method_factory = S3FilterMethodFactory()
filter_tag_factory = S3FilterTagFactory()


class S3FilterType(FilterType):
    KEY_INCLUDES = 'pathinc'
    KEY_EXTENSION = 'ext'
    FILE_SIZE = 'size'
    LAST_MODIFIED_DATE = 'date'
    SUBJECT_ID = 'subjectid'
    SAMPLE_ID = 'sampleid'
    SOURCE = 'source'
    TYPE = 'type'
    LINKED_WITH_LIMS = 'linked'


class S3FilterTypeFactory(FilterTypeFactory):
    def __init__(self) -> None:
        # Instantiate filter type factory using the factory instances above
        super().__init__(filter_tag_factory, filter_method_factory)

        # Self explanatory - see description parameter below
        self.register(FilterType(
            S3FilterType.KEY_INCLUDES,
            self.filter_tag_factory.get(S3FilterTag.KEY),
            self.filter_method_factory.get(S3FilterMethod.CONTAINS),
            'File path includes'
        ))
        self.register(FilterType(
            S3FilterType.KEY_EXTENSION,
            self.filter_tag_factory.get(S3FilterTag.KEY),
            self.filter_method_factory.get(S3FilterMethod.ENDS_WITH),
            'File extension'
        ))
        self.register(FilterType(
            S3FilterType.FILE_SIZE,
            self.filter_tag_factory.get(S3FilterTag.SIZE),
            self.filter_method_factory.get(S3FilterMethod.COMPARE),
            'Compare with file size'
        ))
        self.register(FilterType(
            S3FilterType.LAST_MODIFIED_DATE,
            self.filter_tag_factory.get(S3FilterTag.LAST_MODIFIED_DATE),
            self.filter_method_factory.get(S3FilterMethod.COMPARE),
            'Compare with last modified date of the file'
        ))
        self.register(FilterType(
            S3FilterType.SUBJECT_ID,
            self.filter_tag_factory.get(S3FilterTag.SUBJECT_ID),
            self.filter_method_factory.get(S3FilterMethod.CONTAINS),
            'SubjectID/ExternalSubjectID (in LIMS table) includes'
        ))
        self.register(FilterType(
            S3FilterType.SAMPLE_ID,
            self.filter_tag_factory.get(S3FilterTag.SAMPLE_ID),
            self.filter_method_factory.get(S3FilterMethod.CONTAINS),
            'SampleID (in LIMS table) includes'
        ))
        self.register(FilterType(
            S3FilterType.SOURCE,
            self.filter_tag_factory.get(S3FilterTag.SOURCE),
            self.filter_method_factory.get(S3FilterMethod.CONTAINS),
            'Source (in LIMS table) includes, i.e: FFPE, tissue, blood...'
        ))
        self.register(FilterType(
            S3FilterType.TYPE,
            self.filter_tag_factory.get(S3FilterTag.TYPE),
            self.filter_method_factory.get(S3FilterMethod.CONTAINS),
            'Type (in LIMS table) includes, i.e: WGS, WTS...'
        ))
        self.register(FilterType(
            S3FilterType.LINKED_WITH_LIMS,
            self.filter_tag_factory.get(S3FilterTag.LINKED),
            self.filter_method_factory.get(S3FilterMethod.LINKED_WITH_LIMS),
            'The record is linked with at least one LIMS row'
        ))
        self.set_default(S3FilterType.KEY_INCLUDES)


# Instantiate the factory
filter_type_factory = S3FilterTypeFactory()


class S3ObjectSearchQueryHelper(SearchQueryHelper):
    def __init__(self) -> None:
        super().__init__(filter_type_factory, filter_tag_factory, filter_method_factory)

    def apply_global_filters(self, filters: Dict[str, List[Filter]], queryset: QuerySet) -> QuerySet:
        """
        Apply global filters specific to S3 object search
        """
        queryset = super().apply_global_filters(filters, queryset)

        linked_with_lims_id = S3FilterType.LINKED_WITH_LIMS
        has_linked_with_lims = linked_with_lims_id in filters

        # Only apply this special filter if we have the filter for linked_with_lims
        if has_linked_with_lims:
            linked_with_lims = filters[linked_with_lims_id][0].val is True

            if linked_with_lims:
                queryset = queryset.filter(s3lims__isnull=False)
            else:
                queryset = queryset.filter(s3lims__isnull=True)

        return queryset
