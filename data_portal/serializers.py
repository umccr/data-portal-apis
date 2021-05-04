from typing import Dict, List

from rest_framework import serializers
from rest_framework.fields import empty

from data_portal.models import S3Object, LIMSRow, GDSFile, LabMetadata

READ_ONLY_SERIALIZER = 'READ ONLY SERIALIZER'


class LIMSRowSerializer(serializers.Serializer):
    illumina_id = serializers.CharField()
    run = serializers.IntegerField()
    subject_ids = serializers.SerializerMethodField()
    sample_id = serializers.CharField()

    def get_subject_ids(self, obj: LIMSRow) -> str:
        return f'{obj.subject_id}/{obj.external_subject_id}'

    def update(self, instance, validated_data):
        pass

    def create(self, validated_data):
        pass


class S3ObjectSerializer(serializers.Serializer):
    # Legacy field
    rn = serializers.SerializerMethodField()
    bucket = serializers.CharField()
    key = serializers.CharField()
    path = serializers.SerializerMethodField()
    size = serializers.IntegerField()
    last_modified_date = serializers.DateTimeField()
    illumina_id = serializers.SerializerMethodField()
    run = serializers.SerializerMethodField()
    subject_id = serializers.SerializerMethodField()
    sample_id = serializers.SerializerMethodField()

    def __init__(self, instance=None, data=empty, **kwargs):
        super().__init__(instance, data, **kwargs)
        # Current LIMS data
        self.lims = None
        # Current S3 object
        self.curr_obj = None

    def get_rn(self, obj: S3Object) -> int:
        """
        Use id as row number
        """
        return obj.id

    def get_path(self, obj: S3Object) -> str:
        """
        Get formatted S3 object path
        """
        return 's3://%s/%s' % (obj.bucket, obj.key)

    def get_lims(self, obj: S3Object) -> None:
        """
        Get and set associated LIMS data, so it can be used for other lims field getters
        """
        lims_rows = LIMSRow.objects.filter(s3lims__s3_object=obj)

        # In case we don't have associated lims record
        # Use dict for each value so that we don't get duplicate values
        field_value_dict = {'illumina_id': {}, 'run': {}, 'subject_ids': {}, 'sample_id': {}}
        for lims_row in lims_rows:
            serializer = LIMSRowSerializer(instance=lims_row)
            lims_row_data = serializer.data

            for field, value in lims_row_data.items():
                field_value_dict[field][str(value)] = str(value)

        # Concatenate field value lists
        concatenated_data = {}
        for field, value_dict in field_value_dict.items():
            concatenated_data[field] = ',\n'.join(value_dict.keys())

        # Keep a record of the current object so we know whether lims needs to be reloaded
        self.lims = concatenated_data
        self.curr_obj = obj

    def get_illumina_id(self, obj: S3Object):
        """
        Get concatenated illumina ids
        """
        if self.lims is None or self.curr_obj != obj:
            self.get_lims(obj)

        return self.lims['illumina_id']

    def get_run(self, obj: S3Object):
        """
        Get concatenated run numbers
        """
        if self.lims is None or self.curr_obj != obj:
            self.get_lims(obj)

        return self.lims['run']

    def get_subject_id(self, obj: S3Object):
        """
        Get concatenated subject ids
        """
        if self.lims is None or self.curr_obj != obj:
            self.get_lims(obj)

        return self.lims['subject_ids']

    def get_sample_id(self, obj: S3Object):
        """
        Get concatenated sample ids
        """
        if self.lims is None or self.curr_obj != obj:
            self.get_lims(obj)

        return self.lims['sample_id']

    def to_representation(self, instance: S3Object) -> list:
        """
        Override the default method so that we have
        Object instance -> list of values in field order
        """
        ordered_dict = super().to_representation(instance)

        list_data = []

        for key, value in ordered_dict.items():
            list_data.append(value)

        return list_data

    def get_fields_with_sortable(self) -> List[Dict]:
        """
        Get list of fields in dict where we also have `sortable` information. This is the legacy format.
        :return:
        """
        fields = self.fields
        field_list = []

        for key in fields.keys():
            field_list.append({
                'key': key,
                'sortable': key in S3Object.SORTABLE_COLUMNS
            })

        return field_list

    def update(self, instance, validated_data):
        pass

    def create(self, validated_data):
        pass


class LIMSRowModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = LIMSRow
        fields = '__all__'

class LabMetadataModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = LabMetadata
        fields = '__all__'


class S3ObjectModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = S3Object
        fields = '__all__'


class SubjectIdSerializer(serializers.BaseSerializer):
    def to_representation(self, instance):
        return instance.subject_id

    def to_internal_value(self, data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def update(self, instance, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def create(self, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)


class RunIdSerializer(serializers.BaseSerializer):
    def to_representation(self, instance):
        return instance.illumina_id

    def to_internal_value(self, data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def update(self, instance, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def create(self, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

class ReportIdSerializer(serializers.BaseSerializer):
    def to_representation(self, instance):
        return instance.sample_id

    def to_internal_value(self, data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def update(self, instance, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def create(self, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

class BucketIdSerializer(serializers.BaseSerializer):
    def to_representation(self, instance):
        return instance.bucket

    def to_internal_value(self, data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def update(self, instance, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)

    def create(self, validated_data):
        raise NotImplementedError(READ_ONLY_SERIALIZER)


class GDSFileModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = GDSFile
        fields = '__all__'
