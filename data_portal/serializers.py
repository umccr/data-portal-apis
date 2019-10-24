from typing import Dict, List

from rest_framework import serializers

from data_portal.models import S3Object, LIMSRow


class LIMSRowSerializer(serializers.Serializer):
    illumina_id = serializers.CharField()
    run = serializers.IntegerField()
    subject_id = serializers.CharField()
    external_subject_id = serializers.CharField()
    sample_id = serializers.CharField()

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
    lims_rows = serializers.SerializerMethodField()

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

    def get_lims_rows(self, obj: S3Object) -> List[Dict]:
        """
        Get associated LIMS rows
        :return: list of LIMS data
        """
        lims_rows = LIMSRow.objects.filter(s3lims__s3_object=obj)

        data = []
        for lims_row in lims_rows:
            serializer = LIMSRowSerializer(instance=lims_row)
            data.append(serializer.data)

        return data

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
