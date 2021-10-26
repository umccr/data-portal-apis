from django.db.models import QuerySet, Q, Value


def filter_object_by_field_keyword(qs, field_list, keywords) -> QuerySet:
    # Query all available queryset
    # qs: QuerySet = self.objects.all()

    # Grab keyword if specified
    # keywords = kwargs.get('keywords', None)
    if bool(keywords):
        # Grab field name to iterate in a list
        # field_list = [field.name for field in self._meta.get_fields()]

        # Create a query string
        query_string = None
        for key, value in keywords.items():

            # Only search if key is a match with column names
            if key not in field_list:
                continue

            each_query = Q(**{"%s__iexact" % key: value})
            if query_string:
                query_string = query_string & each_query  # or & for filtering
            else:
                query_string = each_query

        if query_string is not None:
            qs = qs.filter(query_string)

    return qs
