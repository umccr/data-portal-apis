from django.db.models import QuerySet, Q


def filter_object_by_parameter_keyword(qs, keyword_object, field_names=[]) -> QuerySet:
    """
    This function will filter queryset given based on object given. This will 
    loop the object (skipping on what is defined on skip_list) and match to the 
    queryset for key and value.

    :param
        qs: Given queryset to be filter
        keyword_object: An object that will filter queryset based on their key and value
        field_names: A list of key contain a word to search in the object
    :return
        qs: Filtered queryset
    """

    # Create a query string
    query_string = None
    for key, value in keyword_object.items():

        # Only check if key is in keywords
        if key not in field_names:
            continue

        each_query = Q(**{"%s__iexact" % key: value})
        if query_string:
            query_string = query_string & each_query  # or & for filtering
        else:
            query_string = each_query

    if query_string is not None:
        qs = qs.filter(query_string)

    return qs
