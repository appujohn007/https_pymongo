"""Minimal CodecOptions value object for libraries that import PyMongo types."""


class CodecOptions:
    def __init__(self, document_class=dict, tz_aware=False, uuid_representation=0,
                 unicode_decode_error_handler="strict", tzinfo=None,
                 type_registry=None, datetime_conversion=None):
        self.document_class = document_class
        self.tz_aware = tz_aware
        self.uuid_representation = uuid_representation
        self.unicode_decode_error_handler = unicode_decode_error_handler
        self.tzinfo = tzinfo
        self.type_registry = type_registry
        self.datetime_conversion = datetime_conversion

