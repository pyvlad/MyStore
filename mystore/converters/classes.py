"""
This module contains different implementations of 'converters'.
"""
import logging
lg = logging.getLogger(__name__)

from .base import BaseValueConverter
from . import handlers


class CompressedJsonConverter(BaseValueConverter):
    """ Convert Python objects to/from compressed JSON objects. """
    DUMP_HANDLERS = [handlers.object_to_compressed_json_binary]
    LOAD_HANDLERS = [handlers.object_from_compressed_json_binary]


class Base64CompressedJsonConverter(BaseValueConverter):
    """ Convert Python objects to/from base64 encoded
        text strings of compressed JSON objects. """
    DUMP_HANDLERS = [handlers.object_to_compressed_json_binary, handlers.bytes_to_base64_string]
    LOAD_HANDLERS = [handlers.bytes_from_base64_string, handlers.object_from_compressed_json_binary]
