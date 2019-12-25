#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 15:33:56 2019

@author: clauney
"""

import logging as lumberjack
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
from io import BytesIO
import base64
import re

b64_encodable_fields = [] # NOTE! SET THIS AFTER IMPORT IF YOU WANT TO USE IT. (for iothub it should be ['Body'])

# =============================================================================
# # BLOB CRACKING AND PARSING
# =============================================================================
def crack_blob_json(blob_data_obj, blob_text_encoding):
    '''Returns a list of dicts describing messages in the blob
    blob_data_obj - result of iotdrop_blobsvc.get_blob_to_bytes(blob_drop_container, blob.name)
    blob_text_encoding - the encoding, probably should just make this default to 'UTF-8'
    '''
    blob_cont = blob_data_obj.content
    if type(blob_cont) == bytes:# or blob_data_obj.properties.content_settings.content_type == 'application/octet-stream':
        blob_cont = blob_cont.decode(blob_text_encoding, errors='ignore')
    msglist = blob_cont.split('\n')
    return [json.loads(d) for d in msglist]

def crack_blob_avro(blob_data_obj):
    return DataFileReader(BytesIO(blob_data_obj.content), DatumReader())

# =============================================================================
# # DICTIONARY UTILITIES
# =============================================================================
def flatten_dict(dd, separator='__', prefix='', encoding='utf-8'):
    return { prefix + separator + k if prefix else k : un_weird(v, k, encoding)
             for kk, vv in dd.items()
             for k, v in flatten_dict(un_weird(vv, kk, encoding), separator, kk, encoding).items()
             } if isinstance(dd, dict) else { prefix : dd }

# =============================================================================
# # BINARY UTIL SHIZ
# #  NOTE: I am not at all happy about the thing where we just try to do base64 decoding on stuff.
# #        The only reason this exists is that sometimes a field is b64 encoded because iothub can't 
# #        figure out what to do with it, and there didn't seem to be a good, deterministic way to 
# #        see when that was happening. So I just added it to a config.
# =============================================================================
def un_weird(dd_value, dd_key, encoding='utf-8'):
    dd_value = unbyte_me(dd_value, encoding)
    if type(dd_value) == str:
        if dd_key in b64_encodable_fields and is_possibly_b64(dd_value):
            lumberjack.debug('will try to b64 decode')
            try:
                dd_value = unbyte_me(base64.b64decode(dd_value), encoding)
                lumberjack.debug('worked, dd_value: %s', dd_value)
            except:
                dd_value = unbyte_me(dd_value, encoding)
                lumberjack.debug('did not work, dd_value:', dd_value)
        else:
            lumberjack.debug('did not try, ddkey: %s, fields: %s, dd_value: %s', dd_key, b64_encodable_fields, dd_value)
        if dd_value.startswith('{') and dd_value.endswith('}'):
            dd_value = unbyte_me(json.loads(dd_value), encoding)
    return dd_value

def is_possibly_b64(test_str):
    b_64_ok_regex = '^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$'
    return (
            len(test_str) % 4 == 0 
            and re.fullmatch(b_64_ok_regex, test_str) != None
            )

def unbyte_me(val, encoding='utf-8'):
    '''
    NOTE! Throws an exception if bytes can't be decoded. Try/catch this and you can see if 
    your conversion is weird, like if something looks like base64 decodable but decodes to
    something crazypants
    
    '''
    if not type(val) == bytes:
        return val
    else:
        return val.decode(encoding)

#%%