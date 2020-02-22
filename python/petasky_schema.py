# -*- coding: utf-8 -*-

import re
import pyspark

att_line = re.compile(r"^\s*(\S*)\s.*$")


def loadSchema(resource):
    with open(pyspark.SparkFiles.get(resource), "r") as reader:
        reader.next()
        skipEnd = filter(lambda l: l.strip() != ");", reader)
        atts = map(lambda l: re.sub(att_line, r"\1", l).strip(), skipEnd)
        result = list(atts)
        return atts

_sourceS = None
def sourceS():
    global _sourceS
    if not _sourceS:
        _sourceS = loadSchema("Source.sql")
    return _sourceS

_objectS = None
def objectS():
    global _objectS
    if not _objectS:
        _objectS = loadSchema("Object.sql")
    return _objectS

_s_objectId = None
def s_objectId():
    global _s_objectId
    if not _s_objectId:
        _s_objectId = sourceS().index("objectId")
    return _s_objectId

_s_sourceId = None
def s_sourceId():
    global _s_sourceId
    if not _s_sourceId:
        _s_sourceId = sourceS().index("sourceId")
    return _s_sourceId

_s_ra = None
def s_ra():
    global _s_ra
    if not _s_ra:
        _s_ra = sourceS().index("ra")
    return _s_ra

_s_decl = None
def s_decl():
    global _s_decl
    if not _s_decl:
        _s_decl = sourceS().index("decl")
    return _s_decl

