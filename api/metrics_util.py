""" Utility functions for the metrics library. """

import datetime

def jsonDefaultHandler(obj):
    # TODO: Add a handler for bson.objectid.ObjectId
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, datetime.datetime):
        return datetime.datetime
    else:
        return str(obj)

def castDatum(datum):
    if "." in datum:
        # This could be a float or a string...
        try:
            ret_datum = float(datum)
        except ValueError, e:
            ret_datum = unicode(datum)
    else:
        # This could be an int or a string...
        try:
            ret_datum = int(datum)
        except ValueError, e:
            ret_datum = unicode(datum)

    return ret_datum

def toList(cursor, key = None):
    returnList = []
    for item in cursor:
        if key == None:
            returnList.append(item)
        else:
            returnList.append(item[key])
    return returnList
