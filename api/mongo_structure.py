from mongokit import Connection, Document, IS, OR, MultipleResultsFound, ObjectId, Collection

import datetime

class RootDocument(Document):
    """Foundation class for MongoKit usage."""
    use_dot_notation = True
    use_autorefs = True
    skip_validation = False
    structure = {}
    __database__ = "yr-metrics"

class Event(RootDocument):
    __collection__ = "Events"
    structure = {
        "realm": unicode,                   # Which property is this coming from (ie - ADP, Turnstyle, etc)
        "description": unicode,             # Description of event or origin (ie - Shoutcast Server)
        "name": unicode,                    # Name of data point (ie - SONGTITLE)
        "datum": OR(unicode, int, float),   # The actual data being recorded
        "source": unicode,                  # IP Address or other source for event
        "useragent": unicode,               # User Agent info on source
        "dt": datetime.datetime,            # The datetime the event was recorded
        "dtm": datetime.datetime            # The datetime the event recording ended
    }
    required_fields = ["realm", "description", "datum"]

    def __repr__(self):
        return "<Event %r>" % (self.name)

class Count(RootDocument):
  __collection__ = "Analysis"
  use_schemaless = True

  structure = {
    "name": unicode,            # Name of the field to keep a count of; must be unique
    "total": OR(int, float)     # The total count
  }
  required_fields = [ "name" ]

  def incrementBy(self, num):
    """ Increment the count by num. """
    response = self.connection["yr-metrics"].Analysis.update({"name": self.name}, {"$inc": { "total": num }}, upsert=True)
    
    if response:
        return response
    else:
        return True

  def __repr__(self):
    """ Return a string representation of this object. """
    return "<Analysis for %r> %d" % (self.name, self.total)

