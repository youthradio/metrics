import json
import re

import pytz, time, math
from dateutil.relativedelta import relativedelta

from metrics_util import *

COMMERCIALS = ["AllDayPlay.fm - AllDayPlay.fm",
               "AllDayPlay.FM : AllDayPLay.FM - AllDayPlay.FM : AllDayPLay.FM"]

DESCRIPTIONS = ["128K Shoutcast Server", "56K Shoutcast Server"]

class AllDayPlay(object):
    """AllDayPlay metrics."""
    def __init__(self, request, database_connection):
        super(AllDayPlay, self).__init__()
        self.request = request
        self.db = database_connection

    def lastSongsPlayed(self):
        request = self.request
        db = self.db

        error = None

        # Do just the simple find()s in the database that return a single
        # integer as a response.
        result = {}

        constraint = {"realm": "ADP",
                      "name": "Current Song",
                      "datum": {"$nin": COMMERCIALS}}

        lim = 10 if request.args.get('limit') == None else int(request.args.get('limit'))

        result = []

        for song in db.Event.find(constraint,
                                  {"_id": False,
                                   "datum": True},
                                  sort = [("_id", -1)]).limit(lim):
            result.append(song["datum"])

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def totalSongsPlayed(self):
        """ Get the total number of songs played. """
        db = self.db

        error = None
        result = {}

        constraint = {"realm": "ADP",
                      "name": "Current Song",
                      "datum": {"$nin": COMMERCIALS}}

        result = db.Event.find(constraint).count()

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def currentNumberOfListeningSessions(self):
        """ Get the current number of listening sessions. """
        db = self.db
        result = {}
        total = 0

        for desc in DESCRIPTIONS:
            result[desc] = db.Event.find_one({"realm": "ADP",
                                              "description": desc,
                                              "name": "Current Listeners"},
                                             sort = [("_id", -1)])["datum"]
            total += result[desc]

        result["Date"] = db.Event.find_one({"realm": "ADP",
                                            "name": "Current Listeners"},
                                           sort = [("_id", -1)])["dt"]
        result["Total"] = total

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def totalSessionsBounced(self):
        """ Get the total number of listening sessions bounced. """
        db = self.db
        result = {}
        total = 0

        for desc in DESCRIPTIONS:
            result[desc] = db.Event.find({"realm": "ADP",
                                          "description": desc,
                                          "name": "Listener",
                                          "dtm": None}).count()
            total += result[desc]

        result["Total"] = total

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def totalSessions(self):
        """ Get the total number of listening sessions. """
        db = self.db
        result = {}
        total = 0

        for desc in DESCRIPTIONS:
            result[desc] = db.Event.find({"realm": "ADP",
                                          "description": desc,
                                          "name": "Listener"}).count()
            total += result[desc]

        result["Total"] = total

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def totalListenerHours(self):
        db = self.db
        result = {}
        temp = {}
        total = 0.0

        for desc in DESCRIPTIONS:
            temp = db["yr-metrics"].Events.aggregate([
                { "$match" : { "realm" : u"ADP", "description" : desc, "name" : u"Listener", "dtm" : { "$ne" : None } } },
                { "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
                { "$group": { "_id" : None, "totalListeningTimeInSeconds" : { "$sum" : "$listeningTimeInSeconds" } } }
            ])

            result[desc] = temp["result"][0]["totalListeningTimeInSeconds"] / 60 / 60 if temp["ok"] == 1 else "Error"
            total += result[desc] if result[desc] != "Error" else 0

        result["Total"] = total

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def totalListeners(self):
        db = self.db
        result = {}
        temp = {}
        total = 0

        for desc in DESCRIPTIONS:
            result[desc] = len(db.Event.find({"realm": "ADP",
                                              "description": desc,
                                              "name": "Listener",
                                              "dtm": { "$ne": None } }).distinct("datum"))
            total += result[desc]

        result["Total"] = total

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def averageListeningTime(self):

        db = self.db
        result = {}
        temp = {}
        total = 0.0

        # Get the individual average listening time...
        for desc in DESCRIPTIONS:
            temp = db["yr-metrics"].Events.aggregate([
                { "$match" : { "realm" : u"ADP", "description" : desc, "name" : u"Listener", "dtm" : { "$ne" : None } } },
                { "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
                { "$group": { "_id" : None, "avgListeningTimeInSeconds" : { "$avg" : "$listeningTimeInSeconds" } } }
            ])

            result[desc] = temp["result"][0]["avgListeningTimeInSeconds"] / 60 if temp["ok"] == 1 else "Error"

        # Get the overall average listening time...
        temp = db["yr-metrics"].Events.aggregate([
            { "$match" : { "realm" : u"ADP", "description" : { "$in" : DESCRIPTIONS }, "name" : u"Listener", "dtm" : { "$ne" : None } } },
            { "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
            { "$group": { "_id" : None, "avgListeningTimeInSeconds" : { "$avg" : "$listeningTimeInSeconds" } } }
        ])

        result["Overall"] = temp["result"][0]["avgListeningTimeInSeconds"] / 60 if temp["ok"] == 1 else "Error"

        return json.dumps({"Result": result}, default=jsonDefaultHandler)

    def lastX(self, modifier):

        db = self.db
        request = self.request
        result = {}
        temp = {}
        total = 0
        realm = "ADP"

        """ Return information from a previous time to the present.

        This function will perform basic analysis on the information
        returned. It's to be used in the following format:

        http://<server url>/ADP/SESSIONS/LAST4HOURS/
        http://<server url>/ADP/SESSIONS/LAST30MINS/
        http://<server url>/ADP/SESSIONS/LAST7DAYS/

        The information returned will be in the following format:

        {
          "Result" : {
            "date_list" : [
              "2013-04-18T20:00:00.284568",
              "2013-04-18T21:00:00.284568",
              "2013-04-18T22:00:00.284568",
              "2013-04-18T23:00:00.284568"
            ],
            "128K Shoutcast Server" : [
              33,
              10,
              21,
              8
            ],
            "56K Shoutcast Server" : [
              37,
              8,
              11,
              6
            ]
          }
        }
        """
        result["time"] = {}

        # If there's a timezone out there, set it appropriately...
        tz = pytz.timezone('US/Pacific') if not request.args.get('tz') else pytz.timezone(request.args.get('tz'))

        # Set the current time...
        current_time = datetime.datetime.utcnow().replace(microsecond=0)
        time_measure = None

        # Perform a regular expression on the URL...
        start_time = time.time()
        try:
            time_re = re.search("(\d+)(MINS|HOURS|DAYS|WEEKS|MONTHS|YEARS)", modifier.upper())
            time_delta = int(time_re.group(1))
            time_measure = time_re.group(2)

        except AttributeError:
            time_delta = 30
            time_measure = "MINS"

        # Let's put together the time delta...
        if time_measure == "MINS":
            delta = datetime.timedelta(minutes=time_delta)
            date_list = [ current_time -
                            datetime.timedelta(minutes=x) for x in range(0, time_delta) ]
        elif time_measure == "HOURS":
            delta = datetime.timedelta(hours=time_delta)
            date_list = [ current_time -
                            datetime.timedelta(hours=x,
                                               minutes=current_time.minute,
                                               seconds=current_time.second) for x in range(0, time_delta) ]
        elif time_measure == "DAYS":
            delta = datetime.timedelta(days=time_delta)

            # Set up the range for the search depending on time zone differentials.
            # Need to compare the seconds in the timedeltas to make sure that the current
            # time is within the offset.
            if datetime.timedelta(hours=current_time.hour).seconds < math.fabs((tz.utcoffset(current_time).days * 24 * 60 * 60) + tz.utcoffset(current_time).seconds):
                date_range = range(1, time_delta + 1)
            else:
                date_range = range(0, time_delta)

            date_list = [ current_time - tz.utcoffset(current_time) -
                            datetime.timedelta(days=x,
                                               hours=current_time.hour,
                                               minutes=current_time.minute,
                                               seconds=current_time.second) for x in date_range ]
        elif time_measure == "WEEKS":
            delta = datetime.timedelta(weeks=time_delta)
            date_list = [ current_time + tz.utcoffset(current_time) -
                            datetime.timedelta(weeks=x,
                                               hours=current_time.hour,
                                               minutes=current_time.minute,
                                               seconds=current_time.second) for x in range(0, time_delta) ]

        end_time = time.time()
        result["time"]["regex and range"] = end_time - start_time

        # Set the list of datetimes...
        start_time = time.time()
        date_list.reverse()
        result["date_list"] = list(date_list)
        temp = {}
        end_time = time.time()
        result["time"]["date reversal"] = end_time - start_time

        # Run the database query...
        for desc in DESCRIPTIONS:
            start_time = time.time()
            if time_measure == "MINS":
                result[desc] = toList(db.Event.find({"realm": realm,
                                                     "description": desc,
                                                     "name": "Current Listeners",
                                                     "dt": {"$gte": (current_time - delta), "$lte": current_time} },
                                                    {"_id": False,
                                                     "datum": True},
                                                    sort = [("_id", 1)]),
                                      "datum")
                end_time = time.time()
                result["time"][time_measure.lower() + " query - " + desc] = end_time - start_time
            else:

                listeners = db.Event.find({ "realm": realm,
                                            "description": desc,
                                            "name": "Listener",
                                            "dtm": { "$gte": (current_time - delta) } },
                                          { "_id": False,
                                            "dt": True,
                                            "dtm": True },
                                          sort = [("_id", 1)])
                end_time = time.time()
                result["time"][time_measure.lower() + " query - " + desc] = end_time - start_time

                # Go through the query results and compile the results...
                temp = {}
                start_time = time.time()
                for key in date_list:

                    if time_measure == "HOURS":
                        dates = (key, key + relativedelta(hours=+1))
                    elif time_measure == "DAYS":
                        dates = (key, key + relativedelta(days=+1))
                    elif time_measure == "WEEKS":
                        dates = (key, key + relativedelta(months=+1))
                    elif time_measure == "MONTHS":
                        dates = (key, key + relativedelta(days=+1))
                    elif time_measure == "YEARS":
                        dates = (key, key + relativedelta(years=+1))

                    start_time_a = time.time()
                    temp[dates[0]] = db.Event.find(
                                        { "realm": realm,
                                          "description": desc,
                                          "name": "Listener",
                                          "dtm": { "$ne": None },
                                          "$or": [
                                            { "$and": [ { "dt": { "$lte": dates[0] } }, { "dtm": { "$gt": dates[0] } } ] },
                                            { "$and": [ { "dt": { "$gte": dates[0] } }, { "dt": { "$lt": dates[1] } } ] },
                                            { "$and": [ { "dtm": { "$gte": dates[0] } }, { "dtm": { "$lt": dates[1] } } ] }
                                          ] }
                                     ).count()

                    end_time_a = time.time()
                    result["time"]["listeners iter - " + desc] = end_time_a - start_time_a

                end_time = time.time()
                result["time"]["compile results (outer) - " + desc] = end_time - start_time

                # Sort the resulting list and return it.
                start_time = time.time()
                l = list()
                for a in date_list:
                    l.append(temp[a])

                result[desc] = l
                end_time = time.time()
                result["time"]["organize results list - " + desc] = end_time - start_time

        return json.dumps({"Result": result}, default=jsonDefaultHandler)


