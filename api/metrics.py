import datetime, json

from metrics_util import *

class Metrics(object):
    """ The class for adding and updating metrics in the system. """
    def __init__(self, request, database_connection):
        super(Metrics, self).__init__()
        self.db = database_connection
        self.request = request

    def addOrTouchEvent(self, func):
        request = self.request
        db = self.db
        error = None

        if func.upper() in ['ADD', 'TOUCH']:
            current_event = db.Event()
            current_event["realm"] = unicode(request.args.get('realm'))
            current_event["description"] = unicode(request.args.get('description'))
            current_event["name"] = unicode(request.args.get('name'))
            current_event["source"] = unicode(request.args.get('source'))
            current_event["useragent"] = unicode(request.args.get('useragent'))
            current_event["dt"] = datetime.datetime.utcnow()

            # Need to test for whether or not the data is a string, a float, or an int
            current_event["datum"] = castDatum(request.args.get('datum'))

        if func.upper() == "ADD":

            # Save the object to the database...
            current_event.save()

            responseDict = {
                'Response': 'Success'
            }

            return json.dumps(responseDict, default=jsonDefaultHandler)

        elif func.upper() in ["TOUCH", "INCREMENT", "TOUCHANDINCREMENT"]:
            # Touch changes the last modified date for a record in the database.
            # If the record doesn't exist, it will insert the record.

            responseDict = {}
            responseDict['Response'] = {}

            if func.upper() in ["TOUCH", "TOUCHANDINCREMENT"]:

                query_response = list(db.Event.find(
                    spec = {
                        "realm": current_event["realm"],
                        "description": current_event["description"],
                        "name": current_event["name"],
                        "datum": current_event["datum"],
                        "source": current_event["source"],
                        "useragent": current_event["useragent"],
                        "$or": [
                            {"dtm": {"$gte": datetime.datetime.utcnow() - datetime.timedelta(minutes = 5)}},
                            {"$and": [
                                {"dtm": None},
                                {"dt": {"$gte": datetime.datetime.utcnow() - datetime.timedelta(minutes = 5)}}
                            ]}
                        ]
                    },
                    sort = [("_id", -1)]
                ))

                # If the query_response is false, then we need to insert this.
                save_response = None

                if len(query_response) == 0:
                    save_response = current_event.save()
                else:
                    update_event = db.Event()
                    update_event = query_response[0]
                    update_event["dtm"] = datetime.datetime.utcnow()
                    update_event.save()

                responseDict['Response']['f_and_m'] = query_response
                responseDict['Response']['save'] = save_response

            if func.upper() in ["INCREMENT", "TOUCHANDINCREMENT"]:

                # Set up the event
                event = db.Count()
                event['name'] = unicode(request.args.get('name'))
                
                # Get the incremement number
                incNum = int(request.args.get('total')) if request.args.get('total') else 1

                responseDict['Response']['increment'] = event.incrementBy(incNum)

            return json.dumps(responseDict, default=jsonDefaultHandler)


        