from flask import *
from mongokit import Connection, Document, IS, OR
import datetime
import re
import json


# Create the application object
app = Flask(__name__)
app.config.from_pyfile('yr_metrics_api.cfg', silent=False)

# Connect to the database
db = Connection(app.config["MONGODB_HOST"], app.config["MONGODB_PORT"])

# Needed functions
def jsonDefaultHandler(obj):
    # TODO: Add a handler for bson.objectid.ObjectId
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, datetime.datetime):
        return datetime.datetime
    else:
        return str(obj)
        #raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

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


# Mongo Schema
@db.register
class RootDocument(Document):
	"""Foundation class for MongoKit usage."""
	use_dot_notation = True 
	use_autorefs = True
	skip_validation = False
	structure = {}
	__database__ = "yr-metrics"


@db.register
class Event(RootDocument):
	__collection__ = "Events"
	structure = {
		"realm": unicode,					# Which property is this coming from (ie - ADP, Turnstyle, etc)
		"description": unicode,				# Description of event or origin (ie - Shoutcast Server)
		"name": unicode,					# Name of data point (ie - SONGTITLE)
		"datum": OR(unicode, int, float),	# The actual data being recorded
		"source": unicode,					# IP Address or other source for event
		"useragent": unicode,				# User Agent info on source
		"dt": datetime.datetime,			# The datetime the event was recorded
		"dtm": datetime.datetime			# The datetime the event recording ended 
	}
	required_fields = ["realm", "description", "datum"]

	def __repr__(self):
		return "<Event %r>" % (self.name)


# http://127.0.0.1:5000/event/add?realm=ADP&description=Shoutcast%20Server&name=CURRENTLISTENERS&datum=17
@app.route("/event/<func>", methods=["GET"])
def yr_metrics_add_or_touch_event(func):
	error = None
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

	elif func.upper() == "TOUCH":

		# Touch changes the last modified date for a record in the database.
		# If the record doesn't exist, it will insert the record.
		query_response = list(db.Event.find(
			spec = {
				"name": current_event["name"],
				"datum": current_event["datum"],
				"$or": [
					{"dtm": {"$gte": datetime.datetime.utcnow() - datetime.timedelta(minutes = 5)}},
					{"dtm": None}
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

		responseDict = {
			'Response': {'f_and_m': query_response, 'save_response': save_response}
		}

		return json.dumps(responseDict, default=jsonDefaultHandler)


@app.route("/adp/<metric>", methods=["GET"])
@app.route("/ADP/<metric>", methods=["GET"])
@app.route("/adp/<metric>/", methods=["GET"])
@app.route("/ADP/<metric>/", methods=["GET"])
@app.route("/adp/<metric>/<modifier>", methods=["GET"])
@app.route("/ADP/<metric>/<modifier>", methods=["GET"])
@app.route("/adp/<metric>/<modifier>/", methods=["GET"])
@app.route("/ADP/<metric>/<modifier>/", methods=["GET"])
def adp_metrics(metric, modifier = "None"):
	error = None
	result = None
	realm = "ADP"
	descriptions = ["128K Shoutcast Server", "56K Shoutcast Server"]

	# Do just the simple find()s in the database that return a single
	# integer as a response.

	if metric.upper() == "SONGS":

		result = {}

		commercials = ["AllDayPlay.fm - AllDayPlay.fm", 
					   "AllDayPlay.FM : AllDayPLay.FM - AllDayPlay.FM : AllDayPLay.FM"]

		constraint = {"realm": realm,
					  "name": "Current Song",
					  "datum": {"$nin": commercials}}

		lim = 10 if request.args.get('limit') == None else int(request.args.get('limit'))

		if modifier.upper() == "PLAYED":

			result = []

			for song in db.Event.find(constraint, 
									  {"_id": False, 
									   "datum": True},
									  sort = [("_id", -1)]).limit(lim):
				result.append(song["datum"])

		elif modifier.upper() == "TOTAL":

			result = db.Event.find(constraint).count()

	elif metric.upper() == "SESSIONS":

		result = {}
		total = 0

		if modifier.upper() == "CURRENT":

			for desc in descriptions:
				result[desc] = db.Event.find_one({"realm": realm, 
												  "description": desc,
												  "name": "Current Listeners"},
												 sort = [("_id", -1)])["datum"]
				total += result[desc]

			result["Total"] = total

		elif modifier.upper() == "BOUNCED":

			for desc in descriptions:
				result[desc] = db.Event.find({"realm": realm, 
											  "description": desc,
											  "name": "Listener",
											  "dtm": None}).count()
				total += result[desc]

			result["Total"] = total

		elif modifier.upper() == "TOTAL":

			for desc in descriptions:
				result[desc] = db.Event.find({"realm": realm, 
											  "description": desc,
											  "name": "Listener"}).count()
				total += result[desc]

			result["Total"] = total

		elif "LAST" in modifier.upper():

			# Set up the regular expression...
			try:
				time_re = re.search("(\d+)(MINS|HOURS|DAYS|YEARS)", modifier.upper())
				time_delta = int(time_re.group(1))
			except AttributeError:
				time_delta = 30

			# Let's put together the time delta...
			delta = datetime.timedelta(minutes=time_delta)
			current_time = datetime.datetime.utcnow()

			for desc in descriptions:
				result[desc] = toList(db.Event.find({"realm": realm,
													 "description": desc,
													 "name": "Current Listeners",
													 "dt": {"$gte": (current_time - delta), "$lte": current_time} },
													{"_id": False,
													 "datum": True},
													sort = [("_id", 1)]),
									  "datum")


	elif metric.upper() == "LISTENER":

		result = {}
		temp = {}
		total = 0

		if modifier.upper() == "HOURS":

			total = 0.0

			for desc in descriptions:
				temp = db["yr-metrics"].Events.aggregate([
					{ "$match" : { "realm" : u"ADP", "description" : desc, "name" : u"Listener", "dtm" : { "$ne" : None } } },
					{ "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
					{ "$group": { "_id" : None, "totalListeningTimeInSeconds" : { "$sum" : "$listeningTimeInSeconds" } } }
				])

				result[desc] = temp["result"][0]["totalListeningTimeInSeconds"] / 60 / 60 if temp["ok"] == 1 else "Error"
				total += result[desc] if result[desc] != "Error" else 0

			result["Total"] = total

		elif modifier.upper() == "TOTAL":

			for desc in descriptions:
				result[desc] = len(db.Event.find({"realm": realm, 
												  "description": desc,
												  "name": "Listener",
												  "dtm": { "$ne": None } }).distinct("datum"))
				total += result[desc]

			result["Total"] = total

	return json.dumps({"Result": result}, default=jsonDefaultHandler)




if __name__ == "__main__":
	app.debug = app.config["DEBUG"]
	app.run()