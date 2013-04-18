from flask import *
from mongokit import Connection, Document, IS, OR
from dateutil.relativedelta import relativedelta

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

			# Set the current time...
			current_time = datetime.datetime.utcnow()
			time_measure = None

			# Perform a regular expression on the URL...
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
				date_list = [ current_time - datetime.timedelta(minutes=x) for x in range(0, time_delta) ]
			elif time_measure == "HOURS":
				delta = datetime.timedelta(hours=time_delta)
				date_list = [ current_time - datetime.timedelta(hours=x, 
																minutes=current_time.minute, 
																seconds=current_time.second) for x in range(0, time_delta) ]
			elif time_measure == "DAYS":
				delta = datetime.timedelta(days=time_delta)
				date_list = [ current_time - datetime.timedelta(days=x,
																hours=current_time.hour,
																minutes=current_time.minute,
																seconds=current_time.second) for x in range(0, time_delta) ]
			elif time_measure == "WEEKS":
				delta = datetime.timedelta(weeks=time_delta)
				date_list = [ current_time - datetime.timedelta(weeks=x,
																hours=current_time.hour,
																minutes=current_time.minute,
																seconds=current_time.second) for x in range(0, time_delta) ]
			elif time_measure == "YEARS":
				delta = datetime.timedelta(days=365*time_delta)
				date_list = [ current_time - datetime.timedelta(days=x) for x in range(0, time_delta, 365) ]


			# Set the list of datetimes...
			date_list.reverse()
			result["date_list"] = list(date_list)
			temp = {}

			# Run the database query...
			for desc in descriptions:
				if time_measure == "MINS":
					result[desc] = toList(db.Event.find({"realm": realm,
														 "description": desc,
														 "name": "Current Listeners",
														 "dt": {"$gte": (current_time - delta), "$lte": current_time} },
														{"_id": False,
														 "datum": True},
														sort = [("_id", 1)]),
										  "datum")
				else:

					listeners = db.Event.find({ "realm": realm, 
												"description": desc, 
												"name": "Listener",
												"dtm": { "$gte": (current_time - delta) } }, 
											  { "_id": False,
												"dt": True,
												"dtm": True },
											  sort = [("_id", 1)])

					# Go through the query results and compile the results...
					temp = {}
					for key in date_list:
						dates = (key, key + relativedelta(hours=+1))

						for x in range(0, listeners.count()):

							# Start time is before the current hour and the end time is after the current hour.
							if listeners[x]["dt"] <= dates[0] and dates[0] < listeners[x]["dtm"]:
								temp[dates[0]] = temp[dates[0]] + 1 if dates[0] in temp else 1

							# Start time is during this hour.
							elif listeners[x]["dt"] >= dates[0] and listeners[x]["dt"] < dates[1]:
								temp[dates[0]] = temp[dates[0]] + 1 if dates[0] in temp else 1

							# End time is during this hour.
							elif listeners[x]["dtm"] >= dates[0] and listeners[x]["dtm"] < dates[1]:
								temp[dates[0]] = temp[dates[0]] + 1 if dates[0] in temp else 1
					
					# Sort the resulting list and return it.
					l = list()
					for a in date_list:
						l.append(temp[a])

					result[desc] = list(l)


		elif "BETWEEN" in modifier.upper():

			try:
				time_re = re.search("BETWEEN(\d+-\d+-\d+T\d+:\d+:\d+)AND(\d+-\d+-\d+T\d+:\d+:\d+)", modifier.upper())
				start_time = datetime.datetime.strptime(time_re.group(1), '%Y-%m-%dT%H:%M:%S')
				end_time = datetime.datetime.strptime(time_re.group(2), '%Y-%m-%dT%H:%M:%S')

				for desc in descriptions:
					result[desc] = toList(db.Event.find({"realm": realm,
														 "description": desc,
														 "name": "Current Listeners",
														 "dt": {"$gte": start_time, "$lte": end_time} },
														{"_id": False,
														 "datum": True},
														sort = [("dt", 1)]),
										  "datum")

			except AttributeError:
				result = "AttributeError"

		elif modifier.upper() == "AVGLISTENINGTIME":

			total = 0.0

			# Get the individual average listening time...
			for desc in descriptions:
				temp = db["yr-metrics"].Events.aggregate([
					{ "$match" : { "realm" : u"ADP", "description" : desc, "name" : u"Listener", "dtm" : { "$ne" : None } } },
					{ "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
					{ "$group": { "_id" : None, "avgListeningTimeInSeconds" : { "$avg" : "$listeningTimeInSeconds" } } }
				])

				result[desc] = temp["result"][0]["avgListeningTimeInSeconds"] / 60 if temp["ok"] == 1 else "Error"

			# Get the overall average listening time...
			temp = db["yr-metrics"].Events.aggregate([
				{ "$match" : { "realm" : u"ADP", "description" : { "$in" : descriptions }, "name" : u"Listener", "dtm" : { "$ne" : None } } },
				{ "$project" : { "datum" : 1, "dt" : 1, "dtm" : 1, "listeningTimeInSeconds" : { "$divide" : [{ "$subtract" : ["$dtm", "$dt"] }, 1000] } } },
				{ "$group": { "_id" : None, "avgListeningTimeInSeconds" : { "$avg" : "$listeningTimeInSeconds" } } }
			])

			result["Overall"] = temp["result"][0]["avgListeningTimeInSeconds"] / 60 if temp["ok"] == 1 else "Error"


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