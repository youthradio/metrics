from flask import *
from mongokit import Connection, Document, IS, OR
import datetime
import re
import json


# Configuration
DEBUG = True
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017

# Create the application object
app = Flask(__name__)
app.config.from_object(__name__)

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
def yr_metrics_event(func):
	error = None
	current_event = db.Event()
	current_event["realm"] = unicode(request.args.get('realm'))
	current_event["description"] = unicode(request.args.get('description'))
	current_event["name"] = unicode(request.args.get('name'))
	current_event["source"] = unicode(request.args.get('source'))
	current_event["useragent"] = unicode(request.args.get('useragent'))
	current_event["dt"] = datetime.datetime.utcnow()

	# Need to test for whether or not the data is a string, a float, or an int
	datum = None
	if "." in request.args.get('datum'):
		# This could be a float or a string...
		try:
			datum = float(request.args.get('datum'))
		except ValueError, e:
			datum = unicode(request.args.get('datum'))
	else:
		# This could be an int or a string...
		try:
			datum = int(request.args.get('datum'))
		except ValueError, e:
			datum = unicode(request.args.get('datum'))

	current_event["datum"] = datum

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


if __name__ == "__main__":
	app.debug = app.config["DEBUG"]
	app.run()