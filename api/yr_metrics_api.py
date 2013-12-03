from flask import *
from mongokit import Connection, Document, IS, OR
from dateutil.relativedelta import relativedelta

import datetime, pytz, time
import math
import re
import json

import mongo_structure as mdb
from metrics_util import *
from metrics import Metrics
from alldayplay import AllDayPlay

# Create the application object
app = Flask(__name__)
app.config.from_pyfile('yr_metrics_api.cfg', silent=False)

# Connect to the database
db = Connection(app.config["MONGODB_HOST"], app.config["MONGODB_PORT"])

# Set up API methods
metrics = Metrics(request=request, database_connection=db)
adp = AllDayPlay(request=request, database_connection=db)

# Mongo Schema. These objects all live in the mongo_structure import.
db.register(mdb.RootDocument)
db.register(mdb.Event)
db.register(mdb.Count)

# Add an event to the logging table.
app.add_url_rule('/event/<func>', 'event_add_or_touch', lambda func: metrics.addOrTouchEvent(func), methods=["GET", "POST"])

# AllDayPlay Metrics.
app.add_url_rule('/adp/songs/played', 'adp_last_songs_played', adp.lastSongsPlayed, methods=["GET"])
app.add_url_rule('/adp/songs/total', 'adp_total_songs_played', adp.totalSongsPlayed, methods=["GET"])
app.add_url_rule('/adp/sessions/current', 'adp_current_num_sessions', adp.currentNumberOfListeningSessions, methods=["GET"])
app.add_url_rule('/adp/sessions/bounced', 'adp_total_sessions_bounced', adp.totalSessionsBounced, methods=["GET"])
app.add_url_rule('/adp/sessions/total', 'adp_total_sessions', adp.totalSessions, methods=["GET"])
app.add_url_rule('/adp/listener/hours', 'adp_total_listener_hours', adp.totalListenerHours, methods=["GET"])
app.add_url_rule('/adp/listener/total', 'adp_total_listeners', adp.totalListeners, methods=["GET"])
app.add_url_rule('/adp/sessions/avglisteningtime', 'adp_average_session_listening_time', adp.averageListeningTime, methods=["GET"])
app.add_url_rule('/adp/sessions/last<modifier>', 'adp_last_X', lambda modifier: adp.lastX(modifier), methods=["GET"])

# Run the app.
if __name__ == "__main__":
	app.debug = app.config["DEBUG"]
	app.run()