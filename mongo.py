# mongo.py

from flask import Flask
from flask import jsonify
from flask import request
import datetime
import re
from flask_pymongo import PyMongo
from py_zap import Broadcast
from py_zap import Cable
import pymongo
from datetime import datetime, timedelta, date
import copy

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'restdb'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/restdb'

mongo = PyMongo(app)

@app.route('/star', methods=['GET'])
def get_all_stars():
  star = mongo.db.stars
  output = []
  for s in star.find():
    output.append({'name' : s['name'], 'distance' : s['distance']})
  return jsonify({'result' : output})

@app.route('/star/', methods=['GET'])
def get_one_star(name):
  star = mongo.db.stars
  s = star.find_one({'name' : name})
  if s:
    output = {'name' : s['name'], 'distance' : s['distance']}
  else:
    output = "No such name"
  return jsonify({'result' : output})

@app.route('/star', methods=['POST'])
def add_star():
  star = mongo.db.stars
  name = request.json['name']
  distance = request.json['distance']
  star_id = star.insert({'name': name, 'distance': distance})
  new_star = star.find_one({'_id': star_id })
  output = {'name' : new_star['name'], 'distance' : new_star['distance']}
  return jsonify({'result' : output})

@app.route('/rating', methods=['POST'])
def add_rating():
  zap = mongo.db.zaps
  category = request.json['category']
  show = request.json['show']
  net = request.json['net']

  # wrangle the DATE
  airDate = request.json['time']
  airDate = airDate.strip()
  datePieces = airDate.split(" ")
  dateDay = int(datePieces[1])
  # airDate = datePieces[0] + " " + f'{dateDay:02}' + " " + datePieces[2]
  airDate = datePieces[0] + " " + '{:02d}'.format(dateDay) + " " + datePieces[2]
  
  # wrangle the time
  airTime = request.json['airTime']
  airTime = airTime.strip()
  airTime = airTime.replace("p.m.","PM")
  airTime = airTime.replace("a.m.","AM")
  p = re.compile("([01]?[0-9])([:.])?([0-9]{2})?\s?([AP])M?$")
  hours = int(p.match(airTime).group(1))
  if ' PM' in airTime: hours = hours + 12
  hours = '{:02d}'.format(hours)
  if not p.match(airTime).group(3) is None: minutes = int(p.match(airTime).group(3))
  else: minutes = 0
  minutes = '{:02d}'.format(minutes)
  ampm = p.match(airTime).group(4)
  if minutes is None: minutes = "0"
  
  # merge DATE and TIME
  airTime = airDate + " " + hours + ":" + minutes + ":00 EST"

  # convert combined datetime to a timestamp
  airDate = datetime.strptime(airTime,"%B %d %Y %H:%M:%S EST")
  
  rating = request.json['rating']
  viewers = request.json['viewers']
  share = request.json['share']
  try:
    rating_id = zap.insert({'category': category, 'show': title, 'net': net, 'time': airDate, 'rating': rating, 'viewers': viewers, 'share': share})
    new_rating = zap.find_one({'_id': rating_id })
    output = {'category': new_rating['category'], 'title': new_rating['title'], 'net': new_rating['net'], 'time': new_rating['time'], 'rating': new_rating['rating'], 'viewers': new_rating['viewers'], 'share': new_rating['share']}
  except pymongo.errors.DuplicateKeyError:
    output = {"error":"Record already exists"}
  except (RuntimeError, TypeError, NameError):
    pass
  return jsonify({'result' : output})

def getAirDate(mDate,mTime):
  # wrangle the DATE
  print("test [mDate]: " + mDate)
  if isinstance(mDate, str):
    # e.g. October 16 2018 (NO comma)
    airDate = mDate
    airDate = airDate.strip()
    datePieces = airDate.split(" ")
    dateDay = int(datePieces[1])
    airDate = datePieces[0] + " " + '{:02d}'.format(dateDay) + " " + datePieces[2]
  else:
    if isinstance(mDate, date):
      airDate = datetime.strftime(mDate,"%B %d %Y")
    else:
      return False

  # wrangle the time
  airTime = mTime
  airTime = airTime.strip()
  airTime = airTime.replace("p.m.", "PM")
  airTime = airTime.replace("a.m.", "AM")
  p = re.compile("([01]?[0-9])([:.])?([0-9]{2})?\s?([AP])M?$")
  if p.match(airTime) is None:
    print("test [airTime]: " + airTime)
    return False
  hours = int(p.match(airTime).group(1))
  if ' PM' in airTime and hours <> 12: hours = hours + 12
  hours = '{:02d}'.format(hours)
  if not p.match(airTime).group(3) is None:
    minutes = int(p.match(airTime).group(3))
  else:
    minutes = 0
  minutes = '{:02d}'.format(minutes)
  ampm = p.match(airTime).group(4)
  if minutes is None: minutes = "0"

  # merge DATE and TIME
  airTime = airDate + " " + hours + ":" + minutes + ":00 EST"

  # convert combined datetime to a timestamp
  print("test [airTime]: " + airTime)
  airDate = datetime.strptime(airTime, "%B %d %Y %H:%M:%S EST")
  return airDate

def daterange(start_date, end_date):
    end_date = end_date + timedelta(days=1)
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

@app.route('/load', methods=['POST'])
def load_ratings():
  zap = mongo.db.zaps
  fetches = mongo.db.fetches
  nineMonths = timedelta(days=274)
  #nineMonths = timedelta(days=1)
  
  # date to STOP
  try:
    toDate = request.json['toDate']
    toDate = datetime.strptime(toDate,"%Y-%m-%d")
  except:
    toDate = datetime.today()

  # date to START
  try:
    fromDate = request.json['fromDate']
    fromDate = datetime.strptime(fromDate,"%Y-%m-%d")
  except:
    fromDate = toDate - nineMonths

  for single_date in daterange(fromDate, toDate):
    thisDate = single_date.strftime("%B %d %Y")
    ratings = None
    thisCategory = "broadcast"
    try:
      ratings = Broadcast(thisDate)
      thisCategory = "broadcast"
    except:
      pass
    myErrors = []
    myMessages = []
    if not ratings is None:
      for entry in ratings.entries:
        if hasattr(entry, 'share'):
          thisShare = entry.share
        else:
          thisShare = 0
        newDate = getAirDate(thisDate, entry.time)
        if newDate is False:
          myErrors.append({"Date error": {"category": thisCategory, "entry": str(entry)}})
        else:
          rating = {'category': thisCategory, 'show': entry.show, 'net': entry.net, 'time': newDate,
                    'rating': entry.rating,
                    'viewers': entry.viewers, 'share': thisShare}
          copyOfRating = copy.deepcopy(rating)
          try:
            rating_id = zap.insert(rating)
            new_rating = zap.find_one({'_id': rating_id})
            output = {'category': new_rating['category'], 'show': new_rating['show'], 'net': new_rating['net'],
                      'time': new_rating['time'], 'rating': new_rating['rating'], 'viewers': new_rating['viewers'],
                      'share': new_rating['share']}
            myMessages.append({"Success": copyOfRating})
          except pymongo.errors.DuplicateKeyError:
            myErrors.append({"DuplicateKeyError": copyOfRating})
          except (RuntimeError, TypeError, NameError):
            pass
    else:
      myErrors.append({"No ratings": {"category":"broadcast","date":thisDate}})

  output = []
  output.append(myErrors)
  output.append(myMessages)
  return jsonify({'result': output})

if __name__ == '__main__':
    app.run(debug=True)