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
  signal = request.json['signal']
  title = request.json['title']
  network = request.json['network']

  # wrangle the DATE
  airDate = request.json['airDate']
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
  print(airTime)
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
    rating_id = zap.insert({'signal': signal, 'title': title, 'network': network, 'airDate': airDate, 'rating': rating, 'viewers': viewers, 'share': share})
    new_rating = zap.find_one({'_id': rating_id })
    output = {'signal': new_rating['signal'], 'title': new_rating['title'], 'network': new_rating['network'], 'airDate': new_rating['airDate'], 'rating': new_rating['rating'], 'viewers': new_rating['viewers'], 'share': new_rating['share']}
  except pymongo.errors.DuplicateKeyError:
    output = {"error":"Record already exists"}
  except (RuntimeError, TypeError, NameError):
    pass
  return jsonify({'result' : output})

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

@app.route('/load', methods=['POST'])
def load_ratings():
  fetches = mongo.db.fetchs
  nineMonths = timedelta(days=274)
  nineMonths = timedelta(days=1)
  
  # date to STOP
  toDate = request.json['toDate']
  toDate = datetime.strptime(toDate,"%Y-%m-%d")
  if toDate is None: toDate = datetime.date.today()

  # date to START
  fromDate = request.json['fromDate']
  fromDate = datetime.strptime(fromDate,"%Y-%m-%d")
  if fromDate is None: fromDate = toDate - nineMonths

  for single_date in daterange(fromDate, toDate):
    thisDate = single_date.strftime("%B %d %Y")
    try:
      ratings = Broadcast(thisDate)
    except:
      pass
    print(ratings[0])
  
  output = {"message": "DONE"}

  return jsonify({'result' : output})
if __name__ == '__main__':
    app.run(debug=True)