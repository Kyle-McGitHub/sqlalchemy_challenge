from flask import Flask, jsonify
from sqlalchemy import create_engine, MetaData, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime, timedelta

#USE THIS TO RENAME THE APP
hawaii_app = Flask(__name__)

# FILE PATH WORKS.
engine = create_engine('sqlite:///C:/Users/Kyle_McDaniel_Python/Desktop/Columbia_Analytics_Bootcamp/sqlalchemy_challenge/Resources/hawaii.sqlite')
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session factory
SessionLocal = sessionmaker(bind=engine)

@hawaii_app.route('/home')
def index():
    return jsonify({
        "available_routes": [
            "",
            "/precipitation", "Lists daily precipitation for one year before the end of the dateset.",
            "",
            "/stations", "Lists all stations.",
            "",
            "/tobs", "Temperature observations for one year before the end of the dateset.",
            "",
            "/<start_date>", "Enter date in the format 'YYYY-MM-DD', will return to end of dataseet.",
            "",
            "/<start_date>/<end_date>", "Enter both dates in the format 'YYYY-MM-DD'."
        ]
    })

# Precipitation route
@hawaii_app.route('/precipitation')
def get_precipitation():
    # Query the maximum date recorded in the Measurement table

    session = SessionLocal()

    max_date = session.query(func.max(Measurement.date)).scalar()
    
    # Calculate the date one year ago from the maximum date recorded
    one_year_ago = datetime.strptime(max_date, '%Y-%m-%d') - timedelta(days=365)
    
    # Query the database for precipitation data for the last year, sorted by date in descending order
    results = session.query(Measurement.date, Measurement.prcp).\
                    filter(Measurement.date >= one_year_ago).\
                    filter(Measurement.date <= max_date).\
                    order_by(Measurement.date.desc()).\
                    all()

    
    # Convert the query results to a dictionary with date as key and precipitation as value
    precipitation_data = {date: prcp for date, prcp in results}

    session.close()
    
    # Return the precipitation data as a JSON response
    return jsonify(precipitation_data)

# Stations route
@hawaii_app.route('/stations')
def get_stations():
    # Create a new session
    session = SessionLocal()
    
    # Query the database to retrieve all stations
    stations = session.query(Station.station).all()
    
    # Convert the query results to a list
    station_list = [station[0] for station in stations]
    
    # Close the session
    session.close()
    
    # Return the stations data as a JSON response
    return jsonify(station_list)

# TOBS route
@hawaii_app.route('/tobs')
def get_tobs():

    session = SessionLocal()

    # Query the maximum date recorded in the Measurement table
    max_date = session.query(func.max(Measurement.date)).scalar()
    
    # Calculate the date one year ago from the maximum date recorded
    one_year_ago = datetime.strptime(max_date, '%Y-%m-%d') - timedelta(days=365)
    
    # Query the database for temperature observations for the most active station
    results = session.query(Measurement.date, Measurement.tobs).\
                    filter(Measurement.station == 'USC00519281').\
                    filter(Measurement.date >= one_year_ago).\
                    filter(Measurement.date <= max_date).\
                    order_by(Measurement.date.desc()).\
                    all()

    
    # Convert the query results to a dictionary with date as key and temperature as value
    tobs_data = {date: tobs for date, tobs in results}

    session.close()
    
    # Return the TOBS data as a JSON response
    return jsonify(tobs_data)

# START DATE
@hawaii_app.route('/<start_date>')
def start_date_summary(start_date):

    session = SessionLocal()

    # Convert start date string to datetime object
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Query the maximum date recorded in the Measurement table
    max_date = session.query(func.max(Measurement.date)).scalar()
    max_date_dt = datetime.strptime(max_date, '%Y-%m-%d')

    # Query the database to calculate min, max, and average temperatures from start date to end of dataset
    summary = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
                    filter(Measurement.date >= start_date_dt).\
                    filter(Measurement.date <= max_date_dt).\
                    all()
    
    min_temp, max_temp, avg_temp = summary[0]
    
    # Create a dictionary
    summary_data = {
        "start_date": start_date,
        "end_date": max_date,
        "min_temperature": min_temp,
        "max_temperature": max_temp,
        "avg_temperature": avg_temp
    }

    session.close()
    
    # Return as a JSON response
    return jsonify(summary_data)


# START AND END DATE
@hawaii_app.route('/<start_date>/<end_date>')
def date_range_summary(start_date, end_date):

    session = SessionLocal()

    # Convert start and end dates strings to datetime objects
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Query the database to calculate min, max, and average temperatures from start date to end date
    summary = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
                    filter(Measurement.date >= start_date_dt).\
                    filter(Measurement.date <= end_date_dt).\
                    all()
    
    min_temp, max_temp, avg_temp = summary[0]
    
    # Create a dictionary
    summary_data = {
        "start_date": start_date,
        "end_date": end_date,
        "min_temperature": min_temp,
        "max_temperature": max_temp,
        "avg_temperature": avg_temp
    }

    session.close()
    
    # Return as a JSON response
    return jsonify(summary_data)


if __name__ == '__main__':
    hawaii_app.run(debug=True)