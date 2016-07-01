import ConfigParser
import argparse
import datetime as dt
import json
import os
import urllib2
from itertools import chain

import pandas as pd

config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.cfg'))
url = "https://www.googleapis.com/qpxExpress/v1/trips/search?key={0}".format(config.get('Google API', 'key'))

def get_dates(start_date=None):
    start_date = dt.date.today() if not start_date else start_date
    eoy_date = dt.date(start_date.year, 12, 31)

    # get all the weekends between start and end
    t = dt.timedelta((11 - start_date.weekday()) % 7)
    first_friday = start_date + t
    first_sunday = first_friday + dt.timedelta(2)

    # add start time
    first_friday = dt.datetime.combine(first_friday, dt.time(18, 0))
    first_sunday = dt.datetime.combine(first_sunday, dt.time(14, 0))

    # get all the weekends
    fridays = [first_friday + dt.timedelta(i) for i in xrange(7, (eoy_date - start_date).days, 7)]
    sundays = [first_sunday + dt.timedelta(i) for i in xrange(7, (eoy_date - start_date).days, 7)]

    return zip(fridays, sundays)


def generate_code(from_date, to_date, from_city, to_city):
    code = {
        "request": {
            "passengers": {
                "kind": "qpxexpress#passengerCounts",
                "adultCount": 2,
            },
            "slice": [
                {
                    "origin": from_city,
                    "kind": "qpxexpress#sliceInput",
                    "destination": to_city,
                    "maxStops": 0,
                    "permittedDepartureTime": {
                        "latestTime": "23:59",
                        "kind": "qpxexpress#timeOfDayRange",
                        "earliestTime": from_date.strftime('%H:%M'),  # The earliest time of day in HH:MM format.
                    },
                    "date": from_date.strftime('%Y-%m-%d'),  # Departure date in YYYY-MM-DD format.
                },
                {
                    "origin": to_city,
                    "kind": "qpxexpress#sliceInput",
                    "destination": from_city,
                    "maxStops": 0,
                    "permittedDepartureTime": {
                        "latestTime": "23:59",  # The latest time of day in HH:MM format.
                        "kind": "qpxexpress#timeOfDayRange",
                        "earliestTime": to_date.strftime('%H:%M'),  # The earliest time of day in HH:MM format.
                    },
                    "date": to_date.strftime('%Y-%m-%d'),  # Departure date in YYYY-MM-DD format.
                },
            ],
            # "saleCountry": home_city,
            "solutions": 500,  # The number of solutions to return, maximum 500.
            "refundable": False,  # Return only solutions with refundable fares.
        },
    }
    return code


def get_flights(code):
    json_req = json.dumps(code, encoding='utf-8')
    req = urllib2.Request(url, json_req, {'Content-Type': 'application/json'})
    flight = urllib2.urlopen(req)
    response = flight.read()
    flight.close()

    return json.loads(response)


def get_options(destination, home_city='NYC', n_of_options=3):
    dates = get_dates()

    output = []

    for start_date, end_date in dates:
        code = generate_code(start_date, end_date, home_city, destination)
        flights = get_flights(code)

        try:
            sorted_flights = [x for x in sorted(flights['trips']['tripOption'],
                                                key=lambda x: float(x['saleTotal'][3:]))]  # 'USD1020.00' so strip 'USD'
        except KeyError:
            continue

        google_flights_url = "https://www.google.com/flights/#search;f={0};t={1};d={2};r={3};s=0;ti=t{4}-2400,t{5}-2400".format(
            home_city, destination, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),
            start_date.strftime('%H%M'), end_date.strftime('%H%M'))

        row = [start_date, end_date]
        for x in sorted_flights[:n_of_options]:
            row.extend([
                x['saleTotal'],
                x['slice'][0]['segment'][0]['flight']['carrier'] + x['slice'][0]['segment'][0]['flight']['number'],
                x['slice'][0]['segment'][0]['leg'][0]['origin'],
                x['slice'][0]['segment'][0]['leg'][0]['destination'],
                x['slice'][0]['segment'][0]['leg'][0]['departureTime'],
                x['slice'][0]['segment'][0]['leg'][0]['arrivalTime'],
                x['slice'][1]['segment'][0]['flight']['carrier'] + x['slice'][0]['segment'][0]['flight']['number'],
                x['slice'][1]['segment'][0]['leg'][0]['origin'],
                x['slice'][1]['segment'][0]['leg'][0]['destination'],
                x['slice'][1]['segment'][0]['leg'][0]['departureTime'],
                x['slice'][1]['segment'][0]['leg'][0]['arrivalTime']
            ]
            )

        output.append(row)

    option_columns = [
        'Sale Total', 'Flight Number', 'Origin', 'Destination', 'Departure Time', 'Arrival Time',
        'Flight Number', 'Origin', 'Destination', 'Departure Time', 'Arrival Time'
    ]
    col_names = ['Start Date', 'End Date'] + option_columns * n_of_options
    col_index_l1 = [['Option ' + str(i)] * len(option_columns) for i in xrange(1, n_of_options + 1)]
    col_index_l1 = list(chain.from_iterable(col_index_l1))
    col_index_l2 = option_columns * n_of_options
    col_multi_index = pd.MultiIndex.from_tuples(list(zip(col_index_l1, col_index_l2)), names=['Option', 'Details'])

    output = [row + ([None] * (len(col_names) - len(row))) for row in output]

    return_df = pd.DataFrame(output, columns=col_names)
    return_df.set_index(['Start Date', 'End Date'], inplace=True)

    return_df.columns = col_multi_index
    return return_df


def get_destination():
    parser = argparse.ArgumentParser(description='Get flights on weekends for the rest of the year')
    parser.add_argument('dest', nargs=1, type=str, default='AUS', help="Destination to price")
    args = parser.parse_args()
    return args.dest


if '__name__' == '__main__':
    destination = get_destination() # get destination
    output_df = get_options(destination)
    output_df.to_pickle(os.getcwd() + os.sep + 'flights.{}.pickle'.format(destination))
