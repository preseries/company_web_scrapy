#!/usr/bin/env python
# -*- coding: utf-8 -*-
import calendar
import time
from time import sleep
import os, shutil, re
import requests

from datetime import datetime, timedelta
from dateutil import relativedelta
from pymongo.mongo_client import MongoClient
from pymongo.errors import BulkWriteError
from math import ceil, trunc
import traceback


def mkdatetime(datetime_str):
    try:
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
    except:
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')


def milliseconds(datetime_val):
    epoch = time.mktime(datetime_val.timetuple()) + \
                (datetime_val.microsecond / 1000000.)
    return int(round((float('%f' % epoch) * 1000)))


def get_file_by_snapshot_date(snapshot_date, path, prefix, exact_date=True):
    """
    We are going to look into the path for all the files that starts with
    the prefix and with the same snapshot_date as informed or closest
    to it (exact_date)

    If no snapshot_date is informed, we will get the newest snapshot file

    :param snapshot_date: Snapshot Date
    :param path: Resource path
    :param prefix: Resource type (dataset, cluster, ...)
    :param exact_date: Return exactly the date resource, or the closest
    """
    if snapshot_date is not None:
        files = [i for i in os.listdir(path) if os.path.isfile(os.path.join(path, i)) and i.startswith(prefix) and i[len(i)-10:len(i)] <= snapshot_date.strftime('%Y-%m-%d')]
    else:
        files = [i for i in os.listdir(path) if os.path.isfile(os.path.join(path, i)) and i.startswith(prefix)]

    filename = sorted(files, reverse=True)[0] if len(files) > 0 else None

    if filename is not None and exact_date and \
            not filename.endswith(snapshot_date.strftime('%Y-%m-%d')):
        return None

    return "%s/%s" % (path, filename) if filename is not None else None

def download_file(current_date, url, dest_path, local_filename):
    """
     This function downloads the file at the URL parameter and saves it
     in the dest_path using the given local_filename and the current_date

     For instance, download today 2016-12-06 the file
     at http://webserver/myfile.xxx and leave it in the local folder located
     at /data/etlprocess/output_data/my_folder with the suffix name 'dest_file'

     This function will leave the downloaded file here:
        /data/etlprocess/output_data/my_folder/dest_file_2016-12-16

    Then we can use the function get_file_by_snapshot_date to look for a file
     with a given suffix and a date.

    :return: the absolute path to the file
    """
    r = requests.get(url, stream=True)
    with open(os.path.join(dest_path, local_filename), 'wb') as f:
        shutil.copyfileobj(r.raw, f)

    if not os.path.isfile(os.path.join(dest_path, local_filename)):
        raise Exception("Unable to download the %s"
                        " file from: [%s]" %
                        (local_filename, url))

    shutil.move(os.path.join(dest_path, local_filename),
                os.path.join(dest_path, "%s_%s" %
                             (local_filename,
                              current_date.strftime('%Y-%m-%d'))))

    return os.path.join(dest_path, "%s_%s" %
                        (local_filename, current_date.strftime('%Y-%m-%d')))


def clean_string(value):

    '''
    re1 = "(^the )|(^oy )|( ?/ .*$)|( ?- .*$)|( -.*$)|( ?: .*$)|( ?[|] .*$)|( formerly .*$)|(\([^)]*\))|(\")|((( & co)|( ag)|( ab)|( as)|( (and )?co)|( corp)|( corporation)|( limited)|( ((and|&) )?company)|( g?mbh)|( llc)|( llp)|( lp)|( pte)|( pty)|( oy)|( pllc)|( ltd)|( plc)|( pc)|( sa)|( se)|( sas)|( sarl)|( inc)|( [a-z]/[a-z]/[a-z]/[a-z])|( [a-z]/[a-z]/[a-z])|( [a-z]/[a-z])|( [a-z][.][a-z][.][a-z][.][a-z])|( [a-z][.][a-z][.][a-z])|( [a-z][.][a-z]))($|[-.,:|/ ].*$))"
    '''
    re1 = []
    re1.append('(^the )')
    re1.append('(^oy )')
    re1.append('( ?/ .*$)')
    re1.append('( ?- .*$)')
    re1.append('( -.*$)')
    re1.append('( ?: .*$)')
    re1.append('( ?[\', \'] .*$)')
    re1.append('( formerly .*$)')
    re1.append('(\\([^)]*\\))')
    re1.append('(")')
    re1.append('((( & co)')
    re1.append('( ag)')
    re1.append('( ab)')
    re1.append('( as)')
    re1.append('( (and )?co)')
    re1.append('( corp)')
    re1.append('( corporation)')
    re1.append('( limited)')
    re1.append('( ((and\', \'&) )?company)')
    re1.append('( g?mbh)')
    re1.append('( llc)')
    re1.append('( llp)')
    re1.append('( lp)')
    re1.append('( pte)')
    re1.append('( pty)')
    re1.append('( oy)')
    re1.append('( pllc)')
    re1.append('( ltd)')
    re1.append('( plc)')
    re1.append('( pc)')
    re1.append('( sa)')
    re1.append('( se)')
    re1.append('( sas)')
    re1.append('( sarl)')
    re1.append('( inc)')
    re1.append('( [a-z]/[a-z]/[a-z]/[a-z])')
    re1.append('( [a-z]/[a-z]/[a-z])')
    re1.append('( [a-z]/[a-z])')
    re1.append('( [a-z][.][a-z][.][a-z][.][a-z])')
    re1.append('( [a-z][.][a-z][.][a-z])')
    re1.append('( [a-z][.][a-z]))($')
    re1.append('[-.,:\', \'/ ].*$))')
    re1.append('[-.,:,\'/]*')

    re2 = "-"
    # re3 = "\p{Punct}"
    # re4 = "\p{Space}+"

    replaced = re.sub('|'.join(re1), '', value.lower())
    replaced = re.sub(re2, ' ', replaced)
    # replaced = re.sub(re3, '', replaced)
    # replaced = re.sub(re4, ' ', replaced)

    return replaced.strip()

def add_months(sourcedate, months):
    """
        Add a number of months to the sourcedate, and returns the new date

        Ex.
            newdate = add_months(datetime.datetime(2015, 12, 17),2)

            newdate is now  datetime.datetime(2016, 2, 17, 0, 0)

    :param sourcedate: base date
    :param months: number of months to add
    """

    month = sourcedate.month - 1 + months
    year = int(sourcedate.year + month / 12)
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day)


def set_field_interval(date_field, interval):
    """
        Depending on the interval ("year", "quarter", "month", "dayOfYear",
        "week"), and the date_field, returns a dictionary with
        this format

        Ex.
            {"$year": "$date"}
            {"$trunc": {"$ceil": {"$divide": [{"$month": "$date"},3]}}}
            {"$month": "$date"}
            {"$dayOfYear": "$date"}
            {"$week": "$date"}

    :param date_field: name of the field i.e. "date", "updated", "created"
    :param interval: period type i.e. "year", "quarter", "month", "dayOfYear"
    """

    if interval == 'year':
        result = {"$year": "$" + date_field}
    elif interval == 'quarter':
        result = {"$trunc": {"$ceil": {"$divide": [
            {"$month": "$" + date_field}, 3]}}}
    elif interval == 'month':
        result = {"$month": "$" + date_field}
    elif interval == 'dayOfYear':
        result = {"$dayOfYear": "$" + date_field}
    elif interval == 'week':
        result = {"$week": "$" + date_field}
    else:
        print "Wrong interval %s" % interval
        return {}
    return result


def interval_date_range(chg_interval, interval):
    """
        Depending on the interval ("year", "quarter", "month", "dayOfYear",
        "week") and the chg_interval dictionary, returns two dates of the range
        of the period

    Ex.
    year     -> datetime.datetime(2015, 1, 1), datetime.datetime(2016, 1, 1)
    quarter  -> datetime.datetime(2015, 4, 1), datetime.datetime(2015, 7, 1)
    month    -> datetime.datetime(2015, 12, 1), datetime.datetime(2016, 1, 1)
    dayOfYear-> datetime.datetime(2015, 3, 21), datetime.datetime(2015, 3, 22)
    week     -> datetime.datetime(2015, 2, 14), datetime.datetime(2015, 2, 21)

    :param chg_interval: dictionary containing a "year" (ordinal year) and
           "interval" (number of xxxxxx) attributes
    :param interval: period type --> year, quarter, month, dayOfYear, week
    """

    if interval == 'year':
        result = datetime(chg_interval['year'], 1, 1), \
                 datetime(chg_interval['year'] + 1, 1, 1)
    elif interval == 'quarter':
        result = datetime(chg_interval['year'],
                          3 * int(chg_interval['interval']) - 2, 1), \
                 add_months(datetime(chg_interval['year'],
                                     3*int(chg_interval['interval'])-2, 1), 3)
    elif interval == 'month':
        result = datetime(chg_interval['year'], chg_interval['interval'], 1), \
                 add_months(datetime(chg_interval['year'],
                                     chg_interval['interval'], 1), 1)
    elif interval == 'dayOfYear':
        result = datetime(chg_interval['year'], 1, 1) + \
                 timedelta(chg_interval['interval'] - 1), \
                 datetime(chg_interval['year'], 1, 1) + \
                 timedelta(chg_interval['interval'])
    elif interval == 'week':
        result = datetime.strptime(
            "%04d-%02d" % (chg_interval['year'],
                           chg_interval['interval']) + '-0', "%Y-%U-%w"), \
                 datetime.strptime(
            "%04d-%02d" % (chg_interval['year'],
                           chg_interval['interval']) +
            '-0', "%Y-%U-%w") + timedelta(days=7)
    else:
        print "Wrong interval %s" % interval
        return {}
    return result


def calculated_fields(fields_to_update, interval, computed_aggregation,
                      resource_type):
    """
        Depending on the interval ("year", "quarter", "month", "dayOfYear",
        "week") updates the fields_to_insert dictionary with the new keys:

        relative_period, real_year, real_quarter, real_month, real_week,
        real_day

        when available

    :param fields_to_insert: base dictionary containing the foundation_date key
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param computed_aggregation: dictionary containing the "year", "interval"
    """

    base_date = fields_to_update.get('company_foundation_date')
    if resource_type == 'person':
        base_date = fields_to_update.get('born')

    year = computed_aggregation.get('year')
    interval_value = computed_aggregation.get('interval')

    if interval == 'year':
        fields_to_update['real_year'] = year
        if base_date is not None:
            fields_to_update['relative_period'] = year - base_date.year
    elif interval == 'quarter':
        fields_to_update['real_year'] = year
        fields_to_update['real_quarter'] = interval_value
        if base_date is not None:
            fields_to_update['relative_period'] = \
                (year * 4 + interval_value) - \
                (base_date.year * 4 +
                 trunc(ceil(base_date.month / 3.0)))
    elif interval == 'month':
        fields_to_update['real_year'] = year
        fields_to_update['real_quarter'] = trunc(ceil(interval_value / 3.0))
        fields_to_update['real_month'] = interval_value
        if base_date is not None:
            fields_to_update['relative_period'] = \
                (year * 12 + interval_value) - \
                (base_date.year * 12 + base_date.month)
    elif interval == 'dayOfYear':
        doy_date = datetime(year, 1, 1) + timedelta(interval_value - 1)
        fields_to_update['real_year'] = doy_date.year
        fields_to_update['real_quarter'] = trunc(ceil(doy_date.month / 3.0))
        fields_to_update['real_month'] = doy_date.month
        fields_to_update['real_week'] = doy_date.isocalendar()[1]
        fields_to_update['real_day'] = interval_value
        if base_date is not None:
            fields_to_update['relative_period'] = \
                (year * 365 + interval_value) - \
                (base_date.year * 365 +
                 base_date.timetuple().tm_yday)
    elif interval == 'week':
        week_date = datetime.strptime("%04d-%02d" % (year, interval_value) +
                                      '-0', "%Y-%U-%w")
        fields_to_update['real_year'] = week_date.year
        fields_to_update['real_quarter'] = trunc(ceil(week_date.month / 3.0))
        fields_to_update['real_month'] = week_date.month
        fields_to_update['real_week'] = interval_value
        if base_date is not None:
            fields_to_update['relative_period'] = \
                (year * 52 + interval_value) - \
                (base_date.year * 52 + base_date.isocalendar()[1])
    else:
        print "Wrong interval %s" % interval


def get_ts_changed_intervals(source_db_uri, source_collection, last_execution,
                             curr_date, process_field, interval, resource_type):
    """
        Generation of the pipeline aggregation string, execution, and returns
        the periods with changes

        Ex.

        [{"$match":
            {"metric_name": {"$in": ["twitter_followers", "twitter_following"]},
             "updated": {"$gte": last_execution, "$lt": curr_date}}},
         { "$project":
             {"company_id": "$_id",
               "date": "$date",
               "year": {"$year": "$date"},
               "interval": {"$month": "$date"}}},
         {"$group":
             {"_id": { "company_id": "$company_id","year":"$year",
                        "interval": "$interval"}}},
         {"$project":  {"_id": 0, "company_id": "$_id.company_id",
                         "year": "$_id.year", "interval": "$_id.interval"}}]

         Returns a list of changed intervals with this structure:

         [{"company_id": ObjectID("2341231231"), "year": 2013, "interval": 7},
          {"company_id": ObjectID("2341231231"), "year": 2013, "interval": 8},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 9},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 11}
          ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param last_execution: Lower date
    :param curr_date: Upper date
    :param process_field: metric to process: "twitter_followers"
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param resource_type: Resource Type (company, person, ...)
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    pipeline_periods = [
        {"$match": {"metric_name": process_field,
                    "updated": {"$lt": curr_date}}},
        {"$project": {"%s_id" % resource_type: "$%s_id" % resource_type,
                      "date": "$date",
                      "year": {"$year": "$date"},
                      "interval": set_field_interval("date", interval)}},
        {"$group": {"_id": {"%s_id" % resource_type: "$%s_id" % resource_type,
                            "year": "$year",
                            "interval": "$interval"}}},
        {"$project": {"_id": 0, "%s_id" % resource_type: "$_id.%s_id" %
                                                         resource_type,
                      "year": "$_id.year", "interval": "$_id.interval"}}]

    # If last_execution is informed, add the $gte clause to the "$match" elem.
    if last_execution is not None:
        pipeline_periods[0]['$match']["updated"]['$gte'] = last_execution

    changed_intervals = []

    for changed_interval in collection.aggregate(
            pipeline_periods, allowDiskUse=True):
        changed_intervals.append(changed_interval)

    client.close()

    return changed_intervals


def get_ts_changed_intervals_1toN(source_db_uri, source_collection,
                                  last_execution, curr_date, process_field,
                                  date_field, interval, resource_type):
    """
        Generation of the pipeline aggregation string, execution, and returns
        the periods with changes

        Ex.

        [{"$match":
            {"updated": {"$gte": last_execution, "$lt": curr_date}}},
         { "$project":
             {"company_id": "$_id",
               "date": "$date",
               "year": {"$year": "$date"},
               "interval": {"$month": "$date"}}},
         {"$group":
             {"_id": { "company_id": "$company_id","year":"$year",
                        "interval": "$interval"}}},
         {"$project":  {"_id": 0, "company_id": "$_id.company_id",
                         "year": "$_id.year", "interval": "$_id.interval"}}]

         Returns a list of changed intervals with this structure:

         [{"company_id": ObjectID("2341231231"), "year": 2013, "interval": 7},
          {"company_id": ObjectID("2341231231"), "year": 2013, "interval": 8},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 9},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 11}
          ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param last_execution: Lower date
    :param curr_date: Upper date
    :param process_field: metric to process: "twitter_followers"
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param resource_type: Resource Type (company, person, ...)
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    pipeline_periods = [
        {"$match": {"updated": {"$lt": curr_date}}},
        {"$project": {"%s_id" % resource_type: "$%s_id" % resource_type,
                      "date": "$%s" % date_field,
                      "year": {"$year": "$%s" % date_field},
                      "interval": set_field_interval(date_field, interval)}},
        {"$group": {"_id": {"%s_id" % resource_type: "$%s_id" % resource_type,
                            "year": "$year",
                            "interval": "$interval",
                            "date": "$date"}}},
        {"$project": {"_id": 0, "%s_id" % resource_type: "$_id.%s_id" % resource_type,
                      "year": "$_id.year", "interval": "$_id.interval", "date": "$_id.date"}}]

    # If last_execution is informed, add the $gte clause to the "$match" elem.
    if last_execution is not None:
        pipeline_periods[0]['$match']["updated"]['$gte'] = last_execution

    changed_intervals = []

    for changed_interval in collection.aggregate(pipeline_periods, allowDiskUse=True):
        changed_intervals.append(changed_interval)

    client.close()

    return changed_intervals


def get_changed_intervals(source_db_uri, source_collection, last_execution,
                          curr_date, process_field, interval, resource_type):
    """
        Generation of the pipeline aggregation string, execution, and returns
        the periods with changes

        Ex.

        [{"$match":
            {"metric_name": {"$in": ["twitter_followers", "twitter_following"]},
             "updated": {"$gte": last_execution, "$lt": curr_date}}},
         { "$project":
             {"company_id": "$_id",
               "date": "$date",
               "year": {"$year": "$date"},
               "interval": {"$month": "$date"}}},
         {"$group":
             {"_id": { "company_id": "$company_id","year":"$year",
                        "interval": "$interval"}}},
         {"$project":  {"_id": 0, "company_id": "$_id.company_id",
                         "year": "$_id.year", "interval": "$_id.interval"}}]

         Returns a list of changed intervals with this structure:

         [{"company_id": ObjectID("2341231231"), "year": 2013, "interval": 7},
          {"company_id": ObjectID("2341231231"), "year": 2013, "interval": 8},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 9},
          {"company_id": ObjectID("2341231444"), "year": 2015, "interval": 11}
          ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param last_execution: Lower date
    :param curr_date: Upper date
    :param process_field: metric to process: "twitter_followers"
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param resource_type: Resource Type (company, person, ...)
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    pipeline_periods = [
        {"$unwind": "$%s_ts" % process_field },
        { "$project": {
              "%s_id" % resource_type: "$%s_id" % resource_type,
              "value": "$%s_ts.value" % process_field,
              "date": "$%s_ts.date" % process_field,
              "updated": "$%s_ts.updated" % process_field,
              "year": {"$year": "$%s_ts.date" % process_field},
              "interval": set_field_interval("%s_ts.date" % process_field, interval)}},
        {
          "$match": {"updated": {"$lt": curr_date}}
        },
        { "$group": {"_id": {"%s_id" % resource_type: "$%s_id" % resource_type,
                    "year": "$year",
                    "interval": "$interval"}},
        },
        {"$project": {"_id": 0, "%s_id" % resource_type: "$_id.%s_id" % resource_type,
                      "year": "$_id.year", "interval": "$_id.interval"}}]

    # If last_execution is informed, add the $gte clause to the "$match" elem.
    if last_execution is not None:
        pipeline_periods[2]['$match']["updated"]['$gte'] = last_execution

    changed_intervals = []

    for changed_interval in collection.aggregate(pipeline_periods, allowDiskUse=True):
        changed_intervals.append(changed_interval)

    client.close()

    return changed_intervals


def compute_ts_aggregations(source_db_uri, source_collection, changed_intervals,
                         process_field, interval, operators_list,
                         field_names_list, resource_type):
    """
        Generation of the pipeline aggregation string, execution
        (1 aggregated result), and returns the changed_intervals
        dictionary upgraded with the result document

        Ex.

        [{"$match": {
            "metric_name":"twitter_followers",
            "company_id": "2341231231",
            "date" {"$gte": lower_date, "$lt": upper_date}}},
         {"$project": {
              "date": "$date",
              "value": "$value"}},
         {"$sort": {"date": 1}},
         {"$group": {"_id": {"company_id": "$_id"},
                     "aggr_field_name_1": { "$last": "$value" },
                     "aggr_field_name_2": { "$sum": "$value" },
                     "aggr_field_name_3": { "$avg": "$value" }}},
         {"$project":
             {"_id": 0,
              "company_id": "$_id.company_id",
              "aggr_field_name_1": "aggr_field_name_1",
              "aggr_field_name_2": "aggr_field_name_2"
              "aggr_field_name_3": "aggr_field_name_3"}}]

         Returns a result document attached to the changed_intervals document:

         [{"company_id": "2341231231", "year": 2013, "interval": 7},
            "result": {"twitter_followers_last": 456,
                       "twitter_followers_first": 123,
                       "twitter_followers_count": 34
                       ...},
          {"company_id": "2341231231", "year": 2013, "interval": 8},
             "result": {"twitter_followers_last": 135},
          {"company_id": "2341231444", "year": 2015, "interval": 9},
             "result": {"twitter_followers_last": 1023},
          {"company_id": "2341231444", "year": 2015, "interval": 11},
             "result": {"twitter_followers_last": 1050},
         ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param proces_field: field to process "twitter_followers", "twitter_bio",...
    :param changed_intervals: List of documents with the changed intervals
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param operators_list: aggregator operator list i.e. "last sum avg"
    :param field_names_list: aggregator field name list i.e
    "twitter_followers_last twitter_followers_sum twitter_followers_avg"
    :param resource_type: Resource Type (company, person, ...)
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    for changed_interval in changed_intervals:
        lower_date, upper_date = \
            interval_date_range(changed_interval, interval)

        # Base pipeline aggregation list
        pipeline_aggregated = [
            {"$match": {"metric_name": process_field,
                        "%s_id" % resource_type: changed_interval['%s_id' % resource_type],
                        "date": {"$gte": lower_date, "$lt": upper_date}}},
            {"$project": {"date": "$date",
                          "value": "$value",
                          "%s_id" % resource_type: "$%s_id" % resource_type}},
            {"$sort": {"date": 1}},
            {"$group": {"_id": {"%s_id" % resource_type: "$%s_id" % resource_type}}},
            {"$project": {"_id": 0,
                          "%s_id" % resource_type: "$_id.%s_id" % resource_type}}]

        # Upgrade pipeline aggregation list with aggregation operators
        for i in range(len(operators_list)):
            pipeline_aggregated[3]['$group'][field_names_list[i]] = \
                {"$" + operators_list[i]: "$value"} \
                    if (operators_list[i] != 'count') else {"$sum": 1}
            pipeline_aggregated[4]['$project'][field_names_list[i]] = \
                "$" + field_names_list[i]

        # Upgrade changed_interval document with result
        for aggregated_value in collection.aggregate(pipeline_aggregated, allowDiskUse=True):
            changed_interval['result'] = {}
            for i in range(len(operators_list)):
                changed_interval['result'][field_names_list[i]] = \
                    aggregated_value[field_names_list[i]]

    client.close()

    return changed_intervals


def compute_ts_aggregations_1toN(source_db_uri, source_collection,
                                 changed_intervals, process_field, date_field,
                                 interval, operators_list, field_names_list,
                                 resource_type, withinTheInterval=True):
    """
        Generation of the pipeline aggregation string, execution
        (1 aggregated result), and returns the changed_intervals
        dictionary upgraded with the result document

        Ex.

        [{"$match": {
            "company_id": "2341231231",
            "date" {"$gte": lower_date, "$lt": upper_date}}},
         {"$project": {
              "date": "$date",
              "value": "$value"}},
         {"$sort": {"date": 1}},
         {"$group": {"_id": {"company_id": "$_id"},
                     "aggr_field_name_1": { "$last": "$value" },
                     "aggr_field_name_2": { "$sum": "$value" },
                     "aggr_field_name_3": { "$avg": "$value" }}},
         {"$project":
             {"_id": 0,
              "company_id": "$_id.company_id",
              "aggr_field_name_1": "aggr_field_name_1",
              "aggr_field_name_2": "aggr_field_name_2"
              "aggr_field_name_3": "aggr_field_name_3"}}]

         Returns a result document attached to the changed_intervals document:

         [{"company_id": "2341231231", "year": 2013, "interval": 7},
            "result": {"twitter_followers_last": 456,
                       "twitter_followers_first": 123,
                       "twitter_followers_count": 34
                       ...},
          {"company_id": "2341231231", "year": 2013, "interval": 8},
             "result": {"twitter_followers_last": 135},
          {"company_id": "2341231444", "year": 2015, "interval": 9},
             "result": {"twitter_followers_last": 1023},
          {"company_id": "2341231444", "year": 2015, "interval": 11},
             "result": {"twitter_followers_last": 1050},
         ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param proces_field: field to process "twitter_followers", "twitter_bio",...
    :param changed_intervals: List of documents with the changed intervals
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param operators_list: aggregator operator list i.e. "last sum avg"
    :param field_names_list: aggregator field name list i.e
    "twitter_followers_last twitter_followers_sum twitter_followers_avg"
    :param resource_type: Resource Type (company, person, ...)
    :param withinTheInterval: within Interval
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    company_processed = 0
    for changed_interval in changed_intervals:
        lower_date, upper_date = \
            interval_date_range(changed_interval, interval)

        # Base pipeline aggregation list
        pipeline_aggregated = [
            {"$match": {"%s_id" % resource_type: changed_interval['%s_id' % resource_type],
                        date_field: {"$lt": upper_date}}},
            {"$project": {
                "id": "$%s_id" % resource_type,
                "date": "$%s" % date_field,
                "value": "$%s" % process_field}},
            {"$sort": {"date": 1}},
            {"$group": {"_id": {"id": "$id"}}},
            {"$project": {"_id": 0,
                          'id': '$_id.id'}}]

        if withinTheInterval:
            pipeline_aggregated[0]['$match'][date_field]['$gte'] = lower_date

        # Upgrade pipeline aggregation list with aggregation operators
        for i in range(len(operators_list)):
            pipeline_aggregated[3]['$group'][field_names_list[i]] = \
                {"$" + operators_list[i]: "$value"} \
                    if (operators_list[i] != 'count') else {"$sum": 1}
            pipeline_aggregated[4]['$project'][field_names_list[i]] = \
                "$" + field_names_list[i]

        # Upgrade changed_interval document with result
        for aggregated_value in collection.aggregate(pipeline_aggregated, allowDiskUse=True):
            changed_interval['result'] = {}
            for i in range(len(operators_list)):
                changed_interval['result'][field_names_list[i]] = \
                    aggregated_value[field_names_list[i]]

        company_processed += 1

        if company_processed % 100 == 0:
            print "Processed %d/%d companies" % (company_processed, len(changed_intervals))

    client.close()

    return changed_intervals


def compute_aggregations(source_db_uri, source_collection, changed_intervals,
                         process_field, interval, operators_list,
                         field_names_list, resource_type):
    """
        Generation of the pipeline aggregation string, execution
        (1 aggregated result), and returns the changed_intervals
        dictionary upgraded with the result document

        Ex.

        [{"$match": {
            "metric_name":"twitter_followers",
            "company_id": "2341231231",
            "date" {"$gte": lower_date, "$lt": upper_date}}},
         {"$project": {
              "date": "$date",
              "value": "$value"}},
         {"$sort": {"date": 1}},
         {"$group": {"_id": {"company_id": "$_id"},
                     "aggr_field_name_1": { "$last": "$value" },
                     "aggr_field_name_2": { "$sum": "$value" },
                     "aggr_field_name_3": { "$avg": "$value" }}},
         {"$project":
             {"_id": 0,
              "company_id": "$_id.company_id",
              "aggr_field_name_1": "aggr_field_name_1",
              "aggr_field_name_2": "aggr_field_name_2"
              "aggr_field_name_3": "aggr_field_name_3"}}]

         Returns a result document attached to the changed_intervals document:

         [{"company_id": "2341231231", "year": 2013, "interval": 7},
            "result": {"twitter_followers_last": 456,
                       "twitter_followers_first": 123,
                       "twitter_followers_count": 34
                       ...},
          {"company_id": "2341231231", "year": 2013, "interval": 8},
             "result": {"twitter_followers_last": 135},
          {"company_id": "2341231444", "year": 2015, "interval": 9},
             "result": {"twitter_followers_last": 1023},
          {"company_id": "2341231444", "year": 2015, "interval": 11},
             "result": {"twitter_followers_last": 1050},
         ...]

    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param proces_field: field to process "twitter_followers", "twitter_bio",...
    :param changed_intervals: List of documents with the changed intervals
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param operators_list: aggregator operator list i.e. "last sum avg"
    :param field_names_list: aggregator field name list i.e
    "twitter_followers_last twitter_followers_sum twitter_followers_avg"
    """

    client = MongoClient(source_db_uri)
    database = client.get_default_database()
    collection = database[source_collection]

    print "Processing %d changed intervals...." % len(changed_intervals)
    NUM_OF_INTERVALS_TO_INFORM = 1000
    for idx, changed_interval in enumerate(changed_intervals):
        lower_date, upper_date = \
            interval_date_range(changed_interval, interval)

        # Base pipeline aggregation list
        pipeline_aggregated = [
            {"$match": {"%s_id" % resource_type:
                            changed_interval['%s_id' %resource_type]}},
            {"$unwind": "$%s_ts" % process_field},
            { "$project": {
              "%s_id" % resource_type: "$%s_id" % resource_type,
              "value": "$%s_ts.value" % process_field,
              "date": "$%s_ts.date" % process_field,
              "updated": "$%s_ts.updated" % process_field,
              "year": {"$year": "$%s_ts.date" % process_field},
              "interval": set_field_interval(
                  "%s_ts.date" % process_field,interval)}},
            {"$match": {"date": {"$gte": lower_date, "$lt": upper_date}}},
            {"$project": {"date": "$date", "value": "$value",
                          "%s_id" % resource_type: "$%s_id" % resource_type}},
            {"$sort": {"date": 1}},
            {"$group": {"_id": {"%s_id" % resource_type: "%s_id" % resource_type}}},
            {"$project": {
                "_id": 0,
                "%s_id" % resource_type: "$_id.%s_id" % resource_type}}]

        # Upgrade pipeline aggregation list with aggregation operators
        for i in range(len(operators_list)):
            pipeline_aggregated[6]['$group'][field_names_list[i]] = \
                {"$" + operators_list[i]: "$value"} \
                    if (operators_list[i] != 'count') else {"$sum": 1}
            pipeline_aggregated[7]['$project'][field_names_list[i]] = \
                "$" + field_names_list[i]

        # Upgrade changed_interval document with result
        for aggregated_value in collection.aggregate(
                pipeline_aggregated, allowDiskUse=True):
            changed_interval['result'] = {}
            for i in range(len(operators_list)):
                changed_interval['result'][field_names_list[i]] = \
                    aggregated_value[field_names_list[i]]

        if idx != 0 and idx % NUM_OF_INTERVALS_TO_INFORM == 0:
            print "%d intervals processed" % \
               ((idx / NUM_OF_INTERVALS_TO_INFORM) * NUM_OF_INTERVALS_TO_INFORM)

    client.close()
    return changed_intervals


def prepare_fields(src_col, project_clause, resource_type, comp_aggregation,
                   interval, is_prediction):
    fields_to_insert = dict()
    res_id = "%s_id" % resource_type
    pipeline = [{"$match": {res_id: str(comp_aggregation[res_id])}},
                {"$project": project_clause}]
    for c in src_col.aggregate(pipeline, allowDiskUse=True):
        fields_to_insert = c

    fields_to_insert[res_id] = str(comp_aggregation[res_id])
    fields_to_insert['interval'] = interval
    fields_to_insert['interval_value'] = comp_aggregation['interval']
    fields_to_insert['predicted'] = is_prediction
    fields_to_insert['dummy'] = False
    fields_to_insert['date'] = interval_date_range(comp_aggregation,interval)[0]
    fields_to_insert['created'] = datetime.now()

    fields_to_update = comp_aggregation['result']
    fields_to_update['updated'] = datetime.now()
    calculated_fields(fields_to_update, interval, comp_aggregation,
                      resource_type)

    return fields_to_insert, fields_to_update


def get_find_pipeline(resource_type, comp_aggregation, interval,
                      fields_to_insert, is_prediction):
    res_id = "%s_id" % resource_type
    return  {res_id: str(comp_aggregation[res_id]),
             "interval": interval,
             "interval_value": comp_aggregation["interval"],
             "date": fields_to_insert['date'],
             "predicted": is_prediction,
             "dummy": False}


def update_preseries(target_db_uri, target_collection, source_db_uri,
                     source_collection, interval, is_prediction,
                     computed_aggregations, resource_type):
    """
        Update aggregated collection in the target system

    :param target_db_uri: Target DB URI, i.e. mongodb://localhost/databasename
    :param target_collection: Output collection name
    :param source_db_uri: Source DB URI, i.e. mongodb://localhost/databasename
    :param source_collection: Input collection name
    :param interval: period type --> year, quarter, month, dayOfYear, week
    :param is_prediction: Boolean with True or False
    :param computed_aggregations: Document with results
    """

    tgt_client = MongoClient(target_db_uri)
    tgt_db = tgt_client.get_default_database()
    tgt_col = tgt_db[target_collection]

    src_client = MongoClient(source_db_uri)
    src_db = src_client.get_default_database()
    src_col = src_db[source_collection]

    project_clause = {"_id": 0,
                      "company_name": "$company_name",
                      "company_foundation_date": "$foundation_date"}
    if resource_type == 'person':
        project_clause = {"_id": 0,
                          "first_name": "$first_name",
                          "last_name": "$last_name",
                          "gender": "$gender"}
    if resource_type == 'investor':
        project_clause = {"_id": 0,
                          "investor_name": "$investor_name",
                          "investor_foundation_date": "$foundation_date"}

    bulk_counter = 0
    full_counter = 0
    block_size = 10000
    aggregation_num = len(computed_aggregations)
    tries_num = 3
    bulk = tgt_col.initialize_ordered_bulk_op()

    while full_counter < aggregation_num:

        comp_aggregation = computed_aggregations[full_counter]

        fields_to_insert, fields_to_update = \
            prepare_fields(src_col, project_clause, resource_type,
                           comp_aggregation, interval, is_prediction)

        find_pipeline = get_find_pipeline(
            resource_type, comp_aggregation, interval,
            fields_to_insert, is_prediction)

        bulk.find(find_pipeline).upsert().update({
            "$setOnInsert": fields_to_insert, "$set": fields_to_update})

        bulk_counter += 1
        full_counter += 1

        # Manage a page of block_size records
        if bulk_counter == block_size:
            try:
                bulk.execute()
                tries_num = 3
                bulk = tgt_col.initialize_ordered_bulk_op()
                bulk_counter = 0
                print "%d records processed" % full_counter
            except BulkWriteError as ex: # give a second chance to the execute
                if tries_num == 0:
                    print "bulk.execute() failed 3 times..."
                    print "ERROR processing Task. Exception: [%s]" % ex
                    traceback.print_exc()
                    raise ex
                sleep(0.5)
                bulk = tgt_col.initialize_ordered_bulk_op()
                bulk_counter = 0
                full_counter -= block_size
                tries_num -= 1
            except Exception as ex2:
                print "ERROR processing Task. Exception: [%s]" % ex2
                traceback.print_exc()
                raise ex2

    # Manage rest of records from the latest complete page to the end
    if bulk_counter > 0:
        try:
            bulk.execute()
            print "%d records processed. Finished" % full_counter
        except BulkWriteError as ex: # give a second chance to the execute
            sleep(1)
            bulk = tgt_col.initialize_ordered_bulk_op()
            full_counter = aggregation_num - bulk_counter
            for comp_aggr_inx in range(full_counter, aggregation_num):
                comp_aggregation = computed_aggregations[comp_aggr_inx]
                if len(comp_aggregation['result']) == 0:
                    continue

                fields_to_insert, fields_to_update = \
                    prepare_fields(src_col, project_clause, resource_type,
                           comp_aggregation, interval, is_prediction)

                find_pipeline = get_find_pipeline(
                    resource_type, comp_aggregation, interval,
                    fields_to_insert, is_prediction)

                bulk.find(find_pipeline).upsert().update({
                    "$setOnInsert": fields_to_insert, "$set": fields_to_update})

                full_counter += 1

            bulk.execute()
            print "%d records processed. Finished" % full_counter
        except Exception as ex:
            print "ERROR processing Task. Exception: [%s]" % ex
            traceback.print_exc()
            raise ex

    tgt_client.close()
    src_client.close()


def get_snapshot_date(curr_date):
    """
        Calculates the snapshot_date based on a date (curr_date)

    :param curr_date: base date to calculate the snapshot_date
    """
    chg_interval_details = {
        'year': curr_date.year,
        'interval': curr_date.isocalendar()[1]
    }
    snapshot_date = interval_date_range(chg_interval_details, 'week')[1]

    if snapshot_date > curr_date:
        delta = timedelta(weeks=-1)
        curr_date = curr_date + delta
        chg_interval_details = {
            'year': curr_date.year,
            'interval': curr_date.isocalendar()[1]
        }
        snapshot_date = interval_date_range(chg_interval_details, 'week')[1]

    return snapshot_date


def get_snapshot_date_interval(curr_date, interval_type):
    """
        Depending on the interval ("year", "quarter", "month", "dayOfYear",
        "week") and the curr_date, returns the base date of the interval

    Ex.
    year     -> datetime.datetime(2015, 1, 1)
    quarter  -> datetime.datetime(2015, 4, 1)
    month    -> datetime.datetime(2015, 12, 1)
    dayOfYear-> datetime.datetime(2015, 3, 21)
    week     -> datetime.datetime(2015, 2, 14)

    :param curr_date: current_execution_date
    :param interval_type: period type --> year, quarter, month, dayOfYear, week
    """

    if interval_type == 'year':
        result = datetime(curr_date.year, 1, 1)
    elif interval_type == 'quarter':
        chg_interval_details = {
            'year': curr_date.year,
            'interval': trunc(ceil(curr_date.month / 3.0))
        }
        result = interval_date_range(chg_interval_details, interval_type)[0]
    elif interval_type == 'month':
        result = datetime(curr_date.year, curr_date.month, 1)
    elif interval_type == 'dayOfYear':
        result = datetime(curr_date.year, curr_date.month, curr_date.day)
    elif interval_type == 'week':
        chg_interval_details = {
            'year': curr_date.year,
            'interval': curr_date.isocalendar()[1]
        }
        result = interval_date_range(chg_interval_details, 'week')[1]

        if result > curr_date:
            delta = timedelta(weeks=-1)
            current_date = curr_date + delta
            chg_interval_details = {
                'year': current_date.year,
                'interval': current_date.isocalendar()[1]
            }
            result = interval_date_range(chg_interval_details, 'week')[1]
    else:
        print "Wrong interval %s" % interval_type
        return {}
    return result


def get_changed_interval_value_from_date(interval_type, date):
    chg_interval_details = {
        'year': date.year,
        'interval': None
    }

    if interval_type == 'year':
        chg_interval_details['interval'] = date.year
    elif interval_type == 'quarter':
        chg_interval_details['interval'] = trunc(ceil(date.month / 3.0))
    elif interval_type == 'month':
        chg_interval_details['interval'] = date.month
    elif interval_type == 'dayOfYear':
        chg_interval_details['interval'] = date.tm_yday
    elif interval_type == 'week':
        chg_interval_details['interval'] = date.isocalendar()[1]
    else:
        print "Wrong interval %s" % interval_type
        return {}
    return chg_interval_details


def get_next_or_previous_snapshot_date_interval(curr_date, interval_type, direction):
    """
        Returns the date before the current_date based on the defined interval

        For instance: the previous snapshot date
            for 2016-07-17 and interval type "week" will be 2016-07-10

        direction: -1 previous or 1 for next interval
    """
    current_snapshot_date = get_snapshot_date_interval(curr_date, interval_type)

    # We sure that we use -1 or 1
    direction = direction/abs(direction)

    if interval_type == 'year':
        if direction < 0:
            snapshot_date = current_snapshot_date - relativedelta.relativedelta(years=1)
        else:
            snapshot_date = current_snapshot_date + relativedelta.relativedelta(years=1)

        return get_snapshot_date_interval(snapshot_date, interval_type)
    elif interval_type == 'quarter':
        if direction < 0:
            snapshot_date = current_snapshot_date - relativedelta.relativedelta(months=4)
        else:
            snapshot_date = current_snapshot_date + relativedelta.relativedelta(months=4)

        return get_snapshot_date_interval(snapshot_date, interval_type)
    elif interval_type == 'month':
        if direction < 0:
            snapshot_date = current_snapshot_date - relativedelta.relativedelta(months=1)
        else:
            snapshot_date = current_snapshot_date + relativedelta.relativedelta(months=1)

        return get_snapshot_date_interval(snapshot_date, interval_type)
    elif interval_type == 'dayOfYear':
        if direction < 0:
            return current_snapshot_date - timedelta(days=1)
        else:
            return current_snapshot_date + timedelta(days=1)
    elif interval_type == 'week':
        if direction < 0:
            snapshot_date = current_snapshot_date - timedelta(weeks=1)
        else:
            snapshot_date = current_snapshot_date + timedelta(weeks=1)

        return get_snapshot_date_interval(snapshot_date, interval_type)
    else:
        print "Wrong interval %s" % interval_type
        return {}
