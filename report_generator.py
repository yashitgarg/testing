import sqlite3
import csv
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import pytz
from collections import defaultdict


def get_data_from_table(table_name):
    conn = sqlite3.connect('store_data.db')
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name}')
    data = cursor.fetchall()

    # Get column names
    column_names = [description[0] for description in cursor.description]

    # Convert fetched data into a list of dictionaries
    if table_name == 'store_timezones':
        data_as_dict = [{column_names[0]: row[0], column_names[1]: row[1]} for row in data]
    else:
        data_as_dict = [dict(zip(column_names, row)) for row in data]

    conn.close()

    return data_as_dict


def generate_report(report_id, task_status):
    try:
        store_polls = get_data_from_table('store_polls')
        business_hours = get_data_from_table('business_hours')
        store_timezones = get_data_from_table('store_timezones')
        current_time = datetime(2023, 1, 20, 17, 0, 0, tzinfo=timezone.utc)

        report_data = calculate_uptime_downtime(store_polls, business_hours, store_timezones, current_time)

        # Save the report data to a CSV file
        report_filepath = get_report_filepath(report_id)
        with open(report_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                             'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])
            for row in report_data:
                writer.writerow(row)

        task_status[report_id] = "Complete"
    except Exception as e:
        print(f"Error generating report: {e}")
        task_status[report_id] = "Failed"

def calculate_uptime_downtime(store_polls, business_hours, store_timezones, current_time):
    # Create time range limits
    one_hour_ago = current_time - timedelta(hours=1)
    one_day_ago = current_time - timedelta(days=1)
    one_week_ago = current_time - timedelta(weeks=1)

    # Initialize store data
    store_data = defaultdict(lambda: defaultdict(int))

    # Create a dictionary to store timezones for each store
    store_timezones_dict = {store_id: timezone_str for store_id, timezone_str in store_timezones}

    # Process store_polls
    for store_poll in store_polls:
        store_id = store_poll['store_id']
        status = store_poll['status']
        timestamp_utc = store_poll['timestamp_utc']
        timestamp = parse(timestamp_utc).replace(tzinfo=timezone.utc)
        local_tz = pytz.timezone(store_timezones_dict.get(store_id, 'America/Chicago'))
        local_time = timestamp.astimezone(local_tz)

        if one_hour_ago <= timestamp:
            store_data[store_id][f"{status}_last_hour"] += 1
        if one_day_ago <= timestamp:
            store_data[store_id][f"{status}_last_day"] += 1
        if one_week_ago <= timestamp:
            store_data[store_id][f"{status}_last_week"] += 1

    # Calculate uptime and downtime durations
    report_data = []
    for store_id, data in store_data.items():
        intervals = {'hour': 1, 'day': 24, 'week': 168}
        row = [store_id]

        for period in intervals:
            active_intervals = data[f"active_last_{period}"]
            total_intervals = intervals[period]
            uptime_duration = int(active_intervals / total_intervals) * 60
            downtime_duration = 60 - uptime_duration
            row.extend([uptime_duration, downtime_duration])

        report_data.append(row)

    return report_data

    # Create time range limits
    one_hour_ago = current_time - timedelta(hours=1)
    one_day_ago = current_time - timedelta(days=1)
    one_week_ago = current_time - timedelta(weeks=1)

    # Initialize store data
    store_data = defaultdict(lambda: defaultdict(int))

    # Create a dictionary to store timezones for each store
    store_timezones_dict = {store_id: timezone_str for store_id, timezone_str in store_timezones}

    # Process store_polls
    for store_poll in store_polls:
        store_id = store_poll['store_id']
        status = store_poll['status']
        timestamp_utc = store_poll['timestamp_utc']
        timestamp = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f %Z').replace(tzinfo=timezone.utc)
        local_tz = pytz.timezone(store_timezones_dict.get(store_id, 'America/Chicago'))
        local_time = timestamp.astimezone(local_tz)

        if one_hour_ago <= timestamp:
            store_data[store_id][f"{status}_last_hour"] += 1
        if one_day_ago <= timestamp:
            store_data[store_id][f"{status}_last_day"] += 1
        if one_week_ago <= timestamp:
            store_data[store_id][f"{status}_last_week"] += 1

    # Calculate uptime and downtime durations
    report_data = []
    for store_id, data in store_data.items():
        intervals = {'hour': 1, 'day': 24, 'week': 168}
        row = [store_id]

        for period in intervals:
            active_intervals = data[f"active_last_{period}"]
            total_intervals = intervals[period]
            uptime_duration = int(active_intervals / total_intervals) * 60
            downtime_duration = 60 - uptime_duration
            row.extend([uptime_duration, downtime_duration])

        report_data.append(row)

    return report_data

    # Create time range limits
    one_hour_ago = current_time - timedelta(hours=1)
    one_day_ago = current_time - timedelta(days=1)
    one_week_ago = current_time - timedelta(weeks=1)

    # Initialize store data
    store_data = defaultdict(lambda: defaultdict(int))

    # Create a dictionary to store timezones for each store
    store_timezones_dict = {store_id: timezone_str for store_id, timezone_str in store_timezones}

    # Process store_polls
    for store_poll in store_polls:
        store_id = store_poll['store_id']
        status = store_poll['status']
        timestamp_utc = store_poll['timestamp_utc']
        timestamp = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f %Z')
        local_tz = pytz.timezone(store_timezones_dict.get(store_id, 'America/Chicago'))
        local_time = timestamp.replace(tzinfo=pytz.utc).astimezone(local_tz)

        if one_hour_ago <= timestamp:
            store_data[store_id][f"{status}_last_hour"] += 1
        if one_day_ago <= timestamp:
            store_data[store_id][f"{status}_last_day"] += 1
        if one_week_ago <= timestamp:
            store_data[store_id][f"{status}_last_week"] += 1

    # Calculate uptime and downtime durations
    report_data = []
    for store_id, data in store_data.items():
        intervals = {'hour': 1, 'day': 24, 'week': 168}
        row = [store_id]

        for period in intervals:
            active_intervals = data[f"active_last_{period}"]
            total_intervals = intervals[period]
            uptime_duration = int(active_intervals / total_intervals) * 60
            downtime_duration = 60 - uptime_duration
            row.extend([uptime_duration, downtime_duration])

        report_data.append(row)

    return report_data

def get_report_status(report_id, task_status):
    return task_status.get(report_id, "Not found")

def get_report_filepath(report_id):
    return f"{report_id}.csv"

