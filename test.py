import sqlite3
import csv

def create_tables():
    conn = sqlite3.connect('store_data.db')
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS store_polls
                      (store_id INTEGER, status TEXT, timestamp_utc TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS business_hours
                      (store_id INTEGER, day_of_week INTEGER, start_time_local TEXT, end_time_local TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS store_timezones
                      (store_id INTEGER, timezone_str TEXT)''')

    conn.commit()
    conn.close()

def import_csv_to_table(csv_file, table_name):
    conn = sqlite3.connect('store_data.db')
    cursor = conn.cursor()

    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row

        for row in reader:
            if len(row) ==2:
                cursor.execute(f'INSERT INTO {table_name} VALUES (?, ?)', row)
            elif len(row) == 3:
                cursor.execute(f'INSERT INTO {table_name} VALUES (?, ?, ?)', row)
            else:
                cursor.execute(f'INSERT INTO {table_name} VALUES (?, ?, ?, ?)', row)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_tables()

    import_csv_to_table('store_polls.csv', 'store_polls')
    import_csv_to_table('business_hours.csv', 'business_hours') #sahi
    import_csv_to_table('store_timezones.csv', 'store_timezones') #sahi
