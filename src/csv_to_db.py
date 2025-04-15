import pandas as pd
import sqlite3
import os
from src.config import CSV_DIR, DB_PATH
import pytz
from datetime import datetime, timedelta

def csv_to_database(csv_file, table_name):
    df = pd.read_csv(csv_file)
    conn = sqlite3.connect(DB_PATH)
    try:
        print("Started writing to database...")
        df.to_sql(table_name, conn, if_exists = "replace", index = False)
        print("Writing to database done.")
    except Exception as e:
        print("Failed to write to Database with Error : ",e)
    finally:
        conn.close()

def UTC_time_to_local_time(time_string, time_zone):
    timezone = pytz.timezone(time_zone)
    utc_date = datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S.%f UTC")
    local_date = utc_date.replace(tzinfo=pytz.utc).astimezone(timezone)
    # print(local_date)
    return local_date

def convert_menu_hours():
    conn = sqlite3.connect(DB_PATH)
    try:
        print("Reading from database...")
        menu_hours_df = pd.read_sql_query("SELECT * FROM menu_hours", conn)
        print("Done reading menu_hours table")
        # store_status_df = pd.read_sql_query("SELECT * FROM store_status", conn)
        # print("Done reading store_status table")
        timezones_df = pd.read_sql_query("SELECT * FROM timezones", conn)
        print("Done reading timezones table")
        print("Done reading from Database.")
        print("Converting local time to UTC time")
        updated_rows =[]
        for i in menu_hours_df.itertuples(index=False):
            # print(i)
            store_id = i[0]
            start_time_local = i[2]
            end_time_local = i[3]
            day_of_week = i[1]
            # Get corresponding timezone
            timezone_row = timezones_df[timezones_df['store_id'] == store_id]
            timezone_str = timezone_row.iloc[0]['timezone_str'] if not timezone_row.empty else "America/Chicago"
            local_tz = pytz.timezone(timezone_str)
                
            reference_date = datetime(2024, 12, 2)  # Monday
            local_start = local_tz.localize(
                datetime.combine(reference_date + timedelta(days=day_of_week), datetime.strptime(start_time_local, "%H:%M:%S").time())
            )
            local_end = local_tz.localize(
                datetime.combine(reference_date + timedelta(days=day_of_week), datetime.strptime(end_time_local, "%H:%M:%S").time())
            )

            # Convert to UTC
            start_time_utc = local_start.astimezone(pytz.utc).time()
            end_time_utc = local_end.astimezone(pytz.utc).time()

            updated_rows.append({
                "store_id": store_id,
                "day": day_of_week,
                "start_time_utc": start_time_utc.strftime("%H:%M:%S"),
                "end_time_utc": end_time_utc.strftime("%H:%M:%S")
            })

        utc_menu_hours_df = pd.DataFrame(updated_rows)
        print("Conversion complete. Writing to database...")

        utc_menu_hours_df.to_sql("utc_menu_hours", conn, if_exists="replace", index=False)
        print("Writing to database done.")

    except Exception as e:
        print("Failed to convert menu hours with error:", e)
    finally:
        conn.close()




def update_Database():
    table1 = "menu_hours"
    table2 = "store_status"
    table3 = "timezones"
    csv_to_database("../CsvFiles/menu_hours.csv",table1)
    csv_to_database("../CsvFiles/store_status.csv",table2)
    csv_to_database("../CsvFiles/timezones.csv",table3)


if __name__ == "__main__":
    update_Database()
    convert_menu_hours()