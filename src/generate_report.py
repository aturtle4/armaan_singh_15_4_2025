import pandas as pd
from src.config import DB_PATH
import sqlite3
from datetime import datetime, timedelta

def calculate_interval_status(store_polls, start_time_str, end_time_str):
    # print("Filtering polls based on the time interval")
    polls = store_polls[(store_polls["timestamp_utc"] >= start_time_str) & (store_polls["timestamp_utc"] <= end_time_str)].copy()
    polls = polls.sort_values("timestamp_utc")
    # print("Calculating the time(mins) in the interval")
    total_minutes = (datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S.%f") - datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()/60
    if polls.empty:
        return 0.0, round(total_minutes,2)
    if polls.iloc[0]["timestamp_utc"] > start_time_str:
        prev_polls = store_polls[store_polls['timestamp_utc'] < start_time_str]
        initial_status = prev_polls.iloc[-1]['status'] if not prev_polls.empty else "inactive"
        polls = pd.concat([pd.DataFrame([{
            'timestamp_utc':start_time_str,
            "status":initial_status
        }]),
        polls], ignore_index=True)
    if polls.iloc[-1]['timestamp_utc'] < end_time_str:
        polls = pd.concat([polls, pd.DataFrame([{
            'timestamp_utc':end_time_str,
            'status': polls.iloc[-1]['status']
        }])], ignore_index=True)
    
    uptime_mins, downtime_mins = 0.0, 0.0
    for i in range(len(polls)-1):
        curr_timestamp_str = polls.iloc[i]['timestamp_utc']
        next_timestamp_str = polls.iloc[i+1]['timestamp_utc']
        delta_mins = (datetime.strptime(next_timestamp_str, "%Y-%m-%d %H:%M:%S.%f") - datetime.strptime(curr_timestamp_str, "%Y-%m-%d %H:%M:%S.%f")).total_seconds() /60
        if polls.iloc[i]['status'] == 'active':
            uptime_mins += delta_mins
        else:
            downtime_mins += delta_mins
    return round(uptime_mins, 2), round(downtime_mins, 2)

def calculate_interval(menu_df, start_time_str, end_time_str, store_polls):
    total_uptime, total_downtime = 0.0, 0.0
    # print("Extracting the start and end date from the given time strings.")
    start_date = start_time_str[:10]
    end_date = end_time_str[:10]
    date_range = [(datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days +1)]
    # print("Extracted date_range : ", date_range)
    for date in date_range:
        day_of_week = datetime.strptime(date, "%Y-%m-%d").weekday()
        day_menu = menu_df[menu_df['day'] == day_of_week]
        for i in day_menu.itertuples(index = False):
            start_time, end_time = i[2], i[3]
            interval_start = f"{date} {start_time}.000000"
            interval_end = f"{date} {end_time}.000000"

            if end_time < start_time:
                next_day = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                interval_end = f"{next_day} {end_time}.000000"

            if interval_start <= end_time_str and interval_end >= start_time_str:
                effective_start = max(interval_start, start_time_str)
                effective_end = min(interval_end, end_time_str)
                if effective_start < effective_end:
                    uptime,downtime = calculate_interval_status(store_polls, effective_start,effective_end)
                    total_uptime += uptime
                    total_downtime += downtime
    return total_uptime,total_downtime


def generate_report():
    conn = sqlite3.connect(DB_PATH)
    report_rows =[]
    try:
        print("Reading database...")
        store_status_df = pd.read_sql_query("SELECT * FROM store_status", conn)
        print("Read from store_status table.")
        utc_menu_hours_df = pd.read_sql_query("SELECT * FROM utc_menu_hours", conn)
        print("Read from utc_menu_hours table.")
        #Cleaning the utc time stamp from store_status_df
        store_status_df["timestamp_utc"] = store_status_df["timestamp_utc"].str.replace(" UTC","")
        print(store_status_df)
        print("Done reading from the Database.")
        print("Setting the current time stamp to the max time_stamp from the database.")
        current_time_str = store_status_df['timestamp_utc'].max()
        print("Curr_time_stamp : ", current_time_str)
        current_time_datetimeobj = datetime.strptime(current_time_str, "%Y-%m-%d %H:%M:%S.%f")
        h1_ago_datetimeobj = current_time_datetimeobj - timedelta(hours= 1)
        d1_ago_datetimeobj = current_time_datetimeobj - timedelta(days = 1)
        w1_ago_datetimeobj = current_time_datetimeobj - timedelta(weeks = 1)

        h1_ago_str = h1_ago_datetimeobj.strftime("%Y-%m-%d %H:%M:%S.%f")
        d1_ago_str = d1_ago_datetimeobj.strftime("%Y-%m-%d %H:%M:%S.%f")
        w1_ago_str = w1_ago_datetimeobj.strftime("%Y-%m-%d %H:%M:%S.%f")
        print("Finished calculating required date_time strings")
        for store_id in store_status_df["store_id"].unique():
            print("Processing store : ", store_id)
            store_polls = store_status_df[store_status_df["store_id"] == store_id]
            store_menu = utc_menu_hours_df[utc_menu_hours_df["store_id"] == store_id]

            uptime_hour, downtime_hour, uptime_day, downtime_day, uptime_week, downtime_week = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            if store_menu.empty:
                # print("No menu hours found, Assuming working hours as 24/7.")
                uptime_hour, downtime_hour = calculate_interval_status(store_polls, h1_ago_str,current_time_str)
                uptime_day, downtime_day = calculate_interval_status(store_polls,d1_ago_str,current_time_str)
                uptime_week, downtime_week = calculate_interval_status(store_polls, w1_ago_str,current_time_str)
                # print(uptime_hour, downtime_hour, uptime_day, downtime_day, uptime_week, downtime_week)
            else:
                uptime_hour, downtime_hour = calculate_interval(store_menu, h1_ago_str,current_time_str, store_polls)
                uptime_day, downtime_day = calculate_interval(store_menu, d1_ago_str, current_time_str, store_polls)
                uptime_week, downtime_week = calculate_interval(store_menu, w1_ago_str,current_time_str, store_polls)
                # print(uptime_hour, downtime_hour, uptime_day, downtime_day, uptime_week, downtime_week)
            report_rows.append({
                "store_id": store_id,
                "uptime_last_hour(min)": round(uptime_hour, 2),
                "downtime_last_hour(min)": round(downtime_hour, 2),
                "uptime_last_day(hrs)": round(uptime_day / 60, 2),
                "downtime_last_day(hrs)": round(downtime_day / 60, 2),
                "uptime_last_week(hrs)": round(uptime_week / 60, 2),
                "downtime_last_week(hrs)": round(downtime_week / 60, 2)
            })
        report_df = pd.DataFrame(report_rows)
        # print(report_df)
        # report_df.to_sql("report", conn, if_exists="replace")
        return report_df
        
    except Exception as e:
        print("Failed to generate the report with error :", e)
    finally:
        conn.close()

if __name__ == "__main__":
    generate_report()