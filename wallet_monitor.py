import sqlite3
import time
from datetime import datetime, timedelta
from termcolor import colored

def fetch_wallet_data():
    query_current_earnings = """
    SELECT wallet_id, unclaimed_earnings
    FROM wallet_updates
    ORDER BY updated_at DESC
    LIMIT 1
    """

    query_twenty_four_hour_earnings = """
    WITH PreviousEarnings AS (
        SELECT 
            w1.id,
            w1.wallet_id,
            w1.unclaimed_earnings AS current_earnings,
            w1.updated_at AS current_time,
            MAX(w2.updated_at) AS previous_time,
            w2.unclaimed_earnings AS previous_earnings
        FROM 
            wallet_updates w1
        LEFT JOIN 
            wallet_updates w2 
        ON 
            w1.wallet_id = w2.wallet_id 
            AND w2.updated_at < w1.updated_at
        WHERE 
            w1.updated_at >= datetime('now', '-24 hours')
        GROUP BY 
            w1.id, w1.wallet_id
    )
    SELECT 
        wallet_id,
        COALESCE(SUM(current_earnings - COALESCE(previous_earnings, 0)), 0) AS running_24h_earnings
    FROM 
        PreviousEarnings
    GROUP BY 
        wallet_id;
    """

    query_ten_day_earnings = """
    WITH TenDayEarnings AS (
        SELECT 
            wallet_id,
            STRFTIME('%Y-%m-%d', updated_at) AS date,
            COALESCE(SUM(unclaimed_earnings), 0) AS earnings
        FROM 
            wallet_updates
        WHERE 
            updated_at >= datetime('now', '-10 days')
        GROUP BY 
            wallet_id, date
    )
    SELECT 
        date,
        SUM(earnings) AS total_earnings
    FROM 
        TenDayEarnings
    GROUP BY 
        date;
    """

    conn = sqlite3.connect('wallet_monitor.db')
    cursor = conn.cursor()

    cursor.execute(query_current_earnings)
    current_earnings = cursor.fetchone()

    cursor.execute(query_twenty_four_hour_earnings)
    twenty_four_hour_earnings = cursor.fetchall()

    cursor.execute(query_ten_day_earnings)
    ten_day_earnings = cursor.fetchall()

    conn.close()

    return current_earnings, twenty_four_hour_earnings, ten_day_earnings

def display_table(current_earnings, twenty_four_hour_earnings):
    print("=" * 80)
    print("Current Unclaimed Earnings:")
    print("-" * 80)
    print("| {:<15} | {:<20} |".format("Wallet ID", "Unclaimed Earnings"))
    print("-" * 80)
    if current_earnings:
        print("| {:<15} | {:<20.4f} |".format(current_earnings[0], current_earnings[1]))
    else:
        print("| {:<15} | {:<20} |".format("N/A", "N/A"))
    print("-" * 80)

    print("\n24-Hour Earnings:")
    print("-" * 80)
    print("| {:<15} | {:<20} |".format("Wallet ID", "Total Earnings"))
    print("-" * 80)
    for row in twenty_four_hour_earnings:
        print("| {:<15} | {:<20.4f} |".format(row[0], row[1]))
    print("-" * 80)

def display_histogram(ten_day_earnings):
    print("\nHistogram (Target: 1.3 per day):")
    print("-" * 80)
    daily_target = 1.3
    for date, earnings in ten_day_earnings:
        progress = min(earnings / daily_target, 1.0)
        histogram = colored("#" * int(progress * 50), 'green') if progress >= 1.0 else "#" * int(progress * 50)
        print("{:<15}: {:<50} {:.2f}/1.3".format(date, histogram, earnings))

def countdown(seconds):
    while seconds > 0:
        mins, secs = divmod(seconds, 60)
        time_format = f"Next update in {mins:02d}:{secs:02d}"
        print(time_format.center(80), end="\r")
        time.sleep(1)
        seconds -= 1

def main():
    while True:
        current_earnings, twenty_four_hour_earnings, ten_day_earnings = fetch_wallet_data()
        display_table(current_earnings, twenty_four_hour_earnings)
        display_histogram(ten_day_earnings)
        countdown(300)  # 5 minutes countdown
        print("\nRefreshing...\n")

if __name__ == "__main__":
    main()
