import requests
import json
import os
import csv
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Station IDs for different cities
STATIONS = {
    'Hanoi': '1583',
    'Bangkok': '5773',
    'Delhi': '2554',
    'Mumbai': '12454',
    'Seoul': '5508',
    'Beijing': '1451',
    'Shanghai': '1437',
    'Shinjuku (Tokyo)': '2289',
    'Osaka': '5543',
    'Paris': '5722',
    'London': '5724',

    'Frankfurt': '10842', ###

    'Hamburg': '6125',
    'Moscow': '13618',

    ###'Montreal (Canada)': '5922',###
    ###'Toronto': '5914',
    ###'Chengdu': '1450',
    'Wuhan': '1529',
    ###'Michigan (USA)': '5322',###
    ###'Chiang Mai': '5775',
    ###'Saitama-ken': '11530',
    ###'Prague': '2706',

    'Warsaw (Poland)': '3399',

    ###'Helsinki': '5717',###

    'Winnellie (Australia)': '6442',
    'Bangalore (India)': '3758',

    'Chennai (India)': '13739',###

    'Boston': '3577',
    'Georgia': '3906',

    'Phoenix': '5944',
    'Hyderabad (India)': '14125',
    'Denver': '6323',
    'Daegu': '5523',
    'Gyeonggi': '1696',
    'Nagoya': '5540',
    'Fukuoka': '5551',
    'Kawasaki': '5580',
    'Shenzhen': '1539',
    'Zhongshan (Taiwan)': '1597',
    'Chongqing': '1453',
    'Istanbul (Turkey)': '4151',
    ###'Sihhiye (Turkey)': '13190',
    
}

OUTPUT_FILE = "aqi_data.csv"

def backup_csv_file():
    """Tạo bản sao dự phòng file CSV"""
    if os.path.exists(OUTPUT_FILE):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = f"{backup_dir}/aqi_data_{timestamp}.csv"
        shutil.copy(OUTPUT_FILE, backup_file)
        print(f"🛟 Backup created: {backup_file}")

def load_existing_records():
    """Load các bản ghi đã có để tránh ghi trùng"""
    existing = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing.add((row['datetime'], row['city']))
    return existing

def crawl_city(city, station_id, token, existing_records):
    try:
        url = f"https://api.waqi.info/feed/@{station_id}/?token={token}"
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            result = response.json()
            if result['status'] == 'ok' and result.get('data'):
                parsed = parse_aqi_response(result['data'], city)
                if parsed:
                    key = (parsed['datetime'], parsed['city'])
                    if key not in existing_records:
                        print(f"✅ Crawled {city}")
                        return parsed
                    else:
                        print(f"⚠️ Skipped (duplicate) {city}")
            else:
                print(f"❌ API error for {city}: {result.get('data')}")
    except Exception as e:
        print(f"❌ Failed {city}: {e}")
    return None

def crawl_aqi_data():
    """Crawl AQI data từ AQICN API"""
    token = os.getenv('AQICN_TOKEN')
    if not token:
        raise RuntimeError("Missing AQICN_TOKEN environment variable")

    existing_records = load_existing_records()
    all_data = []

    # Thêm timestamp cho dễ debug
    print(f"🚀 Starting crawl at {datetime.now()}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(crawl_city, city, sid, token, existing_records)
            for city, sid in STATIONS.items()
        ]
        for future in futures:
            result = future.result()
            if result:
                all_data.append(result)
    
    print(f"📊 Summary: {len(all_data)} new records, {len(existing_records)} total existing")

    return all_data

def parse_aqi_response(data, city_name):
    try:
        iaqi = data["iaqi"]
        time = data["time"]["iso"]

        return {
            "datetime": time,
            "city": city_name,
            "lat": data["city"]["geo"][0],
            "lon": data["city"]["geo"][1],
            "aqi_hour": data["aqi"],
            "dominantpol_aqi": data.get("dominentpol"),

            # Pollutants
            "pm25": iaqi.get("pm25", {}).get("v"),
            "pm10": iaqi.get("pm10", {}).get("v"),
            "no2": iaqi.get("no2", {}).get("v"),
            "so2": iaqi.get("so2", {}).get("v"),
            "co": iaqi.get("co", {}).get("v"),
            "o3": iaqi.get("o3", {}).get("v"),

            # Weather
            "temperature": iaqi.get("t", {}).get("v"),
            "humidity": iaqi.get("h", {}).get("v"),
            "pressure": iaqi.get("p", {}).get("v"),
            "wind_speed": iaqi.get("w", {}).get("v"),
            "wind_gust": iaqi.get("wg", {}).get("v"),
            "dew_point": iaqi.get("dew", {}).get("v"),
        }

    except Exception as e:
        print(f"[ERROR] Parsing error for {city_name}: {e}")
        return None

def save_to_csv(data_list):
    if not data_list:
        return

    file_exists = os.path.isfile(OUTPUT_FILE)

    with open(OUTPUT_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data_list[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(data_list)

def main():
    all_data = crawl_aqi_data()
    if all_data:
        backup_csv_file()
        save_to_csv(all_data)
        print(f"✅ Saved {len(all_data)} records at {datetime.now()}")
    else:
        print("ℹ️ No new data to save.")

if __name__ == "__main__":
    main()
