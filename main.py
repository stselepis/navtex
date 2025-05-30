#parse NAVTEX messages for one day.
import requests
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from datetime import datetime
import re, os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from shapely.geometry import Point, LineString, Polygon
from sqlalchemy.sql import text as sql_text
load_dotenv()


def build_geometry(coords):
    """Determine geometry type and return WKT + type."""
    if not coords:
        return None, None

    if len(coords) == 1:
        geom = Point(coords[0])
        geom_type = 'POINT'
    elif len(coords) == 2:
        geom = LineString(coords)
        geom_type = 'LINESTRING'
    else:
        coords.append(coords[0])  # Close the polygon
        geom = Polygon(coords)
        geom_type = 'POLYGON'

    return geom.wkt, geom_type

def insert_message(engine, data, source_url):
    wkt_geom, geom_type = build_geometry(data["coordinates"])
    insert_sql = sql_text("""
        INSERT INTO navtex_messages (
            station_id, subject_id, serial_number,
            timestamp_utc, message_text, raw_text,
            source_url, error_code, geom, geom_type
        )
        VALUES (
            :station_id, :subject_id, :serial_number,
            :timestamp_utc, :message_text, :raw_text,
            :source_url, :error_code,
            ST_GeomFromText(:geom, 4326), :geom_type
        )
    """)

    with engine.connect() as conn:
        conn.execute(insert_sql, {
            "station_id": data["station_id"],
            "subject_id": data["subject_id"],
            "serial_number": data["serial"],
            "timestamp_utc": data["timestamp_utc"],
            "message_text": data["message_text"],
            "raw_text": data["raw_text"],
            "source_url": source_url,
            "error_code": "A000",
            "geom": wkt_geom,
            "geom_type": geom_type
        })
        conn.commit()


# Function to fetch data from PostgreSQL and return as DataFrame
def connect_to_pg(dbname, user, password, host, port):
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string)
    return engine

#connection parameters PostgresSQL database
connection_params_pg = {
    'dbname': 'maritime_assets',
    'user': 'analyst_ddl',
    'password': quote_plus(os.getenv('PG_PASSWORD')),
    'host': "maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com",
    'port': '5432'
}
#Create enngine
engine = connect_to_pg(**connection_params_pg)


def fetch_navtex_messages(date: str):
    """Fetch and parse NAVTEX messages for a given date (YYYY-MM-DD)."""
    url = f"https://www.navtex.net/archive/{date.replace('-', '/')}/"
    response = requests.get(url)
    if response.status_code != 200:
        print("No data available for", date)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    raw_messages = soup.find_all("pre")

    messages = []
    for pre in raw_messages:
        text = pre.text.strip()
        parsed = parse_message(text)
        if parsed:
            messages.append(parsed)

    return messages


def parse_message(raw_text):
    """Flexible parser for NAVTEX messages (GB08 style)."""
    print("\n🔍 Attempting to parse message:\n" + "-"*40)
    print(raw_text[:500])
    print("-" * 40)

    raw_text = raw_text.strip()
    lines = raw_text.splitlines()

    try:
        start_idx = next(i for i, line in enumerate(lines) if line.startswith("ZCZC"))
        end_idx = next(i for i, line in enumerate(lines) if line.strip() == "NNNN")
        message_block = "\n".join(lines[start_idx:end_idx + 1])
    except StopIteration:
        print("❌ Could not find ZCZC...NNNN block")
        return None

    print("✅ Found ZCZC-NNNN block:\n" + message_block)

    # Extract identifier like GB08
    header_match = re.match(r"ZCZC\s+([A-Z])([A-Z])(\d{2})", lines[start_idx])
    if not header_match:
        print("❌ Header match failed")
        return None

    station_id, subject_id, serial = header_match.groups()

    # Try to extract a date from message lines
    body_lines = lines[start_idx + 1:end_idx]
    timestamp = None
    for line in body_lines:
        date_match = re.search(r"(\d{1,2})\s+([A-Z]{3,9})\s+(\d{4})\s+UTC", line.upper())
        if date_match:
            day, month_str, hourmin = date_match.groups()
            try:
                timestamp = datetime.strptime(f"{day} {month_str} {hourmin}", "%d %b %H%M")
                timestamp = timestamp.replace(year=datetime.utcnow().year)
                break
            except Exception as e:
                print(f"⚠️ Timestamp parse failed: {e}")
                continue

    body_text = "\n".join(body_lines).strip()
    coords = extract_coordinates(body_text)

    return {
        "station_id": station_id,
        "subject_id": subject_id,
        "serial": int(serial),
        "timestamp_utc": timestamp,
        "message_text": body_text,
        "coordinates": coords,
        "raw_text": raw_text
    }
def extract_coordinates(text):
    """Extract DMS-style coordinates and convert to decimal."""
    coord_pattern = r"(\d{2,3})[-°](\d{2,3}\.\d+)([NS])\s+(\d{2,3})[-°](\d{2,3}\.\d+)([EW])"
    matches = re.findall(coord_pattern, text)

    coords = []
    for lat_deg, lat_min, lat_dir, lon_deg, lon_min, lon_dir in matches:
        lat = float(lat_deg) + float(lat_min) / 60
        lon = float(lon_deg) + float(lon_min) / 60
        if lat_dir == "S":
            lat *= -1
        if lon_dir == "W":
            lon *= -1
        coords.append((lon, lat))  # PostGIS expects (lon, lat)
    return coords

def run_for_today(day):
    day = datetime.utcnow().strftime("%Y-%m-%d")
    archive_url = f"https://www.navtex.net/Navtex_Archive/{today}/"
    print(f"Fetching index: {archive_url}")

    response = requests.get(archive_url)
    if response.status_code != 200:
        print(f"❌ No index found for {today}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)

    text_links = [a["href"] for a in links if a["href"].endswith(".txt")]
    if not text_links:
        print(f"⚠️ No messages found for {today}")
        return

    print(f"Found {len(text_links)} messages")

    for link in text_links:
        full_url = archive_url + link
        try:
            raw_msg = requests.get(full_url).text.strip()
            parsed = parse_message(raw_msg)
            if parsed:
                insert_message(engine, parsed, full_url)
                print(f"✅ Inserted message {parsed['station_id']}{parsed['subject_id']}{parsed['serial']}")
            else:
                print(f"⚠️ Could not parse message from: {link}")
        except Exception as e:
            print(f"❌ Error processing {link}: {e}")

from datetime import datetime, timezone
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

run_for_today(today)