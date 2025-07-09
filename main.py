import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text
from urllib.parse import quote_plus
from shapely.geometry import Point, LineString, Polygon
import openai
import json
from openai import OpenAI

load_dotenv()
# Initialize OpenAI client globally
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Database connection
def connect_to_pg(dbname, user, password, host, port):
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_string)

connection_params_pg = {
    'dbname': 'maritime_assets',
    'user': 'analyst_ddl',
    'password': quote_plus(os.getenv('PG_PASSWORD')),
    'host': "maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com",
    'port': '5432'
}
engine = connect_to_pg(**connection_params_pg)

# Geometry
def build_geometry(coords):
    if not coords:
        return None, None
    if len(coords) == 1:
        geom = Point(coords[0])
        geom_type = 'POINT'
    elif len(coords) == 2:
        geom = LineString(coords)
        geom_type = 'LINESTRING'
    else:
        coords.append(coords[0])
        geom = Polygon(coords)
        geom_type = 'POLYGON'
    return geom.wkt, geom_type

# Insert
def insert_message(engine, data, source_url):
    wkt_geom, geom_type = build_geometry(data.get("coordinates"))
    insert_sql = sql_text("""
        INSERT INTO sandbox.navtex_messages (
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
            "serial_number": data["serial_number"],
            "timestamp_utc": data["timestamp_utc"],
            "message_text": data["message_text"],
            "raw_text": data["raw_text"],
            "source_url": source_url,
            "error_code": "A000",
            "geom": wkt_geom,
            "geom_type": geom_type
        })
        conn.commit()

# AI parser
def ai_parse_message(raw_text):
    prompt = f"""Extract structured NAVTEX data from this message. Output valid JSON with:
- station_id (1 character)
- subject_id (1 character)
- serial_number (integer)
- timestamp_utc (ISO format or null)
- message_text (full body text)
- coordinates (list of [lon, lat] pairs, if any)

Message:
\"\"\"
{raw_text}
\"\"\"
"""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    try:
        structured = json.loads(response.choices[0].message.content)
        structured["raw_text"] = raw_text
        print (structured)
        return structured
    except Exception as e:
        print(f"❌ Failed to parse AI response: {e}")
        return None

# Main runner
def run_for_today():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive_url = f"https://www.navtex.net/Navtex_Archive/{today}/"
    print(f"📡 Fetching index: {archive_url}")

    response = requests.get(archive_url)
    if response.status_code != 200:
        print(f"❌ No index found for {today}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".txt")]

    print(f"🧾 Found {len(links)} messages")

    for link in links:
        full_url = archive_url + link
        try:
            raw_msg = requests.get(full_url).text.strip()
            parsed = ai_parse_message(raw_msg)
            if parsed:
                insert_message(engine, parsed, full_url)
                print(f"✅ Inserted: {parsed['station_id']}{parsed['subject_id']}{parsed['serial_number']}")
            else:
                print(f"⚠️ Skipped (unparsed): {link}")
        except Exception as e:
            print(f"❌ Error processing {link}: {e}")

run_for_today()
