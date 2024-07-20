import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd 
import math
import json
from langdetect import detect







# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1bukAvea564vEBxLeoSyQ4SIObU6RyVTx_SsQ-RF08Wc"
SAMPLE_RANGE_NAME = "A:N"






def normalize_name_vn(name):
    name = name.lower()
    prefixes = ['xã ', 'phường ', 'quận ', 'huyện ', 'thành phố ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    return name.strip()
def normalize_name_en(name):
    name = name.lower()
    suffixes = ['district', 'city', 'county', 'town', 'village']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    return name.strip()

def detect_language(name):
    try:
        return detect(name)
    except:
        return 'unknown'
    



def read_metadata():
    with open('metadata.json', 'r') as f:
        metadata = json.load(f)
    ward_to_district = {}
    district_to_city = {}
    all_wards = set()
    all_districts = set()
    all_cities = set()

    for province in metadata.values():
        city_name = province['FullName']
        city_name_en = province['FullNameEn']
        all_cities.add(city_name)
        all_cities.add(city_name_en)
        
        for district in province['District']:
            district_name = district['FullName']
            district_name_en = district['FullNameEn']
            district_to_city[district_name] = city_name
            district_to_city[district_name_en] = city_name_en
            all_districts.add(district_name)
            all_districts.add(district_name_en)
            
            for ward in district['Ward']:
                ward_name = ward['FullName']
                ward_name_en = ward['FullNameEn']
                ward_to_district[ward_name] = district_name
                ward_to_district[ward_name_en] = district_name_en
                all_wards.add(ward_name)
                all_wards.add(ward_name_en)
    return ward_to_district,district_to_city

def fill_missing_data(row,ward_to_district,district_to_city):
    lang = detect_language(row['name'])
    
    if lang == 'vi':
        if pd.isna(row['ward']) and pd.notna(row['district']):
            district_name = normalize_name_vn(row['district'])
            row['ward'] = next((ward for ward, district in ward_to_district.items()
                                if normalize_name_vn(district) == district_name or district.startswith(district_name)), None)
        
        if pd.isna(row['district']) and pd.notna(row['ward']):
            ward_name = normalize_name_vn(row['ward'])
            row['district'] = next((district for ward, district in ward_to_district.items()
                                    if normalize_name_vn(ward) == ward_name or ward.startswith(ward_name)), None)
        
        if pd.isna(row['city']) and pd.notna(row['district']):
            district_name = normalize_name_vn(row['district'])
            row['city'] = district_to_city.get(district_name, None)
    
    elif lang == 'en':
        if pd.isna(row['ward']) and pd.notna(row['district']):
            district_name = normalize_name_en(row['district'])
            row['ward'] = next((ward for ward, district in ward_to_district.items()
                                if normalize_name_vn((district) == district_name)), None)
        
        if pd.isna(row['district']) and pd.notna(row['ward']):
            ward_name = normalize_name_en(row['ward'])
            row['district'] = next((district for ward, district in ward_to_district.items()
                                    if ward == ward_name), None)
        
        if pd.isna(row['city']) and pd.notna(row['district']):
            district_name = normalize_name_en(row['district'])
            row['city'] = district_to_city.get(normalize_name_en(district_name), None)
    
    return row

class PlaceInfo:
    def __init__(self, no,place_id, lat, long, name, types, street, ward, district, city, address, phone, open_hours, link):
        self.no = no
        self.place_id = place_id
        self.lat = float(lat)
        self.long = float(long)
        self.info = [name, types, street, ward, district, city, address, phone, open_hours, link]

        
def SortRow(sheet,end):
    
    requests = [{
        "sortRange": {
            "range": {
                "sheetId": 0, 
                "startRowIndex": 2,
                "endRowIndex": end,
                "startColumnIndex": 0,
                "endColumnIndex": 20
            },
            "sortSpecs": [{
                "dimensionIndex": 2,
                "sortOrder": "ASCENDING"
            }]
        }
    }]

    batch_update_request_body = {
        'requests': requests
    }

    response = sheet.batchUpdate(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        body=batch_update_request_body
    ).execute()

def load_csv(path):
    df = pd.read_csv(path)
    ward_to_district,district_to_city= read_metadata()
    df = df.apply(fill_missing_data, axis=1,args=(ward_to_district,district_to_city))
    df = df.astype(str)
    places = [
        PlaceInfo(
            no=row['stt'],
            place_id = row['place_id'],
            lat=float(row['location_lat']),
            long=float(row['location_lng']),
            name=row['name'],
            types=row['types'],
            street=row['street'],
            ward=row['ward'],
            district=row['district'],
            city=row['city'],
            address=row['address'],
            phone=row['phone'],
            open_hours=row['open_hours'],
            link=row['link']
        )
        for index, row in df.iterrows()
    ]
    return places


def dist(p1, p2):
    return math.sqrt((p1.long - p2.long)**2 + (p1.lat - p2.lat)**2)

def find_closest_points(sheet_places, csv_places):
    def closest_strip(strip, size, d):
        min_dist = d
        strip = sorted(strip, key=lambda point: point.lat)
        p1, p2 = None, None
        for i in range(size):
            for j in range(i + 1, min(i + 7, size)):  
                if dist(strip[i], strip[j]) < min_dist:
                    min_dist = dist(strip[i], strip[j])
                    p1, p2 = strip[i], strip[j]
        return min_dist, p1, p2

    def closest_pair(points, n):
        if n <= 3:
            return bruteForce(points, n)

        mid = n // 2
        midPoint = points[mid]

        dl, left_p1, left_p2 = closest_pair(points[:mid], mid)
        dr, right_p1, right_p2 = closest_pair(points[mid:], n - mid)

        if dl < dr:
            d, p1, p2 = dl, left_p1, left_p2
        else:
            d, p1, p2 = dr, right_p1, right_p2

        strip = [points[i] for i in range(n) if abs(points[i].long - midPoint.long) < d]
        ds, sp1, sp2 = closest_strip(strip, len(strip), d)

        if ds < d:
            return ds, sp1, sp2
        else:
            return d, p1, p2

    def bruteForce(P, n):
        min_dist = float("inf")
        p1, p2 = None, None
        for i in range(n):
            for j in range(i + 1, n):
                if dist(P[i], P[j]) < min_dist:
                    min_dist = dist(P[i], P[j])
                    p1, p2 = P[i], P[j]
        return min_dist, p1, p2

    csv_places_sorted = sorted(csv_places, key=lambda place: place.long)
    sheet_places_sorted = sorted(sheet_places, key=lambda place: place.long)

    closest_points = []
    for csv_place in csv_places_sorted:
        min_dist_csv = float("inf")
        closest_place = None
        sheet_range = None 
        for sheet_place in sheet_places_sorted:
            if abs(sheet_place.long - csv_place.long) >= min_dist_csv:
                break
            distance = dist(csv_place, sheet_place)
            if distance < min_dist_csv:
                min_dist_csv = distance
                closest_place = sheet_place
                sheet_range = sheet_places.index(sheet_place) + 1 
        closest_points.append((csv_place, closest_place, min_dist_csv, sheet_range))

    return closest_points


# def insert_sorted(csv_place, sheet_place,sheet_range):
#     try:
#         append_range = sheet_range
#         if csv_place.long > sheet_place.long:
#             append_range +=1
#         elif csv_place.long == sheet_place.long:
#             if csv_place.lat > sheet_place.long:
#                 append_range +=1
#             else:
#                 append_range-=1
#         else:
#             append_range -=1
#         return append_range
#     except:
#         pass
        
    


def update_google_sheet(sheet, nearest_places):
    new_row = []
    for place in nearest_places:
        
        csv_place, sheet_place, min_dist, sheet_range = place
        if min_dist == 0:
            if csv_place.info != sheet_place.info:
                values = [csv_place.no,csv_place.place_id, csv_place.lat, csv_place.long] + csv_place.info

                body = {
                    'values': [values],
                    'majorDimension': 'ROWS',
                }
                sheet.values().update(
                    spreadsheetId=SAMPLE_SPREADSHEET_ID,
                    range=f'A{sheet_range+1}',
                    valueInputOption="USER_ENTERED",
                    body=body
                ).execute()

        else:
              
            #   range = insert_sorted(csv_place,sheet_place,sheet_range)
            #   print(range)
            #     # range += incre
              new_row.append([csv_place.no,csv_place.place_id, csv_place.lat, csv_place.long] + csv_place.info)
    
    resource = {
              "majorDimension": "ROWS",
              "values": new_row
          }
        
    sheet.values().append(
                          spreadsheetId=SAMPLE_SPREADSHEET_ID,
                          range=SAMPLE_RANGE_NAME,
                          valueInputOption='USER_ENTERED',
                          body=resource).execute()
    




def main():
 
  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
        .execute()
    )
    values = result.get("values", [])
    path = "parking_test_place_api_2024_06_17_source_gcs_test_db_parking_place_api.csv"
    new_data = load_csv(path)
    if len(values) > 1 :
        places = [
            PlaceInfo(
                no=row[0],
                place_id = row[1],
                lat=row[2],
                long=row[3],
                 name=row[4],
                types=row[5],
                street=row[6],
                ward=row[7],
                district=row[8],
                city=row[9],
                address=row[10],
                phone=row[11],
                open_hours=row[12],
                link=row[13]
            )
            for row in values[1:]  ]

       
        nearest_places = find_closest_points(places, new_data)

        update_google_sheet(sheet, nearest_places)
    else:

        data = [[csv_place.no,csv_place.place_id, csv_place.lat, csv_place.long] + csv_place.info for csv_place in new_data]
        resource = {
              "majorDimension": "ROWS",
              "values": data
          }
        
        sheet.values().append(
                          spreadsheetId=SAMPLE_SPREADSHEET_ID,
                          range=SAMPLE_RANGE_NAME,
                          valueInputOption='USER_ENTERED',
                          body=resource).execute()

    end = len(values)-1
    if end > 1:
        SortRow(sheet,end)

        
    
   
  except HttpError as err:
    print(err)








if __name__ == "__main__":
  main()