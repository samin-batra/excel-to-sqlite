from bs4 import BeautifulSoup
import requests, io, os, sqlite3
from zipfile import ZipFile
import pandas as pd


ERGAST_URL = "http://ergast.com/mrd/db/"
DB_DOWNLOAD = "http://ergast.com"
DB_COLUMNS = {
    'circuits':'circuitId int PRIMARY KEY, circuitRef text, name text, location text, country text, lat numeric, lng numeric, alt int, url text',
    'constructors':'constructorId int PRIMARY KEY, constructorRef text, name text, nationality text, url text',
    'drivers':'driverId int PRIMARY KEY, driverRef text, number int, code text, forename text, surname text, dob text',
    'seasons': 'year int PRIMARY KEY, url text',
    'races': 'raceId int PRIMARY KEY, year int , round int, circuitId int , name text, date text, time text, url text, fp1_date text, fp1_time text, fp2_date text, fp2_time text,'
             ' fp3_date text, fp3_time text, quali_date text, quali_time text, sprint_date text, sprint_time text, FOREIGN KEY(year) REFERENCES seasons(year), FOREIGN KEY(circuitId) REFERENCES circuits(circuitId)',
    'constructor_standings':'constructorStandingsId int PRIMARY KEY, raceId int , constructorId int , points numeric, position int, positionText text, wins int, FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(constructorId) REFERENCES constructors(constructorId)' ,
    'constructor_results': 'constructorResultsId int PRIMARY KEY, raceId int , constructorId int , points int, status text, FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(constructorId) REFERENCES constructors(constructorId)',
    'qualifying':'qualifyId int PRIMARY KEY, raceId int , driverId int , constructorId int, number int, position int, q1 text, q2 text, q3 text, FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId), FOREIGN KEY(constructorId) REFERENCES constructors(constructorId)',
    'sprint_results':'sprintResultId int PRIMARY KEY, raceId int , driverId int , constructorId int , number int, grid int, position int, positionText text,'
                    'positionOrder int, points numeric, laps int, milliseconds int, fastestLap int, fastestLapTime text, statusId int , FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId), FOREIGN KEY(constructorId) REFERENCES constructors(constructorId), FOREIGN KEY(statusId) REFERENCES status(statusId)',
    'status':'statusId int PRIMARY KEY, status text',
    'results':'resultId int PRIMARY KEY, raceId int , driverId int , constructorId int , number int, grid int, position int, positionText text,'
                'positionOrder int, points numeric, laps int, time text, milliseconds int, fastestLap int, rank int, fastestLapTime text, fastestLapSpeed text, statusId int ,'
              'FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId), FOREIGN KEY(constructorId) REFERENCES constructors(constructorId), FOREIGN KEY(statusId) REFERENCES status(statusId)',
    'pit_stops':'raceId int , driverId int , stop int, lap int, time text, duration text, milliseconds int,'
                'FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId)',
    'lap_times': 'raceId int, driverId int , lap int, position int,  time text, milliseconds int,'
                 'FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId)',
    'driver_standings':'driverStandingsId int PRIMARY KEY, raceId int , driverId int , points numeric, position int, positionText text, wins int,'
                       'FOREIGN KEY(raceId) REFERENCES races(raceId), FOREIGN KEY(driverId) REFERENCES drivers(driverId)'
}



def download_f1_files():
    res = requests.get(ERGAST_URL)
    webp = BeautifulSoup(res.text,'html.parser')
    db_link = webp.find(attrs={'name':'csv'})
    db_download_url = DB_DOWNLOAD + db_link['href']
    try:
        excel_files = requests.get(db_download_url,allow_redirects=True)
        z = ZipFile(io.BytesIO(excel_files.content))
        current_dir = os.path.dirname(os.path.abspath(__file__))
        z.extractall(current_dir+"/f1_files")
        return z.namelist()
    except Exception as e:
        print(e)
        print("Something went wrong!")


def feed_files_to_db():
    file_names = download_f1_files()
    print(file_names)
    connection = sqlite3.connect("f1.db")
    c = connection.cursor()
    if len(file_names)!=0:
        for name in file_names:
            table_name = name.split(".")[0]
            print(table_name)
            # c.execute(f'''CREATE TABLE {table_name} ({DB_COLUMNS[table_name]}) ''')
            data = pd.read_csv("f1_files/" + name)
            data.to_sql(table_name,connection)
            query_data = pd.read_sql(f'select * from {table_name} LIMIT 20',connection)
            print(query_data.head())

    #Create the tables



feed_files_to_db()