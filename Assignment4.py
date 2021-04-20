import csv
import random
import mysql.connector
from faker import Faker

db = mysql.connector.connect(
        host="34.94.182.22",
        user="entriken@chapman.edu",
        passwd="FooBar!@#$",
        database="entriken_db"
    )

file = input("Enter the name of the file with .csv ...")
records = int(input("Enter the number of records that you want generated ..."))

def main():
    create_tables()
    gen_data()
    import_data()

def drop(table):
    mycursor = db.cursor()
    try:
        mycursor.execute("DROP TABLE " + table)
    except mysql.connector.errors.ProgrammingError:
        pass
    mycursor.close()

# creates tables for database schema
def create_tables():
    mycursor = db.cursor()
    drop("Location_Record_Times")
    drop("Meet_Information")
    drop("Collegiate_Runner_Information")
    drop("Collegiate_Meet_Records")
    mycursor.close()

    # -------------------- COLLEGIATE MEET RECORDS
    mycursor = db.cursor()
    mycursor.execute("CREATE TABLE Collegiate_Meet_Records "
                     "(RaceID INT,"
                     "Event VARCHAR(10),"
                     "Time TIME,"
                     "RunnerID INT UNIQUE,"
                     "MeetID INT UNIQUE,"
                     "PRIMARY KEY (RaceID))"
                     )
    mycursor.close()

    # -------------------- COLLEGIATE RUNNER INFORMATION
    mycursor = db.cursor()
    mycursor.execute("CREATE TABLE Collegiate_Runner_Information "
                     "(RunnerID INT UNIQUE,"
                     "FirstName VARCHAR(50),"
                     "LastName VARCHAR(50),"
                     "Nationality VARCHAR(50),"
                     "FOREIGN KEY (RunnerID) REFERENCES Collegiate_Meet_Records(RunnerID))"
                     )
    mycursor.close()

    # -------------------- MEET INFORMATION
    mycursor = db.cursor()
    mycursor.execute("CREATE TABLE Meet_Information "
                     "(MeetID INT UNIQUE,"
                     "Date DATE,"
                     "Location VARCHAR(100),"
                     "LocationID INT UNIQUE,"
                     "Environment VARCHAR(10),"
                     "FOREIGN KEY (MeetID) REFERENCES Collegiate_Meet_Records(MeetID))"
                     )
    mycursor.close()

    # -------------------- LOCATION RECORD TIMES
    mycursor = db.cursor()
    mycursor.execute("CREATE TABLE Location_Record_Times "
                     "(LocationID INT UNIQUE,"
                     "Location VARCHAR(100),"
                     "100m TIME,"
                     "200m TIME,"
                     "400m TIME,"
                     "FOREIGN KEY (LocationID) REFERENCES Meet_Information(LocationID))"
                     )
    mycursor.close()

# generates a time appropriate for the event
def generate_time(event, lrecord):
    if event == "100m":
        if lrecord:
            return round(random.uniform(9.50, 10.00), 2)
        return round(random.uniform(9.50, 11.00), 2)
    if event == "200m":
        if lrecord:
            return round(random.uniform(19.50, 20.00), 2)
        return round(random.uniform(19.50, 22.00), 2)
    if event == "400m":
        if lrecord:
            return round(random.uniform(43.50, 44.00), 2)
        return round(random.uniform(43.50, 47.00), 2)

# generates appropriately random data
def gen_data():
    fake = Faker()

    csv_file = open(file, "w")
    writer = csv.writer(csv_file)

    # -------------------- COLLEGIATE MEET RECORDS
    race_id = []
    events = ["100m", "200m", "400m"]
    event = []
    time = []
    runner_id = []
    meet_id = []

    # runner and meet ids
    counter = 0
    while counter < records:
        runner_id.append(counter + 1)
        meet_id.append(counter + 1)
        counter = counter + 1

    for i in range(0, records):
        race_id.append(i + 1)
        event.append(random.choice(events))
        time.append(generate_time(event[i], False))
        writer.writerow([race_id[i], event[i], time[i], runner_id[i], meet_id[i]])

    # -------------------- COLLEGIATE RUNNER INFORMATION
    for i in set(runner_id):
        writer.writerow([i, fake.first_name(), fake.last_name(), fake.country()])

    # -------------------- MEET INFORMATION
    locations = []
    for i in set(meet_id):
        locations.append(fake.city())
        writer.writerow([i, fake.date_between("-2y", "today"), locations[i-1], i, random.choice(["indoor", "outdoor"])])

    # -------------------- LOCATION RECORD TIMES
    count = 1
    for x in locations:
        writer.writerow([count, x, generate_time("100m", True), generate_time("200m", True), generate_time("400m", True)])
        count = count + 1



# imports generated data into appropriate tables
def import_data():
    mycursor = db.cursor()
    rows = []

    with open(file, "r") as csv:
        for row in csv:
            row = row.split(',')
            count = 0
            for e in row:
                row[count] = e.rstrip('\n')  # https://stackoverflow.com/questions/275018/how-can-i-remove-a-trailing-newline
                count = count + 1
            rows.append(row)

    # -------------------- COLLEGIATE MEET RECORDS
    for i in range(0, records):
        mycursor.execute("INSERT INTO Collegiate_Meet_Records(RaceID, Event, Time, RunnerID, MeetID) VALUES (%s,%s,%s,%s,%s)", (rows[i][0], rows[i][1], rows[i][2], rows[i][3], rows[i][4]));

    # -------------------- COLLEGIATE RUNNER INFORMATION
    for i in range(0, records):
        mycursor.execute("INSERT INTO Collegiate_Runner_Information(RunnerID, FirstName, LastName, Nationality) VALUES (%s,%s,%s,%s)", (rows[i+records][0], rows[i+records][1], rows[i+records][2], rows[i+records][3]));

    # -------------------- MEET INFORMATION
    for i in range(0, records):
        mycursor.execute("INSERT INTO Meet_Information(MeetID, Date, Location, LocationID, Environment) VALUES (%s,%s,%s,%s,%s)", (rows[i+records*2][0], rows[i+records*2][1], rows[i+records*2][2], rows[i+records*2][3], rows[i+records*2][4]));

    # -------------------- LOCATION RECORD TIMES
    for i in range(0, records):
        mycursor.execute("INSERT INTO Location_Record_Times(LocationID, Location, 100m, 200m, 400m) VALUES (%s,%s,%s,%s,%s)", (rows[i+records*3][0], rows[i+records*3][1], rows[i+records*3][2], rows[i+records*3][3], rows[i+records*3][4]));

    mycursor.close()
    db.commit()

main()
