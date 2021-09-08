import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Bucket

myBucket = "xipcDerived"

# InfluxDB Client
myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up InfluxDB client")

# Query API
myQueryAPI = myClient.query_api()
print("Set up query API")

# Write API
myWriteAPI = myClient.write_api(write_options= SYNCHRONOUS)
print("Set up write API")

# Query 1
# Average users in the past 1 hour for instance 1
# "CurUsers"
# currentUsers

sumCurrentUsers = 0
numberofCurrentUsers = 0
averageCurrentUsers = None
query1 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables1 = myQueryAPI.query(query1)

for currentTable in allTables1:
   
    for currentRow in currentTable:
        
        # Extract currentUsers

        if(currentRow.get_measurement() == "qisInstance1" and currentRow.get_field() == "currentUsers"):
            sumCurrentUsers = sumCurrentUsers + currentRow.get_value()
            numberofCurrentUsers = numberofCurrentUsers + 1

averageCurrentUsers = int(sumCurrentUsers / numberofCurrentUsers)

print("Number of records for current users = ", numberofCurrentUsers)
print("Average number of current users for instance 1 = ", averageCurrentUsers)

# Write average to bucket

pt1 = Point("qisInstance1").tag("instanceName", "instance1").field("avgCurrentUsers", averageCurrentUsers)
myWriteAPI.write(bucket=myBucket, record=pt1)
print("Wrote 1st point to bucket")

# Query 2
# Average number of queues in the past hour for instance 1
# "CurQueues"
# currentQueues

sumCurrentQueues = 0
numberofCurrentQueues = 0
averageCurrentQueues = None
query2 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables2 = myQueryAPI.query(query2)

for currentTable2 in allTables2:

    for currentRow2 in currentTable2:

        # Extract currentQueues

        if(currentRow2.get_measurement() == "qisInstance1" and currentRow2.get_field() == "currentQueues"):
            sumCurrentQueues = sumCurrentQueues + currentRow2.get_value()
            numberofCurrentQueues = numberofCurrentQueues + 1

averageCurrentQueues = int(sumCurrentQueues / numberofCurrentQueues)

print("Number of records for current queues = ", numberofCurrentQueues)
print("Average number of current queues for instance 1 = ", averageCurrentQueues)

# Write second average to bucket

pt2 = Point("qisInstance1").tag("instanceName", "instance1").field("avgCurrentQueues", averageCurrentQueues)
myWriteAPI.write(bucket=myBucket, record=pt2)
print("Wrote 2nd point to bucket")

# Query 3
# Average number of users in the past hour for instance 2
# "CurUsers"
# currentUsers

sumCurrentUsers2 = 0
numberofCurrentUsers2 = 0
averageCurrentUsers2 = None
query3 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables3 = myQueryAPI.query(query3)

for currentTable3 in allTables3:

    for currentRow3 in currentTable3:

        # Extract current users

        if(currentRow3.get_measurement() == "qisInstance2" and currentRow3.get_field() == "currentUsers"):
            sumCurrentUsers2 = sumCurrentUsers2 + currentRow3.get_value()
            numberofCurrentUsers2 = numberofCurrentUsers2 + 1

averageCurrentUsers2 = int(sumCurrentUsers2 / numberofCurrentUsers2)

print("Number of records for current users (2) = ", numberofCurrentUsers2)
print("Average number of current users for instance 2 = ", averageCurrentUsers2)

# Write third average to bucket

pt3 = Point("qisInstance2").tag("instanceName", "instance2").field("avgCurrentUsers", averageCurrentUsers2)
myWriteAPI.write(bucket=myBucket, record=pt3)
print("Wrote 3rd point to bucket")


# Query 4
# Average number of queues in the past hour for instance 2
# "CurQueues"
# currentQueues

sumCurrentQueues2 = 0
numberofCurrentQueues2 = 0
averageCurrentQueues2 = None
query4 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables4 = myQueryAPI.query(query4)

for currentTable4 in allTables4:

    for currentRow4 in currentTable4:

        # Extract current queues

        if(currentRow4.get_measurement() == "qisInstance2" and currentRow4.get_field() == "currentQueues"):
            sumCurrentQueues2 = sumCurrentQueues2 + currentRow4.get_value()
            numberofCurrentQueues2 = numberofCurrentQueues2 + 1


averageCurrentQueues2 = int(sumCurrentQueues2 / numberofCurrentQueues2)

print("Number of records for current queues (2) = ", numberofCurrentQueues2)
print("Average number of current queues for instance 2 = ", averageCurrentQueues2)

# Write 4th point to bucket

pt4 = Point("qisInstance2").tag("instanceName", "instance2").field("avgCurrentQueues", averageCurrentQueues2)
myWriteAPI.write(bucket=myBucket, record=pt4)
print("Wrote 4th point to bucket")

# Query 5
# Average number of free nodes in the past hour for instance 1
# "FreeNCnt"
# freeNodes

sumFreeNodes = 0
numberOfFreeNodes = 0
averageFreeNodes = None
query5 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables5 = myQueryAPI.query(query5)

for currentTable5 in allTables5:

    for currentRow5 in currentTable5:

        # Extract free nodes

        if(currentRow5.get_measurement() == "qisInstance1" and currentRow5.get_field() == "freeNodes"):
            sumFreeNodes = sumFreeNodes + currentRow5.get_value()
            numberOfFreeNodes = numberOfFreeNodes + 1


averageFreeNodes = int(sumFreeNodes / numberOfFreeNodes)

print("Number of records for free nodes = ", numberOfFreeNodes)
print("Average number of free nodes for instance 1 = ", averageFreeNodes)

# Write 5th point to bucket

pt5 = Point("qisInstance1").tag("instanceName", "instance1").field("avgFreeNodes", averageFreeNodes)
myWriteAPI.write(bucket=myBucket, record=pt5)
print("Wrote 5th point to bucket")

# Query 6
# Average number of free nodes for instance 2
# "FreeNCnt"
# freeNodes

sumFreeNodes2 = 0
numberOfFreeNodes2 =0
averageFreeNodes2 = None
query6 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables6 = myQueryAPI.query(query6)

for currentTable6 in allTables6:

    for currentRow6 in currentTable6:

        # Extract free nodes

        if(currentRow6.get_measurement() == "qisInstance2" and currentRow6.get_field() == "freeNodes"):
            sumFreeNodes2 = sumFreeNodes2 + currentRow6.get_value()
            numberOfFreeNodes2 = numberOfFreeNodes2 + 1

averageFreeNodes2 = int(sumFreeNodes2 / numberOfFreeNodes2)

print("Number of records for free nodes (2) = ", numberOfFreeNodes2)
print("Average number of free nodes for instance 2 = ", averageFreeNodes2)

# Write 6th point to bucket

pt6 = Point("qisInstance2").tag("instanceName", "instance2").field("avgFreeNodes", averageFreeNodes2)
myWriteAPI.write(bucket=myBucket, record=pt6)
print("Wrote 6th point to bucket")

# Query 7
# Average number of free headers for instance 1
# "FreeHCnt"
# freeHeaders

sumFreeHeaders = 0
numberOfFreeHeaders = 0
averageFreeHeaders = None
query7 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables7 = myQueryAPI.query(query7)

for currentTable7 in allTables7:

    for currentRow7 in currentTable7:

        # Extract free headers

        if(currentRow7.get_measurement() == "qisInstance1" and currentRow7.get_field() == "freeHeaders"):
            sumFreeHeaders = sumFreeHeaders + currentRow7.get_value()
            numberOfFreeHeaders = numberOfFreeHeaders + 1

averageFreeHeaders = int(sumFreeHeaders / numberOfFreeHeaders)

print("Number of records for free headers = ", numberOfFreeHeaders)
print("Average number of free headers for instance 1 = ", averageFreeHeaders)

# Write 7th point

pt7 = Point("qisInstance1").tag("instanceName", "instance1").field("avgFreeHeaders", averageFreeHeaders)
myWriteAPI.write(bucket=myBucket, record=pt7)
print("Wrote 7th point")

# Query 8
# Average number of free headers for instance 2
# "FreeHCnt"
# freeHeaders

sumFreeHeaders2 = 0
numberOfFreeHeaders2 = 0
averageFreeHeaders2 = None
query8 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
allTables8 = myQueryAPI.query(query8)

for currentTable8 in allTables8:

    for currentRow8 in currentTable8:

        # Extract free headers

        if(currentRow8.get_measurement() == "qisInstance2" and currentRow8.get_field() == "freeHeaders"):
            sumFreeHeaders2 = sumFreeHeaders2 + currentRow8.get_value()
            numberOfFreeHeaders2 = numberOfFreeHeaders2 + 1

averageFreeHeaders2 = int(sumFreeHeaders2 / numberOfFreeHeaders2)

print("Number of records for free headers (2) = ", numberOfFreeHeaders2)
print("Average number of free headers for instance 2 = ", averageFreeHeaders2)

# Write 8th point

pt8 = Point("qisInstance2").tag("instanceName", "instance2").field("avgFreeHeaders", averageFreeHeaders2)
myWriteAPI.write(bucket=myBucket, record=pt8)
print("Wrote 8th point")


# Query 9
# Throughput
# "CountMessages"
# countMessages
# Instance 1
# Queue 1

# query9 = 'from(bucket: "simulateXIPC") |> range(start: -1h)'
# allTables9 = myQueryAPI.query(query9)

# for currentTable9 in allTables9:

#     for currentRow9 in currentTable9:

#         # Extract count messages
