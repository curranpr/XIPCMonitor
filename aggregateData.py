import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import date

bucketToWriteTo = "xipcAggregate2"

# Set up client

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org= "Envoy Tech")
print("Set up client")

# Set up write API

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

# Set up query API

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query 1
# Window: 1 hour
# Measurement: "qisInstance1"
# Field: currentUsers

query1 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables1 = myQueryAPI.query(query1)

df1 = pd.DataFrame()
list1 = list()
list2 = list()
list3 = list()
list4 = list()

for currentTable1 in allTables1:

    for currentRow1 in currentTable1:

        if(currentRow1.get_measurement() == "qisInstance1" and currentRow1.get_field() == "currentUsers"):
            list1.append(currentRow1.get_start())
            list2.append(currentRow1.get_stop())
            list3.append(currentRow1.get_measurement())
            list4.append(currentRow1.get_value())

df1["start_time"] = list1
df1["end_time"] = list2
df1["measurement"] = list3
df1["current_users"] = list4

# Compute count

query1Count = df1.count()
print("Query 1 count (overall) = ", query1Count)

# Compute mean of "current_users"

query1MeanCurrentUsers = df1["current_users"].mean()
query1MeanCurrentUsers = int(query1MeanCurrentUsers)
print("Query 1 mean of current users = ", query1MeanCurrentUsers)

# Write(s)
# Mean of current users, instance 1
# query1MeanCurrenUsers

pt1 = Point("queinfosys").tag("instanceName", "instance1").field("meanCurrentUsers", query1MeanCurrentUsers)
myWriteAPI.write(bucket=bucketToWriteTo, record=pt1)

# Compute daily count
# Group by "start_time"
# "count" on current_users

groupBy1 = df1.groupby("start_time", as_index=False)["current_users"].count()

print("1st group by (daily count)")
print(groupBy1)

# Print date, count of current_users mapping

for i in range(len(groupBy1)):
    print("Date: ", groupBy1["start_time"].loc[i], "Count: ", groupBy1["current_users"].loc[i])

# Compute daily mean
# Group by "start_time"
# "mean" on current_users

groupBy2 = df1.groupby("start_time", as_index=False)["current_users"].mean()
groupBy2Copy = groupBy2.copy()

for j in range(len(groupBy2)):
    groupBy2Copy["current_users"].loc[j] = int(groupBy2["current_users"].loc[j])

print("2nd group by (daily mean)")
print(groupBy2Copy)

# Print date, mean of current_users mapping

for i in range(len(groupBy2Copy)):
    print("Date: ", groupBy2Copy["start_time"].loc[i], "Mean: ", groupBy2Copy["current_users"].loc[i])

# Write(s)
# Daily mean current users, instance 1
# groupBy2Copy

for i in range(len(groupBy2Copy)):
    dmCurrent1 = groupBy2Copy["current_users"].loc[i]
    pt3 = Point("queinfosys").tag("instanceName", "instance1").field("dailyMeanCurrentUsers", dmCurrent1).time(groupBy2Copy["start_time"].loc[i])
    myWriteAPI.write(bucket=bucketToWriteTo, record=pt3)

# Compute min of "current_users"

df1Copy = df1.copy()

query1MinCurrentUsers = df1Copy["current_users"].min()
print("Query 1 min of current users = ", query1MinCurrentUsers)

# Write(s)
# Daily min current users, instance 1
# query1MinCurrentUsers

newPointMinCurrentUsers1 = Point("queinfosys").tag("instanceName", "instance1").field("minCurrentUsers", int(query1MinCurrentUsers))
myWriteAPI.write(bucket=bucketToWriteTo, record=newPointMinCurrentUsers1)

# Compute daily min
# Group by "start_time"
# "min" on current_users

groupBy3 = df1.groupby("start_time", as_index=False)["current_users"].min()

print("3rd group by (daily min)")
print(groupBy3)

# Print date, min of current_users mapping

for i in range(len(groupBy3)):
    print("Date: ", groupBy3["start_time"].loc[i], "Min: ", groupBy3["current_users"].loc[i])

# Compute max of "current_users"

query1MaxCurrentUsers = df1["current_users"].max()
print("Query 1 max of current users = ", query1MaxCurrentUsers)

# Write(s)
# Max of current users, instance 1
# query1MaxCurrentUsers

ptMCU1 = Point("queinfosys").tag("instanceName", "instance1").field("maxCurrentUsers", int(query1MaxCurrentUsers))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMCU1)

# Compute daily max
# Group by "start_time"
# "max" on current_users

groupBy4 = df1.groupby("start_time", as_index=False)["current_users"].max()

print("4th group by (daily max)")
print(groupBy4)

# Print date, max of current_users mapping

for i in range(len(groupBy4)):
    print("Date: ", groupBy4["start_time"].loc[i], "Max: ", groupBy4["current_users"].loc[i])


# Query 2
# Window: 1 hour
# Measurement: "qisInstance2"
# Field: currentUsers

query2 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables2 = myQueryAPI.query(query2)

df2 = pd.DataFrame()
list1a = list()
list2a = list()
list3a = list()
list4a = list()

for currentTable2 in allTables2:

    for currentRow2 in currentTable2:

        # Extract "currentUsers"

        if(currentRow2.get_measurement() == "qisInstance2" and currentRow2.get_field() == "currentUsers"):

            list1a.append(currentRow2.get_start())
            list2a.append(currentRow2.get_stop())
            list3a.append(currentRow2.get_measurement())
            list4a.append(currentRow2.get_value())


df2["start_time"] = list1a
df2["end_time"] = list2a
df2["measurement"] = list3a
df2["current_users"] = list4a

# Compute count (overall)

query2Count = df2.count()
print("Query 2 count (overall)")
print(query2Count)

# Compute mean of current_users

query2MeanCurrentUsers = int(df2["current_users"].mean())
print("Query 2 mean of current users = ", query2MeanCurrentUsers)

# Write(s)
# Mean of current users, instance 2
# query2MeanCurrentUsers

pt2 = Point("queinfosys").tag("instanceName", "instance2").field("meanCurrentUsers", query2MeanCurrentUsers)
myWriteAPI.write(bucket=bucketToWriteTo, record=pt2)

# Compute daily count of current_users

groupBy1a = df2.groupby("start_time", as_index=False)["current_users"].count()
print("Group by 1a")
print(groupBy1a)

# Print date, daily count of current_users mapping

for i in range(len(groupBy1a)):
    print("Date: ", groupBy1a["start_time"].loc[i], "Daily count: ", groupBy1a["current_users"].loc[i])

# Compute daily mean of current_users

groupBy2a = df2.groupby("start_time", as_index=False)["current_users"].mean()
groupBy2aCopy = groupBy2a.copy()

for i in range(len(groupBy2aCopy)):
    groupBy2aCopy["current_users"].loc[i] = int(groupBy2a["current_users"].loc[i])

print("Group by 2a")
print(groupBy2aCopy)

# Print date, daily mean of current_users mapping

for i in range(len(groupBy2aCopy)):
    print("Date: ", groupBy2aCopy["start_time"].loc[i], "Daily mean: ", groupBy2aCopy["current_users"].loc[i])

# Write(s)
# Daily mean current users, instance 2
# groupBy2aCopy

for i in range(len(groupBy2aCopy)):
    dmCurrent2 = groupBy2aCopy["current_users"].loc[i]
    dmCurrent2Time = groupBy2aCopy["start_time"].loc[i]
    pt4 = Point("queinfosys").tag("instanceName", "instance2").field("dailyMeanCurrentUsers", dmCurrent2).time(dmCurrent2Time)
    myWriteAPI.write(bucket=bucketToWriteTo, record=pt4)

# Compute min of current_users

query2MinCurrentUsers = df2["current_users"].min()
print("Query 2 min of current users = ", query2MinCurrentUsers)

# Write(s)
# Min current users, instance2
# query2MinCurrentUsers

newPointMinCurrentUsers2 = Point("queinfosys").tag("instanceName", "instance2").field("minCurrentUsers", int(query2MinCurrentUsers))
myWriteAPI.write(bucket=bucketToWriteTo, record=newPointMinCurrentUsers2)

# Compute daily min of current_users

groupBy3a = df2.groupby("start_time", as_index=False)["current_users"].min()
print("Group by 3a")
print(groupBy3a)

# Print date, daily min of current_users mapping

for i in range(len(groupBy3a)):
    print("Date: ", groupBy3a["start_time"].loc[i], "Daily min: ", groupBy3a["current_users"].loc[i])

# Compute max of current_users

query2MaxCurrentUsers = df2["current_users"].max()
print("Query 2 max of current users = ", query2MaxCurrentUsers)

# Write(s)
# Max current users, instance 2
# query2MaxCurrentUsers

ptMCU2 = Point("queinfosys").tag("instanceName", "instance2").field("maxCurrentUsers", int(query2MaxCurrentUsers))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMCU2)

# Compute daily max of current_users

groupBy4a = df2.groupby("start_time", as_index=False)["current_users"].max()
print("Group by 4a")
print(groupBy4a)

# Print date, daily max of current_users mapping

for i in range(len(groupBy4a)):
    print("Date: ", groupBy4a["start_time"].loc[i], "Daily max: ", groupBy4a["current_users"].loc[i])

# Query 3
# Window: 1 hour
# Measurement: "qisInstance1"
# Field: currentQueues

query3 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables3 = myQueryAPI.query(query3)

df3 = pd.DataFrame()
listA = list()
listB = list()
listC = list()
listD = list()

for currentTable3 in allTables3:

    for currentRow3 in currentTable3:

        # Extract currentQueues

        if(currentRow3.get_measurement() == "qisInstance1" and currentRow3.get_field() == "currentQueues"):
            listA.append(currentRow3.get_start())
            listB.append(currentRow3.get_stop())
            listC.append(currentRow3.get_measurement())
            listD.append(currentRow3.get_value())

df3["start_time"] = listA
df3["end_time"] = listB
df3["measurement"] = listC
df3["current_queues"] = listD

# Compute count (overall)

query3Count = df3.count()
print("Query 3 count")
print(query3Count)

# Compute mean current_queues

query3MeanCurrentQueues = df3["current_queues"].mean()
query3MeanCurrentQueues = int(query3MeanCurrentQueues)
print("Query 3 mean current_queues = ", query3MeanCurrentQueues)

# Write(s)
# Mean current queues, instance 1
# query3MeanCurrentQueues

ptMCQ1 = Point("queinfosys").tag("instanceName", "instance1").field("meanCurrentQueues", query3MeanCurrentQueues)
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMCQ1)

# Compute daily count of current_queues

groupBy5 = df3.groupby("start_time", as_index=False)["current_queues"].count()
print("Group by 5")
print(groupBy5)

# Print date, daily count of current_queues mapping

for i in range(len(groupBy5)):
    print("Date: ", groupBy5["start_time"].loc[i], "Daily count: ", groupBy5["current_queues"].loc[i])

# Compute daily mean of current_queues

groupBy6 = df3.groupby("start_time", as_index=False)["current_queues"].mean()
groupBy6Copy = groupBy6.copy()

for i in range(len(groupBy6)):
    groupBy6Copy["current_queues"].loc[i] = int(groupBy6["current_queues"].loc[i])

print("Group by 6")
print(groupBy6Copy)

# Print date, daily mean of current_queues mapping

for i in range(len(groupBy6Copy)):
    print("Date: ", groupBy6Copy["start_time"].loc[i], "Daily mean: ", groupBy6Copy["current_queues"].loc[i])

# Write(s)
# Daily mean for current queues, instance 1
# groupBy6Copy

for i in range(len(groupBy6Copy)):
    dmCurrentQueues1 = groupBy6Copy["current_queues"].loc[i]
    dmCurrentQueues1Time1 = groupBy6Copy["start_time"].loc[i]

    ptDMQ1 = Point("queinfosys").tag("instanceName", "instance1").field("dailyMeanCurrentQueues", dmCurrentQueues1).time(dmCurrentQueues1Time1)
    myWriteAPI.write(bucket=bucketToWriteTo, record=ptDMQ1)

# Compute min of current_queues

query3MinCurrentQueues = df3["current_queues"].min()
print("Query 3 min of current queues = ", query3MinCurrentQueues)

# Write(s)
# Min for current queues, instance 1
# query3MinCurrentQueues

ptMinCQ1 = Point("queinfosys").tag("instanceName", "instance1").field("minCurrentQueues", int(query3MinCurrentQueues))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMinCQ1)

# Compute daily min of current_queues

groupBy7 = df3.groupby("start_time", as_index=False)["current_queues"].min()
print("Group by 7")
print(groupBy7)

# Print date, daily min of current_queues mapping

for i in range(len(groupBy7)):
    print("Date: ", groupBy7["start_time"].loc[i], "Daily min: ", groupBy7["current_queues"].loc[i])

# Compute max of current_queues

query3MaxCurrentQueues = df3["current_queues"].max()
print("Query 3 max of current queues = ", query3MaxCurrentQueues)

# Write(s)
# Max for current queues, instance 1
# query3MaxCurrentQueues

ptMaxCQ1 = Point("queinfosys").tag("instanceName", "instance1").field("maxCurrentQueues", int(query3MaxCurrentQueues))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMaxCQ1)

# Compute daily max of current_queues

groupBy8 = df3.groupby("start_time", as_index=False)["current_queues"].max()
print("Group by 8")
print(groupBy8)

# Print date, daily max of current_queues mapping

for i in range(len(groupBy8)):
    print("Date: ", groupBy8["start_time"].loc[i], "Daily max: ", groupBy8["current_queues"].loc[i])


# Query 4
# Window: 1 hour
# Measurement: "qisInstance2"
# Field: currentQueues

query4 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables4 = myQueryAPI.query(query4)

df4 = pd.DataFrame()
list_a = list()
list_b = list()
list_c = list()
list_d = list()

for currentTable4 in allTables4:

    for currentRow4 in currentTable4:

        # Extract current queues

        if(currentRow4.get_measurement() == "qisInstance2" and currentRow4.get_field() == "currentQueues"):
            list_a.append(currentRow4.get_start())
            list_b.append(currentRow4.get_stop())
            list_c.append(currentRow4.get_measurement())
            list_d.append(currentRow4.get_value())

df4["start_time"] = list_a
df4["end_time"] = list_b
df4["measurement"] = list_c
df4["current_queues"] = list_d

# Compute count overall

query4Count = df4.count()
print("Count for query 4 = ", query4Count)

# Compute mean of current_queues

query4MeanCurrentQueues = df4["current_queues"].mean()
query4MeanCurrentQueues = int(query4MeanCurrentQueues)
print("Mean of current_queues = ", query4MeanCurrentQueues)

# Write(s)
# Mean current queues, instance 2
# query4MeanCurrentQueues

ptMCQ2 = Point("queinfosys").tag("instanceName", "instance2").field("meanCurrentQueues", query4MeanCurrentQueues)
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMCQ2)

# Compute daily count current_queues

dailyCountCurrentQueues = df4.groupby("start_time", as_index=False)["current_queues"].count()
print("Daily count current queues")
print(dailyCountCurrentQueues)

# Print date, daily count of current queues mapping

for i in range(len(dailyCountCurrentQueues)):
    print("Date: ", dailyCountCurrentQueues["start_time"].loc[i], "Daily count: ", dailyCountCurrentQueues["current_queues"].loc[i])

# Compute daily mean current_queues

dailyMeanCurrentQueues = df4.groupby("start_time", as_index=False)["current_queues"].mean()
dailyMeanCurrentQueuesCopy = dailyMeanCurrentQueues.copy()

for i in range(len(dailyMeanCurrentQueues)):
    dailyMeanCurrentQueuesCopy["current_queues"].loc[i] = int(dailyMeanCurrentQueues["current_queues"].loc[i])

print("Daily mean current_queues")
print(dailyMeanCurrentQueuesCopy)

# Print date, daily mean current_queues mapping

for i in range(len(dailyMeanCurrentQueuesCopy)):
    print("Date: ", dailyMeanCurrentQueuesCopy["start_time"].loc[i], "Daily mean: ", dailyMeanCurrentQueuesCopy["current_queues"].loc[i])

# Write(s)
# Daily mean current queues, instance 2
# dailyMeanCurrentQueuesCopy

for i in range(len(dailyMeanCurrentQueuesCopy)):
    dMCQC = dailyMeanCurrentQueuesCopy["current_queues"].loc[i]
    dMCQCTime = dailyMeanCurrentQueuesCopy["start_time"].loc[i]

    ptDMCQC = Point("queinfosys").tag("instanceName", "instance2").field("dailyMeanCurrentQueues", dMCQC).time(dMCQCTime)
    myWriteAPI.write(bucket=bucketToWriteTo, record=ptDMCQC)

# Compute min current_queues

minCurrentQueues = df4["current_queues"].min()
print("Min of current queues = ", minCurrentQueues)

# Write(s)
# Write min for current queues, instance 2
# minCurrentQueues

ptMinCQ2 = Point("queinfosys").tag("instanceName", "instance2").field("minCurrentQueues", int(minCurrentQueues))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMinCQ2)

# Compute daily min current_queues

dailyMinCurrentQueues = df4.groupby("start_time", as_index=False)["current_queues"].min()
print("Daily min current_queues")
print(dailyMinCurrentQueues)

# Print date, daily min current_queues mapping

for i in range(len(dailyMinCurrentQueues)):
    print("Date: ", dailyMinCurrentQueues["start_time"].loc[i], "Daily min: ", dailyMinCurrentQueues["current_queues"].loc[i])

# Compute max current_queues

maxCurrentQueues = df4["current_queues"].max()
print("Max current_queues = ", maxCurrentQueues)

# Write(s)
# Max for current queues, instance 2
# maxCurrentQueues

ptMCQ2 = Point("queinfosys").tag("instanceName", "instance2").field("maxCurrentQueues", int(maxCurrentQueues))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMCQ2)

# Compute daily max current_queues

dailyMaxCurrentQueues = df4.groupby("start_time", as_index=False)["current_queues"].max()
print("Daily max current_queues")
print(dailyMaxCurrentQueues)

# Print date, daily max current_queues mapping

for i in range(len(dailyMaxCurrentQueues)):
    print("Date: ", dailyMaxCurrentQueues["start_time"].loc[i], "Daily max: ", dailyMaxCurrentQueues["current_queues"].loc[i])

# Query 5
# Window: 1 hour
# Measurement: "qisInstance1"
# Field: freeNodes

query5 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables5 = myQueryAPI.query(query5)

df5 = pd.DataFrame()
listq5_1 = list()
listq5_2 = list()
listq5_3 = list()
listq5_4 = list()

for currentTable5 in allTables5:

    for currentRow5 in currentTable5:

        # Extract freeNodes

        if(currentRow5.get_measurement() == "qisInstance1" and currentRow5.get_field() == "freeNodes"):
            listq5_1.append(currentRow5.get_start())
            listq5_2.append(currentRow5.get_stop())
            listq5_3.append(currentRow5.get_measurement())
            listq5_4.append(currentRow5.get_value())

df5["start_time"] = listq5_1
df5["end_time"] = listq5_2
df5["measurement"] = listq5_3
df5["free_nodes"] = listq5_4

# Compute count (overall)

q5CountOverall = df5.count()
print("Query 5 count (overall)")
print(q5CountOverall)

# Compute mean free_nodes

meanFreeNodes = df5["free_nodes"].mean()
meanFreeNodes = int(meanFreeNodes)
print("Mean free nodes = ", meanFreeNodes)

# Write(s)
# Mean for free nodes, instance 1
# meanFreeNodes

ptMFN1 = Point("queinfosys").tag("instanceName", "instance1").field("meanFreeNodes", meanFreeNodes)
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMFN1)

# Compute daily count free_nodes

dailyCountFreeNodes = df5.groupby("start_time", as_index=False)["free_nodes"].count()
print("Daily count free nodes")
print(dailyCountFreeNodes)

# Print date, daily count free_nodes mapping

for i in range(len(dailyCountFreeNodes)):
    print("Date: ", dailyCountFreeNodes["start_time"].loc[i], "Daily count: ", dailyCountFreeNodes["free_nodes"].loc[i])

# Compute daily mean free_nodes

dailyMeanFreeNodes = df5.groupby("start_time", as_index=False)["free_nodes"].mean()
dailyMeanFreeNodesCopy = dailyMeanFreeNodes.copy()

for i in range(len(dailyMeanFreeNodes)):
    dailyMeanFreeNodesCopy["free_nodes"].loc[i] = int(dailyMeanFreeNodes["free_nodes"].loc[i])

print("Daily mean free nodes")
print(dailyMeanFreeNodesCopy)

# Print date, daily mean free nodes mapping

for i in range(len(dailyMeanFreeNodesCopy)):
    print("Date: ", dailyMeanFreeNodesCopy["start_time"].loc[i], "Daily mean: ", dailyMeanFreeNodesCopy["free_nodes"].loc[i])

# Write(s)
# Daily mean for free nodes, instance 1
# dailyMeanFreeNodesCopy

for i in range(len(dailyMeanFreeNodesCopy)):
    dmFN1 = dailyMeanFreeNodesCopy["free_nodes"].loc[i]
    dmFN1Time = dailyMeanFreeNodesCopy["start_time"].loc[i]

    ptDMFN1 = Point("queinfosys").tag("instanceName", "instance1").field("dailyMeanFreeNodes", dmFN1).time(dmFN1Time)
    myWriteAPI.write(bucket=bucketToWriteTo, record=ptDMFN1)

# Compute min free_nodes

minFreeNodes = df5["free_nodes"].min()
print("Min free_nodes = ", minFreeNodes)

# Write(s)
# Min for free nodes, instance 1

ptMinFN1 = Point("queinfosys").tag("instanceName", "instance1").field("minFreeNodes", int(minFreeNodes))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMinFN1)

# Compute daily min free_nodes

dailyMinFreeNodes = df5.groupby("start_time", as_index=False)["free_nodes"].min()
print("Daily min free_nodes")
print(dailyMinFreeNodes)

# Print date, daily min free_nodes mapping

for i in range(len(dailyMinFreeNodes)):
    print("Date: ", dailyMinFreeNodes["start_time"].loc[i], "Daily min: ", dailyMinFreeNodes["free_nodes"].loc[i])

# Compute max free_nodes

maxFreeNodes = df5["free_nodes"].max()
print("Max free_nodes = ", maxFreeNodes)

# Write(s)
# Max of free nodes, instance 1
# maxFreeNodes

ptMaxFN1 = Point("queinfosys").tag("instanceName", "instance1").field("maxFreeNodes", int(maxFreeNodes))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMaxFN1)

# Compute daily max free_nodes

dailyMaxFreeNodes = df5.groupby("start_time", as_index=False)["free_nodes"].max()
print("Daily max free nodes")
print(dailyMaxFreeNodes)

# Print date, daily max free_nodes mapping

for i in range(len(dailyMaxFreeNodes)):
    print("Date: ", dailyMaxFreeNodes["start_time"].loc[i], "Daily max: ", dailyMaxFreeNodes["free_nodes"].loc[i])

# Query 6
# Window: 1 hour
# Measurement: "qisInstance2"
# Field: freeNodes

query6 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
allTables6 = myQueryAPI.query(query6)

df6 = pd.DataFrame()
q6List1 = list()
q6List2 = list()
q6List3 = list()
q6List4 = list()

for currentTable6 in allTables6:

    for currentRow6 in currentTable6:

        # Extract free nodes

        if(currentRow6.get_measurement() == "qisInstance2" and currentRow6.get_field() == "freeNodes"):
            q6List1.append(currentRow6.get_start())
            q6List2.append(currentRow6.get_stop())
            q6List3.append(currentRow6.get_measurement())
            q6List4.append(currentRow6.get_value())

df6["start_time"] = q6List1
df6["end_time"] = q6List2
df6["measurement"] = q6List3
df6["free_nodes"] = q6List4

# Compute count (overall)

countDF6 = df6.count()
print("Count (overall) df6")
print(countDF6)

# Compute mean free_nodes

meanFreeNodes2 = df6["free_nodes"].mean()
meanFreeNodes2 = int(meanFreeNodes2)
print("Mean free_nodes = ", meanFreeNodes2)

# Write(s)
# Mean for free nodes, instance 2
# meanFreeNodes2

ptMFN2 = Point("queinfosys").tag("instanceName", "instance2").field("meanFreeNodes", meanFreeNodes2)
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMFN2)

# Compute daily count free_nodes

dailyCountFreeNodes2 = df6.groupby("start_time", as_index=False)["free_nodes"].count()
print("Daily count free nodes")
print(dailyCountFreeNodes2)

# Print date, daily count free_nodes mapping

for i in range(len(dailyCountFreeNodes2)):
    print("Date: ", dailyCountFreeNodes2["start_time"].loc[i], "Daily count: ", dailyCountFreeNodes2["free_nodes"].loc[i])

# Compute daily mean free_nodes

dailyMeanFreeNodes2 = df6.groupby("start_time", as_index=False)["free_nodes"].mean()
dailyMeanFreeNodes2Copy = dailyMeanFreeNodes2.copy()

for i in range(len(dailyMeanFreeNodes2)):
    dailyMeanFreeNodes2Copy["free_nodes"].loc[i] = int(dailyMeanFreeNodes2["free_nodes"].loc[i])

print("Daily mean free_nodes")
print(dailyMeanFreeNodes2Copy)

# Print date, daily mean free_nodes mapping

for i in range(len(dailyMeanFreeNodes2Copy)):
    print("Date: ", dailyMeanFreeNodes2Copy["start_time"].loc[i], "Daily mean: ", dailyMeanFreeNodes2Copy["free_nodes"].loc[i])

# Write(s)
# Daily mean for free nodes, instance 2
# dailyMeanFreeNodes2Copy

for i in range(len(dailyMeanFreeNodes2Copy)):
    dmFN2 = dailyMeanFreeNodes2Copy["free_nodes"].loc[i]
    dmFN2Time = dailyMeanFreeNodes2Copy["start_time"].loc[i]

    ptDMFN2 = Point("queinfosys").tag("instanceName", "instance2").field("dailyMeanFreeNodes", dmFN2).time(dmFN2Time)
    myWriteAPI.write(bucket=bucketToWriteTo, record=ptDMFN2)

# Compute min free_nodes

minFreeNodes2 = df6["free_nodes"].min()
print("Min free_nodes = ", minFreeNodes2)

# Write(s)
# Min for free nodes, instance 2
# minFreeNodes2

ptMinFN2 = Point("queinfosys").tag("instanceName", "instance2").field("minFreeNodes", int(minFreeNodes2))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMinFN2)

# Compute daily min free_nodes

dailyMinFreeNodes2 = df6.groupby("start_time", as_index=False)["free_nodes"].min()
print("Daily min free_nodes")
print(dailyMinFreeNodes2)

# Print date, daily min free_nodes mapping

for i in range(len(dailyMinFreeNodes2)):
    print("Date: ", dailyMinFreeNodes2["start_time"].loc[i], "Daily min: ", dailyMinFreeNodes2["free_nodes"].loc[i])

# Compute max free_nodes

maxFreeNodes2 = df6["free_nodes"].max()
print("Max free_nodes = ", maxFreeNodes2)

# Write(s)
# Max for free nodes, instance 2
# maxFreeNodes2

ptMaxFN2 = Point("queinfosys").tag("instanceName", "instance2").field("maxFreeNodes", int(maxFreeNodes2))
myWriteAPI.write(bucket=bucketToWriteTo, record=ptMaxFN2)

# Compute daily max free_nodes

dailyMaxFreeNodes2 = df6.groupby("start_time", as_index=False)["free_nodes"].max()

print("Daily max free_nodes")
print(dailyMaxFreeNodes2)

# Print date, daily max free nodes mapping

for i in range(len(dailyMaxFreeNodes2)):
    print("Date: ", dailyMaxFreeNodes2["start_time"].loc[i], "Daily max: ", dailyMaxFreeNodes2["free_nodes"].loc[i])

