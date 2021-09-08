import pandas as pd 
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Global variables

myBucket = "countMessages"

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up client")

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query
# Instance 1, Queue 1
# Measurement: "qiqQueue1Instance1"
# "countMessages"

queryI1Q1 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tablesI1Q1 = myQueryAPI.query(queryI1Q1)

dfI1Q1 = pd.DataFrame()
listI1Q1_1 = list()
listI1Q1_2 = list()
listI1Q1_3 = list()
listI1Q1_4 = list()

for currentTable in tablesI1Q1:

    for currentRow in currentTable:

        # Extract "countMessages"

        if (currentRow.get_measurement() == "qiqQueue1Instance1" and currentRow.get_field() == "countMessages"):
            listI1Q1_1.append(currentRow.get_start())
            listI1Q1_2.append(currentRow.get_stop())
            listI1Q1_3.append(currentRow.get_measurement())
            listI1Q1_4.append(currentRow.get_value())

dfI1Q1["start_time"] = listI1Q1_1
dfI1Q1["end_time"] = listI1Q1_2
dfI1Q1["measurement"] = listI1Q1_3
dfI1Q1["count_messages"] = listI1Q1_4

# Query
# Instance 1, Queue 2
# Field: "countMessages"
# Measurement: "qiqQueue2Instance1"

queryI1Q2 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tablesI1Q2 = myQueryAPI.query(queryI1Q2)

df2 = pd.DataFrame()
list2_1 = list()
list2_2 = list()
list2_3 = list()
list2_4 = list()

for currentTableI1Q2 in tablesI1Q2:

    for currentRowI1Q2 in currentTableI1Q2:

        # Extract "countMessages"

        if (currentRowI1Q2.get_measurement() == "qiqQueue2Instance1" and currentRowI1Q2.get_field() == "countMessages"):
            list2_1.append(currentRowI1Q2.get_start())
            list2_2.append(currentRowI1Q2.get_stop())
            list2_3.append(currentRowI1Q2.get_measurement())
            list2_4.append(currentRowI1Q2.get_value())

df2["start_time"] = list2_1
df2["end_time"] = list2_2
df2["measurement"] = list2_3
df2["count_messages"] = list2_4

# Query
# Instance 2, Queue 1
# Measurement: "qiqQueue1Instance2"
# Field: "countMessages"
# "3"

query3 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables3 = myQueryAPI.query(query3)

df3 = pd.DataFrame()
list3_1 = list()
list3_2 = list()
list3_3 = list()
list3_4 = list()

for currentTable3 in tables3:

    for currentRow3 in currentTable3:

        # Extract "countMessages"

        if (currentRow3.get_measurement() == 'qiqQueue1Instance2' and currentRow3.get_field() == "countMessages"):

            list3_1.append(currentRow3.get_start())
            list3_2.append(currentRow3.get_stop())
            list3_3.append(currentRow3.get_measurement())
            list3_4.append(currentRow3.get_value())

df3["start_time"] = list3_1
df3["end_time"] = list3_2
df3["measurement"] = list3_3
df3["count_messages"] = list3_4

# Query
# Instance 2, Queue 2
# Measurement: "qiqQueue2Instance2"
# Field: "countMessages"
# "4"

query4 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables4 = myQueryAPI.query(query4)

df4 = pd.DataFrame()
list4_1 = list()
list4_2 = list()
list4_3 = list()
list4_4 = list()


for table4 in tables4:

    for row4 in table4:

        # Extract "countMessages"

        if(row4.get_measurement() == "qiqQueue2Instance2" and row4.get_field() == "countMessages"):
            list4_1.append(row4.get_start())
            list4_2.append(row4.get_stop())
            list4_3.append(row4.get_measurement())
            list4_4.append(row4.get_value())

df4["start_time"] = list4_1
df4["end_time"] = list4_2
df4["measurement"] = list4_3
df4["count_messages"] = list4_4

def getCountOverall1():

    count1 = dfI1Q1.count()
    print("Count (overall) for count messages for instance 1, queue 1")
    print(count1)

def getMean1():
    
    mean1 = dfI1Q1["count_messages"].mean()
    mean1 = int(mean1)
    print("Mean for count messages for instance 1, queue 1 = ", mean1)

    # Write(s)

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("meanCountMessages", mean1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyCount1():

    dc1Groupby = dfI1Q1.groupby("start_time", as_index=False)["count_messages"].count()
    print("Daily count for count messages for instance 1, queue 1")
    print(dc1Groupby)

def getDailyMean1():

    dm1Groupby = dfI1Q1.groupby("start_time", as_index=False)["count_messages"].mean()
    dm1GroupbyCopy = dm1Groupby.copy()

    for i in range(len(dm1Groupby)):
        dm1GroupbyCopy["count_messages"].loc[i] = int(dm1Groupby["count_messages"].loc[i])

    print("Daily mean for count messages for instance 1, queue 1")
    print(dm1GroupbyCopy)

    # Write(s)

    for i in range(len(dm1Groupby)):
        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("dailymeanCountMessages", dm1GroupbyCopy["count_messages"].loc[i]).time(dm1GroupbyCopy["start_time"].loc[i])
        myWriteAPI.write(bucket=myBucket, record=pt)

def getMin1():

    min1 = dfI1Q1["count_messages"].min()
    min1 = int(min1)

    print("Min value for count messages for instance 1, queue 1 = ", min1)

    # Write(s)
    
    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("minCountMessages", min1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax1():

    max1 = dfI1Q1["count_messages"].max()
    max1 = int(max1)

    print("Max value for count messages for instance 1, queue 1 = ", max1)

    # Write(s)

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("maxCountMessages", max1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getCountOverall2():

    count2Overall = df2.count()
    print("Count overall for count messages for instance 1, queue 2")
    print(count2Overall)

def getMean2():

    mean2 = df2["count_messages"].mean()
    mean2 = int(mean2)
    print("Mean for count messages for instance 1, queue 2 = ", mean2)

    # Write(s)
    # mean2

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("meanCountMessages", mean2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyCount2():

    dc2 = df2.groupby("start_time", as_index=False)["count_messages"].count()
    print("Daily count for count messages for instance 1, queue 2")
    print(dc2)

def getDailyMean2():

    dm2 = df2.groupby("start_time", as_index=False)["count_messages"].mean()
    dm2Copy = dm2.copy()

    for i in range(len(dm2)):
        dm2Copy["count_messages"].loc[i] = int(dm2["count_messages"].loc[i])

    print("Daily mean for count messages for instance 1, queue 2")
    print(dm2Copy)

    # Write(s)
    # Variable: dm2Copy
    # Daily mean: dm2Copy["count_messages"].loc[i]
    # Time: dm2Copy["start_time"].loc[i]

    for i in range(len(dm2Copy)):
        dailyMean = dm2Copy["count_messages"].loc[i]
        time = dm2Copy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("dailymeanCountMessages", dailyMean).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)

def getMin2():

    min2 = df2["count_messages"].min()
    min2 = int(min2)
    print("Min value for count messages for instance 1, queue 2 =", min2)

    # Write(s)
    # Instance 1, Queue 2
    # Variable: min2

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("minCountMessages", min2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax2():

    max2 = df2["count_messages"].max()
    max2 = int(max2)
    print("Max value for count messages for instance 1, queue 2 = ", max2)

    # Write(s)
    # Instance 1, Queue 2
    # Variable: max2

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("maxCountMessages", max2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMean3():

    mean3 = df3["count_messages"].mean()
    mean3 = int(mean3)
    print("Mean for count messages for instance 2, queue 1 = ", mean3)

    # Write(s)
    # Instance 2, Queue 1
    # Variable: mean3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("meanCountMessages", mean3)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyMean3():

    dm3Groupby = df3.groupby("start_time", as_index=False)["count_messages"].mean()
    dm3GroupbyCopy = dm3Groupby.copy()

    for i in range(len(dm3Groupby)):
        dm3GroupbyCopy["count_messages"].loc[i] = int(dm3Groupby["count_messages"].loc[i])

    print("Daily mean for count messages for instance 2, queue 1")
    print(dm3GroupbyCopy)

    # Write(s)
    # Instance 2, Queue 1
    # Variable: dm3GroupbyCopy
    # Field: dm3GroupbyCopy["count_messages"].loc[i]
    # Time: dm3GroupbyCopy["start_time"].loc[i]

    for i in range(len(dm3GroupbyCopy)):
        field = dm3GroupbyCopy["count_messages"].loc[i]
        time = dm3GroupbyCopy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("dailymeanCountMessages", field).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)


def getMin3():

    min3 = df3["count_messages"].min()
    min3 = int(min3)
    print("Min value for count messages for instance 2, queue 1 = ", min3)

    # Write(s)
    # Instance 2, Queue 1
    # Variable: min3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("minCountMessages", min3)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax3():

    max3 = df3["count_messages"].max()
    max3 = int(max3)
    print("Max value for count messages for instance 2, queue 1 = ", max3)

    # Write(s)
    # Instance 2, Queue 1
    # Variable: max3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("maxCountMessages", max3)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMean4():

    mean4 = df4["count_messages"].mean()
    mean4 = int(mean4)
    print("Mean for count messages for instance 2, queue 2 = ", mean4)

    # Write(s)
    # Instance 2, Queue 2
    # Variable: mean4

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("meanCountMessages", mean4)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyMean4():

    dm4Groupby = df4.groupby("start_time", as_index=False)["count_messages"].mean()
    dm4GroupbyCopy = dm4Groupby.copy()

    for i in range(len(dm4Groupby)):
        dm4GroupbyCopy["count_messages"].loc[i] = int(dm4Groupby["count_messages"].loc[i])

    print("Daily mean for count messages for instance 2, queue 2")
    print(dm4GroupbyCopy)

    # Write(s)
    # Instance 2, Queue 2
    # Variable: dm4GroupbyCopy
    # Field: dm4GroupbyCopy["count_messages"].loc[i]
    # Time: dm4GroupbyCopy["start_time"].loc[i]

    for i in range(len(dm4GroupbyCopy)):
        field = dm4GroupbyCopy["count_messages"].loc[i]
        time = dm4GroupbyCopy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("dailymeanCountMessages", field).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)

def getMinMax4():

    min4 = df4["count_messages"].min()
    min4 = int(min4)

    max4 = df4["count_messages"].max()
    max4 = int(max4)

    print("Min (4) = ", min4, " Max (4) = ", max4)

    # Write(s)
    # Instance 2, Queue 2
    # Variables: min4, max4

    pt1 = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("minCountMessages", min4)
    pt2 = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("maxCountMessages", max4)

    myWriteAPI.write(bucket=myBucket, record=pt1)
    myWriteAPI.write(bucket=myBucket, record=pt2)

def closeClients():

    myWriteAPI.__del__()
    myClient.__del__()


getCountOverall1()
getMean1()
getDailyCount1()
getDailyMean1()
getMin1()
getMax1()
getCountOverall2()
getMean2()
getDailyCount2()
getDailyMean2()
getMin2()
getMax2()
getMean3()
getDailyMean3()
getMin3()
getMax3()
getMean4()
getDailyMean4()
getMinMax4()
closeClients()