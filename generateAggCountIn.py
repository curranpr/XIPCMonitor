import pandas as pd 
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Global variables

myBucket = "countIn"

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up client")

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query
# Measurement: "qiqQueue1Instance1"
# Field: "countIn"
# "1"

query1 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables1 = myQueryAPI.query(query1)

df1 = pd.DataFrame()
list1_1 = list()
list1_2 = list()
list1_3 = list()
list1_4 = list()

for currentTable1 in tables1:

    for currentRow1 in currentTable1:

        # Extract "countIn"

        if (currentRow1.get_measurement() == "qiqQueue1Instance1" and currentRow1.get_field() == "countIn"):
            list1_1.append(currentRow1.get_start())
            list1_2.append(currentRow1.get_stop())
            list1_3.append(currentRow1.get_measurement())
            list1_4.append(currentRow1.get_value())

df1["start_time"] = list1_1
df1["end_time"] = list1_2
df1["measurement"] = list1_3
df1["count_in"] = list1_4

# Query
# Measurement: "qiqQueue2Instance1"
# Field: "countIn"
# "2"

query2 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables2 = myQueryAPI.query(query2)

df2 = pd.DataFrame()
list2_1 = list()
list2_2 = list()
list2_3 = list()
list2_4 = list()

for currentTable2 in tables2:

    for currentRow2 in currentTable2:

        # Extract "countIn"

        if (currentRow2.get_measurement() == "qiqQueue2Instance1" and currentRow2.get_field() == "countIn"):
            list2_1.append(currentRow2.get_start())
            list2_2.append(currentRow2.get_stop())
            list2_3.append(currentRow2.get_measurement())
            list2_4.append(currentRow2.get_value())

df2["start_time"] = list2_1
df2["end_time"] = list2_2
df2["measurement"] = list2_3
df2["count_in"] = list2_4

# Query
# Instance 2, Queue 1
# Measurement: "qiqQueue1Instance2"
# Field: "countIn"
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

        # Extract "countIn"

        if (currentRow3.get_measurement() == "qiqQueue1Instance2" and currentRow3.get_field() == "countIn"):
            list3_1.append(currentRow3.get_start())
            list3_2.append(currentRow3.get_stop())
            list3_3.append(currentRow3.get_measurement())
            list3_4.append(currentRow3.get_value())

df3["start_time"] = list3_1
df3["end_time"] = list3_2
df3["measurement"] = list3_3
df3["count_in"] = list3_4

# Query
# Instance 2, Queue 2
# Measurement: "qiqQueue2Instance2"
# Field: "countIn"
# "4"

query4 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables4 = myQueryAPI.query(query4)

df4 = pd.DataFrame()
list4_1 = list()
list4_2 = list()
list4_3 = list()
list4_4 = list()

for currentTable4 in tables4:

    for currentRow4 in currentTable4:

        # Extract "countIn"

        if (currentRow4.get_measurement() == "qiqQueue2Instance2" and currentRow4.get_field() == "countIn"):
            list4_1.append(currentRow4.get_start())
            list4_2.append(currentRow4.get_stop())
            list4_3.append(currentRow4.get_measurement())
            list4_4.append(currentRow4.get_value())

df4["start_time"] = list4_1
df4["end_time"] = list4_2
df4["measurement"] = list4_3
df4["count_in"] = list4_4

def getDailyMean1():

    groupbyObj = df1.groupby("start_time", as_index=False)["count_in"].mean()
    groupbyObjCopy = groupbyObj.copy()

    for i in range(len(groupbyObj)):
        groupbyObjCopy["count_in"].loc[i] = int(groupbyObj["count_in"].loc[i])

    print("Daily mean for count in for instance 1, queue 1")
    print(groupbyObjCopy)

    # Write(s)
    # Variable: groupbyObjCopy
    # Daily mean
    # Time
    # Instance 1
    # Queue 1

    var = groupbyObjCopy
    
    for i in range(len(var)):
        dailyMean = var["count_in"].loc[i]
        time = var["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("dailymeanCountIn", dailyMean).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)
        

def getMean1():

    mean1 = df1["count_in"].mean()
    mean1 = int(mean1)

    print("Mean for count in for instance 1, queue 1 = ", mean1)

    # Write(s)
    # "mean1"
    # Instance 1
    # Queue 1

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("meanCountIn", mean1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMin1():

    min1 = df1["count_in"].min()
    min1 = int(min1)

    print("Min value for count in for instance 1, queue 1 = ", min1)

    # Write(s)
    # "min1"
    # Queue 1
    # Instance 1

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("minCountIn", min1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax1():

    max1 = df1["count_in"].max()
    max1 = int(max1)

    print("Max value for count in for instance 1, queue 1 = ", max1)

    # Write(s)
    # "max1"
    # Instance 1
    # Queue 1

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("maxCountIn", max1)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyMean2():

    groupbyObj2 = df2.groupby("start_time", as_index=False)["count_in"].mean()
    groupbyObj2Copy = groupbyObj2.copy()

    for i in range(len(groupbyObj2)):
        groupbyObj2Copy["count_in"].loc[i] = int(groupbyObj2["count_in"].loc[i])

    print("Daily mean for count in for instance 1, queue 2")
    print(groupbyObj2Copy)

    # Write(s)
    # Variable: groupbyObj2Copy
    # Instance 1
    # Queue 2
    
    var = groupbyObj2Copy

    for i in range(len(var)):
        dailyMean2 = var["count_in"].loc[i]
        time = var["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("dailymeanCountIn", dailyMean2).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)

def getMean2():

    mean2 = df2["count_in"].mean()
    mean2 = int(mean2)

    print("Mean for count in for instance 1, queue 2 = ", mean2)

    # Write(s)
    # Instance 1
    # Queue 2
    # "mean2"

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("meanCountIn", mean2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMin2():

    min2 = df2["count_in"].min()
    min2 = int(min2)

    print("Min value for count in for instance 1, queue 2 = ", min2)

    # Write
    # Instance 1
    # Queue 2
    # "min2"

    pt = Point("queinfoque").tag("instanceName" ,"instance1").tag("queueName", "queue2").field("minCountIn", min2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax2():

    max2 = int(df2["count_in"].max())

    print("Max value for count in for instance 1, queue 2 = ", max2)

    # Write
    # Instance 1
    # Queue 2
    # "max2"

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("maxCountIn", max2)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyMean3():

    groupbyObj3 = df3.groupby("start_time", as_index=False)["count_in"].mean()
    groupbyObj3Copy = groupbyObj3.copy()

    for i in range(len(groupbyObj3)):
        groupbyObj3Copy["count_in"].loc[i] = int(groupbyObj3["count_in"].loc[i])

    print("Daily mean for count in for instance 2, queue 1")
    print(groupbyObj3Copy)

    # Writes
    # Instance 2, Queue 1
    # Variable: groupbyObj3Copy

    var = groupbyObj3Copy

    for i in range(len(var)):
        dailyMean3 = var["count_in"].loc[i]
        time = var["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("dailymeanCountIn", dailyMean3).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)

def getMean3():

    mean3 = int(df3["count_in"].mean())

    print("Mean value for count in for instance 2, queue 1 = ", mean3)

    # Write
    # Instance 2, Queue 1 
    # Variable: mean3

    var = mean3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("meanCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMin3():

    min3 = int(df3["count_in"].min())

    print("Min value for count in for instance 2, queue 1 = ", min3)

    # Write
    # Instance 2, Queue 1
    # Variable: min3

    var = min3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("minCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax3():

    max3 = int(df3["count_in"].max())

    print("Max value for count in for instance 2, queue 1 = ", max3)

    # Write
    # Instance 2, Queue 1
    # Variable: max3

    var = max3

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("maxCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getDailyMean4():

    dm4 = df4.groupby("start_time", as_index=False)["count_in"].mean()
    dm4Copy = dm4.copy()

    for i in range(len(dm4)):
        dm4Copy["count_in"].loc[i] = int(dm4["count_in"].loc[i])

    print("Daily mean for count in for instance 2, queue 2")
    print(dm4Copy)

    # Writes
    # Instance 2
    # Queue 2
    # Variable: dm4Copy

    var = dm4Copy
    instance = "instance2"
    queue = "queue2"

    for i in range(len(var)):
        dailyMean4 = var["count_in"].loc[i]
        time = var["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", instance).tag("queueName", queue).field("dailymeanCountIn", dailyMean4).time(time)
        myWriteAPI.write(bucket=myBucket, record=pt)


def getMean4():

    mean4 = int(df4["count_in"].mean())
    print("Mean value for count in for instance 2, queue 2 = ", mean4)

    # Write
    # Instance 2
    # Queue 2
    # Variable: mean4

    instance = "instance2"
    queue = "queue2"
    var = mean4

    pt = Point("queinfoque").tag("instanceName", instance).tag("queueName", queue).field("meanCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMin4():

    min4 = int(df4["count_in"].min())
    print("Min value for count in for instance 2, queue 2 = ", min4)

    # Write
    # Instance 2
    # Queue 2
    # Variable: min4

    instance = "instance2"
    queue = "queue2"
    var = min4

    pt = Point("queinfoque").tag("instanceName", instance).tag("queueName", queue).field("minCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def getMax4():

    max4 = int(df4["count_in"].max())
    print("Max value for count in for instance 2, queue 2 = ", max4)

    # Write
    # Instance 2
    # Queue 2
    # Variable: max4

    instance = "instance2"
    queue = "queue2"
    var = max4

    pt = Point("queinfoque").tag("instanceName", instance).tag("queueName", queue).field("maxCountIn", var)
    myWriteAPI.write(bucket=myBucket, record=pt)

def closeClients():

    myWriteAPI.__del__()
    myClient.__del__()


getDailyMean1()
getMean1()
getMin1()
getMax1()

getDailyMean2()
getMean2()
getMin2()
getMax2()

getDailyMean3()
getMean3()
getMin3()
getMax3()

getDailyMean4()
getMean4()
getMin4()
getMax4( )

closeClients()