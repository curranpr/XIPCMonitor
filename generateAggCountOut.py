import pandas as pd  
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Global variables

bucket = "countOut"

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up client")

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query 1
# Instance 1
# Queue 1
# Measurement: "qiqQueue1Instance1"
# Field: "countOut"

query1 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables1 = myQueryAPI.query(query1)

dfI1Q1 = pd.DataFrame()
listI1Q1_1 = list()
listI1Q1_2 = list()
listI1Q1_3 = list()
listI1Q1_4 = list()

for currentTable1 in tables1:

    for currentRow1 in currentTable1:

        # Extract "countOut"

        if (currentRow1.get_measurement() == "qiqQueue1Instance1" and currentRow1.get_field() == "countOut"):
            listI1Q1_1.append(currentRow1.get_start())
            listI1Q1_2.append(currentRow1.get_stop())
            listI1Q1_3.append(currentRow1.get_measurement())
            listI1Q1_4.append(currentRow1.get_value())

dfI1Q1["start_time"] = listI1Q1_1
dfI1Q1["end_time"] = listI1Q1_2
dfI1Q1["measurement"] = listI1Q1_3
dfI1Q1["count_out"] = listI1Q1_4

# Query 2
# Instance 1
# Queue 2
# Measurement: "qiqQueue2Instance1"
# Field: "countOut"

query2 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables2 = myQueryAPI.query(query2)

dfI1Q2 = pd.DataFrame()
listI1Q2_1 = list()
listI1Q2_2 = list()
listI1Q2_3 = list()
listI1Q2_4 = list()

for currentTable2 in tables2:

    for currentRow2 in currentTable2:

        # Extract "countOut"

        if (currentRow2.get_measurement() == "qiqQueue2Instance1" and currentRow2.get_field() == "countOut"):
            listI1Q2_1.append(currentRow2.get_start())
            listI1Q2_2.append(currentRow2.get_stop())
            listI1Q2_3.append(currentRow2.get_measurement())
            listI1Q2_4.append(currentRow2.get_value())

dfI1Q2["start_time"] = listI1Q2_1
dfI1Q2["end_time"] = listI1Q2_2
dfI1Q2["measurement"] = listI1Q2_3
dfI1Q2["count_out"] = listI1Q2_4

# Query 3
# Instance 2
# Queue 1
# Measurement: qiqQueue1Instance2
# Field: countOut

query3 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables3 = myQueryAPI.query(query3)

df3 = pd.DataFrame()
list3_1 = list()
list3_2 = list()
list3_3 = list()
list3_4 = list()

for currentTable3 in tables3:

    for currentRow3 in currentTable3:

        # Get "countOut"

        if (currentRow3.get_measurement() == "qiqQueue1Instance2" and currentRow3.get_field() == "countOut"):
            list3_1.append(currentRow3.get_start())
            list3_2.append(currentRow3.get_stop())
            list3_3.append(currentRow3.get_measurement())
            list3_4.append(currentRow3.get_value())

df3["start_time"] = list3_1
df3["end_time"] = list3_2
df3["measurement"] = list3_3
df3["count_out"] = list3_4

# Query 4
# Instance 2
# Queue 2
# Measurement: qiqQueue2Instance2
# Field: countOut

query4 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables4 = myQueryAPI.query(query4)

df4 = pd.DataFrame()
list4_1 = list()
list4_2 = list()
list4_3 = list()
list4_4 = list()

for tableObj in tables4:

    for rowObj in tableObj:

        # Get "countOut"

        if (rowObj.get_measurement() == "qiqQueue2Instance2" and rowObj.get_field() == "countOut"):
            list4_1.append(rowObj.get_start())
            list4_2.append(rowObj.get_stop())
            list4_3.append(rowObj.get_measurement())
            list4_4.append(rowObj.get_value())

df4["start_time"] = list4_1
df4["end_time"] = list4_2
df4["measurement"] = list4_3
df4["count_out"] = list4_4


def getDailyMean1():

    groupBy1 = dfI1Q1.groupby("start_time", as_index=False)["count_out"].mean()
    groupBy1Copy = groupBy1.copy()

    for i in range(len(groupBy1)):
        groupBy1Copy["count_out"].loc[i] = int(groupBy1["count_out"].loc[i])

    print("Daily mean for count out for instance 1, queue 1")
    print(groupBy1Copy)

    # Writes
    # Bucket: bucket

    for i in range(len(groupBy1Copy)):
        
        dm1 = groupBy1Copy["count_out"].loc[i]
        time = groupBy1Copy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("dailymeanCountOut", dm1).time(time)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean1():

    mean1 = int(dfI1Q1["count_out"].mean())

    print("Mean value for count out for instance 1, queue 1 = ", mean1)

    # Write
    # Bucket: bucket

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("meanCountOut", mean1)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin1():

    min1 = int(dfI1Q1["count_out"].min())

    print("Min value for count out for instance 1, queue 1 = ", min1)

    # Write
    # Bucket: bucket

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("minCountOut", min1)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax1():

    max1 = int(dfI1Q1["count_out"].max())

    print("Max value for count out for instance 1, queue 1 = ", max1)

    # Write
    # Bucket: bucket

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("maxCountOut", max1)
    myWriteAPI.write(bucket=bucket, record=pt)

def getDailyMean2():

    dm2 = dfI1Q2.groupby("start_time", as_index=False)["count_out"].mean()
    dm2Copy = dm2.copy()

    for i in range(len(dm2)):
        dm2Copy["count_out"].loc[i] = int(dm2["count_out"].loc[i])

    print("Daily mean for instance 1, queue 2")
    print(dm2Copy)

    # Writes
    # Bucket: bucket

    for i in range(len(dm2Copy)):
        dm2 = dm2Copy["count_out"].loc[i]
        time = dm2Copy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("dailymeanCountOut", dm2).time(time)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean2():

    mean2 = int(dfI1Q2["count_out"].mean())
    print("Mean value for count out for instance 1, queue 2 = ", mean2)

    # Write
    # Bucket: bucket

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("meanCountOut", mean2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin2():

    min2 = int(dfI1Q2["count_out"].min())
    print("Min value for count out for instance 1, queue 2 = ", min2)

    # Write
    # Bucket: bucket
    # Instance: Instance 1
    # Queue: Queue 2

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("minCountOut", min2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax2():

    max2 = int(dfI1Q2["count_out"].max())
    print("Max value for count out for instance 1, queue 2 = ", max2)

    # Write
    # Bucket: bucket
    # Instance: Instance 1
    # Queue: Queue 2

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("maxCountIn", max2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getDailyMean3():

    dm3 = df3.groupby("start_time", as_index=False)["count_out"].mean()
    dm3Copy = dm3.copy()

    for i in range(len(dm3)):

        dm3Copy["count_out"].loc[i] = int(dm3["count_out"].loc[i])

    print("Daily mean for count out for instance 2, queue 1")
    print(dm3Copy)

    # Writes
    # Bucket: bucket
    # Instance: Instance 2
    # Queue: Queue 1

    for i in range(len(dm3Copy)):
        dailyMean = dm3Copy["count_out"].loc[i]
        time = dm3Copy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("dailymeanCountOut", dailyMean).time(time)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean3():

    mean3 = int(df3["count_out"].mean())

    print("Mean value for count out for instance 2, queue 1 = ", mean3)

    # Write
    # Bucket: bucket
    # Instance 2
    # Queue 1

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("meanCountOut", mean3)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin3():

    min3 = int(df3["count_out"].min())

    print("Min value for count out for instance 2, queue 1 = ", min3)

    # Write point
    # Bucket: bucket
    # Instance 2
    # Queue 1

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("minCountOut", min3)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax3():

    max3 = int(df3["count_out"].max())

    print("Max value for count out for instance 2, queue 1 = ", max3)

    # Write 1 point
    # Bucket: bucket
    # Instance 2
    # Queue 1

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("maxCountOut", max3)
    myWriteAPI.write(bucket=bucket, record=pt)

def getDailyMean4():

    dm4 = df4.groupby("start_time", as_index=False)["count_out"].mean()
    dm4Copy = dm4.copy()

    for i in range(len(dm4)):

        dm4Copy["count_out"].loc[i] = int(dm4["count_out"].loc[i])

    print("Daily mean for count out for instance 2, queue 2")
    print(dm4Copy)

    # Write points
    # Bucket: bucket
    # Instance 2
    # Queue 2

    for i in range(len(dm4Copy)):
        currentDM = dm4Copy["count_out"].loc[i]
        currentTime = dm4Copy["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("dailymeanCountOut", currentDM).time(currentTime)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean4():

    mean4 = int(df4["count_out"].mean())

    print("Mean value for count out for instance 2, queue 2 = ", mean4)

    # Write point
    # Bucket: bucket
    # Instance 2
    # Queue 2

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("meanCountOut", mean4)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin4():

    min4 = int(df4["count_out"].min())
    
    print("Min value for count out for instance 2, queue 2 = ", min4)

    # Write point
    # Bucket: bucket
    # Instance 2
    # Queue 2

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("minCountOut", min4)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax4():

    max4 = int(df4["count_out"].max())

    print("Max value for instance 2, queue 2 = ", max4)

    # Write point
    # Bucket: bucket
    # Instance 2, Queue 2

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("maxCountOut", max4)
    myWriteAPI.write(bucket=bucket, record=pt)

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
getMax4()

closeClients()