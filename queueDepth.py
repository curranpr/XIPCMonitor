import pandas as pd   
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "queueDepth"

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up client")

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query 1
# Measurement: "qiqQueue1Instance1"
# Extract "countIn"
# Extract "countOut"
# Queue depth = "countIn" - "countOut"

query1 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables1 = myQueryAPI.query(query1)

dfI1Q1 = pd.DataFrame()
list1_1 = list()
list1_2 = list()
list1_3 = list()
list1_4 = list()

for table1 in tables1:

    for row1 in table1:

        # Extract "countIn" and "countOut"

        if (row1.get_measurement() == "qiqQueue1Instance1" and row1.get_field() == "countIn"):
            list1_1.append(row1.get_start())
            list1_2.append(row1.get_value())

        if (row1.get_measurement() == "qiqQueue1Instance1" and row1.get_field() == "countOut"):
            list1_3.append(row1.get_value())

# "countIn" - "countOut"

zipObj = zip(list1_2, list1_3)

for l1, l2 in zipObj:
    list1_4.append(l1-l2)

# Create DataFrame

dfI1Q1["start_time"] = list1_1
dfI1Q1["queue_depth"] = list1_4

# Query 2
# Measurement: qiqQueue2Instance1
# queue_depth = countIn - countOut

query2 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables2 = myQueryAPI.query(query2)

dfI1Q2 = pd.DataFrame()
list2_1 = list()
list2_2 = list()
list2_3 = list()
list2_4 = list()

for table2 in tables2:
    for row2 in table2:
        # Extract countIn and countOut

        if (row2.get_measurement() == "qiqQueue2Instance1" and row2.get_field() == "countIn"):
            list2_1.append(row2.get_start())
            list2_2.append(row2.get_value())

        if (row2.get_measurement() == "qiqQueue2Instance1" and row2.get_field() == "countOut"):
            list2_3.append(row2.get_value())

# countIn - countOut

zipObj2 = zip(list2_2, list2_3)

for lA, lB in zipObj2:
    list2_4.append(lA-lB)

# Add columns to DataFrame

dfI1Q2["start_time"] = list2_1
dfI1Q2["queue_depth"] = list2_4

# Query 3
# Measurement: qiqQueue1Instance2
# countIn
# countOut
# Queue depth = countIn - countOut

query3 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables3 = myQueryAPI.query(query3)

dfI2Q1 = pd.DataFrame()
list3_startTime = list()
list3_countIn = list()
list3_countOut = list()
list3_queueDepth = list()

for table3 in tables3:

    for row3 in table3:

        # Extract countIn

        if (row3.get_measurement() == "qiqQueue1Instance2" and row3.get_field() == "countIn"):
            list3_startTime.append(row3.get_start())
            list3_countIn.append(row3.get_value())

        # Extract countOut

        if (row3.get_measurement() == "qiqQueue1Instance2" and row3.get_field() == "countOut"):
            list3_countOut.append(row3.get_value())

zipObj3 = zip(list3_countIn, list3_countOut)

for l3_1, l3_2 in zipObj3:
    list3_queueDepth.append(l3_1-l3_2)

dfI2Q1["start_time"] = list3_startTime
dfI2Q1["queue_depth"] = list3_queueDepth

# Query 4
# Measurement: qiqQueue2Instance2

query4 = 'from(bucket: "simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tables4 = myQueryAPI.query(query4)

dfI2Q2 = pd.DataFrame()
list4_startTime = list()
list4_countIn = list()
list4_countOut = list()
list4_queueDepth = list()

for table4 in tables4:

    for row4 in table4:

        # countIn

        if (row4.get_measurement() == "qiqQueue2Instance2" and row4.get_field() == "countIn"):
            list4_startTime.append(row4.get_start())
            list4_countIn.append(row4.get_value())

        # countOut

        if (row4.get_measurement() == "qiqQueue2Instance2" and row4.get_field() == "countOut"):
            list4_countOut.append(row4.get_value())

zipObj4 = zip(list4_countIn, list4_countOut)

for l_a, l_b in zipObj4:
    
    list4_queueDepth.append(l_a-l_b)

dfI2Q2["start_time"] = list4_startTime
dfI2Q2["queue_depth"] = list4_queueDepth

def writeAllPoints1():

    for i in range(len(dfI1Q1)):
        
        startTime = dfI1Q1["start_time"].loc[i]
        qDepth = int(dfI1Q1["queue_depth"].loc[i])

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("queueDepth", qDepth).time(startTime)
        myWriteAPI.write(bucket=bucket, record=pt)

    print("Wrote all points for instance 1, queue1")

def getDailyMean1():

    groupby1 = dfI1Q1.groupby("start_time", as_index=False)["queue_depth"].mean()
    groupby1Copy = groupby1.copy()

    for i in range(len(groupby1)):
        groupby1Copy["queue_depth"].loc[i] = int(groupby1["queue_depth"].loc[i])

    print("Daily mean for instance 1, queue 1")
    print(groupby1Copy)

    # Write points

    for j in range(len(groupby1Copy)):
        dm = groupby1Copy["queue_depth"].loc[j]
        sTime = groupby1Copy["start_time"].loc[j]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("dailymeanQueueDepth", dm).time(sTime)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean1():

    mean1 = dfI1Q1["queue_depth"].mean()
    mean1 = int(mean1)

    print("Mean value for instance 1, queue 1")
    print(mean1)

    # Write

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("meanQueueDepth", mean1)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin1():

    min1 = dfI1Q1["queue_depth"].min()
    min1 = int(min1)

    print("Min value for instance 1, queue 1")
    print(min1)

    # Write

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("minQueueDepth", min1)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax1():

    max1 = dfI1Q1["queue_depth"].max()
    max1 = int(max1)

    print("Max value for instance 1, queue 1")
    print(max1)

    # Write max value

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue1").field("maxQueueDepth", max1)
    myWriteAPI.write(bucket=bucket, record=pt)

def writeAllPoints2():

    for i in range(len(dfI1Q2)):
        
        qDepth2 = int(dfI1Q2["queue_depth"].loc[i])
        time2 = dfI1Q2["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("queueDepth", qDepth2).time(time2)
        myWriteAPI.write(bucket=bucket, record=pt)

    print("Wrote all points for instance 1, queue 2")

def getDailyMean2():

    groupby2 = dfI1Q2.groupby("start_time", as_index=False)["queue_depth"].mean()
    groupby2Copy = groupby2.copy()

    # Convert each mean value to an int value
    
    for i in range(len(groupby2)):
        
        groupby2Copy["queue_depth"].loc[i] = int(groupby2["queue_depth"].loc[i])

    print("Daily mean for instance 1, queue 2")
    print(groupby2Copy)

    # Write daily mean points

    for j in range(len(groupby2Copy)):
        dm = groupby2Copy["queue_depth"].loc[j]
        time = groupby2Copy["start_time"].loc[j]

        pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("dailymeanQueueDepth", dm).time(time)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean2():

    mean2 = int(dfI1Q2["queue_depth"].mean())
    
    print("Mean value for instance 1, queue 2")
    print(mean2)

    # Write mean point

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("meanQueueDepth", mean2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin2():

    min2 = int(dfI1Q2["queue_depth"].min())

    print("Min value for instance 1, queue 2")
    print(min2)

    # Write min point

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("minQueueDepth", min2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax2():

    max2 = int(dfI1Q2["queue_depth"].max())

    print("Max value for instance 1, queue 2")
    print(max2)

    # Write max point

    pt = Point("queinfoque").tag("instanceName", "instance1").tag("queueName", "queue2").field("maxQueueDepth", max2)
    myWriteAPI.write(bucket=bucket, record=pt)

def writeAllPoints3():

    for i in range(len(dfI2Q1)):
        
        currentQD = int(dfI2Q1["queue_depth"].loc[i])
        currentTime = dfI2Q1["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("queueDepth", currentQD).time(currentTime)
        myWriteAPI.write(bucket=bucket, record=pt)

    print("Wrote all points for instance 2, queue 1")

def getDailyMean3():

    groupby_DM3 = dfI2Q1.groupby("start_time", as_index=False)["queue_depth"].mean()
    groupby_DM3Copy = groupby_DM3.copy()

    for i in range(len(groupby_DM3)):
        
        groupby_DM3Copy["queue_depth"].loc[i] = int(groupby_DM3["queue_depth"].loc[i])

    print("Daily mean for instance 2, queue 1")
    print(groupby_DM3Copy)

    # Write points

    for j in range(len(groupby_DM3Copy)):
        
        currentDM = groupby_DM3Copy["queue_depth"].loc[j]
        currentTime = groupby_DM3Copy["start_time"].loc[j]

        currentPoint = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("dailymeanQueueDepth", currentDM).time(currentTime)
        myWriteAPI.write(bucket=bucket, record=currentPoint)

def getMean3():

    mean3 = dfI2Q1["queue_depth"].mean()
    mean3 = int(mean3)

    print("Mean value for instance 2, queue 1")
    print(mean3)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("meanQueueDepth", mean3)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin3():

    min3 = dfI2Q1["queue_depth"].min()
    min3 = int(min3)

    print("Min value for instance 2, queue 1")
    print(min3)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("minQueueDepth", min3)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMax3():

    max3 = dfI2Q1["queue_depth"].max()
    max3 = int(max3)

    print("Max value for instance 2, queue 1")
    print(max3)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue1").field("maxQueueDepth", max3)
    myWriteAPI.write(bucket=bucket, record=pt)

def writeAllPoints4():

    for i in range(len(dfI2Q2)):
        
        qD = dfI2Q2["queue_depth"].loc[i]
        qD = int(qD)
        sT = dfI2Q2["start_time"].loc[i]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("queueDepth", qD).time(sT)
        myWriteAPI.write(bucket=bucket, record=pt)

    print("Wrote all points for instance 2, queue 2")

def getDailyMean4():

    groupby_DM4 = dfI2Q2.groupby("start_time", as_index=False)["queue_depth"].mean()
    groupby_DM4Copy = groupby_DM4.copy()

    # Convert each mean value to an int value

    for i in range(len(groupby_DM4)):

        groupby_DM4Copy["queue_depth"].loc[i] = int(groupby_DM4["queue_depth"].loc[i])

    print("Daily mean for instance 2, queue 2")
    print(groupby_DM4Copy)

    # Write points

    for j in range(len(groupby_DM4Copy)):
        
        dm = groupby_DM4Copy["queue_depth"].loc[j]
        timeVal = groupby_DM4Copy["start_time"].loc[j]

        pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("dailymeanQueueDepth", dm).time(timeVal)
        myWriteAPI.write(bucket=bucket, record=pt)

def getMean4():

    meanQD_I2Q2 = dfI2Q2["queue_depth"].mean()
    meanQD_I2Q2 = int(meanQD_I2Q2)

    print("Mean value for instance 2, queue 2")
    print(meanQD_I2Q2)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("meanQueueDepth", meanQD_I2Q2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMin4():

    minQD_I2Q2 = int(dfI2Q2["queue_depth"].min())

    print("Min value for instance 2, queue 2")
    print(minQD_I2Q2)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("minQueueDepth", minQD_I2Q2)
    myWriteAPI.write(bucket=bucket, record=pt)

def getMaxI2Q2():

    maxQD_I2Q2 = int(dfI2Q2["queue_depth"].max())

    print("Max value for instance 2, queue 2")
    print(maxQD_I2Q2)

    # Write point

    pt = Point("queinfoque").tag("instanceName", "instance2").tag("queueName", "queue2").field("maxQueueDepth", maxQD_I2Q2)
    myWriteAPI.write(bucket=bucket, record=pt)

def closeClients():

    myWriteAPI.__del__()
    myClient.__del__()

writeAllPoints1()
getDailyMean1()
getMean1()
getMin1()
getMax1()

writeAllPoints2()
getDailyMean2()
getMean2()
getMin2()
getMax2()

writeAllPoints3()
getDailyMean3()
getMean3()
getMin3()
getMax3()

writeAllPoints4()
getDailyMean4()
getMean4()
getMin4()
getMaxI2Q2()

closeClients()