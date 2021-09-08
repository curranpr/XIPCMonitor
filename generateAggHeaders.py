import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Globals

writeBucket = "xipcAggregate2"

myClient = InfluxDBClient(url="http://localhost:8086", token="wOyUKHrQTB5scMJB8CnEjuOwF9RlQBlyKERIuwb4Er2Q63fQZQKvsVIeFqEOmXgRzSVtxaZrE7IZXvBRutedBA==", org="Envoy Tech")
print("Set up client")

myWriteAPI = myClient.write_api(write_options=SYNCHRONOUS)
print("Set up write API")

myQueryAPI = myClient.query_api()
print("Set up query API")

# Query for free headers, instance 1

queryFHI1 = 'from(bucket:"simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tablesFHI1 = myQueryAPI.query(queryFHI1)

df1 = pd.DataFrame()
list1 = list()
list2 = list()
list3 = list()
list4 = list()

for currentTable in tablesFHI1:

    for currentRow in currentTable:

        # Extract free headers

        if(currentRow.get_measurement() == "qisInstance1" and currentRow.get_field() == "freeHeaders"):
            list1.append(currentRow.get_start())
            list2.append(currentRow.get_stop())
            list3.append(currentRow.get_measurement())
            list4.append(currentRow.get_value())

df1["start_time"] = list1
df1["end_time"] = list2
df1["measurement"] = list3
df1["free_headers"] = list4

# Query for free headers, instance 2

queryFHI2 = 'from(bucket:"simulateXIPC") |> range(start: -7d) |> window(every: 1d)'
tablesFHI2 = myQueryAPI.query(queryFHI2)

df2 = pd.DataFrame()
list1_a = list()
list2_a = list()
list3_a = list()
list4_a = list()

for currentTable2 in tablesFHI2:

    for currentRow2 in currentTable2:

        # Extract free headers

        if(currentRow2.get_measurement() == "qisInstance2" and currentRow2.get_field() == "freeHeaders"):
            list1_a.append(currentRow2.get_start())
            list2_a.append(currentRow2.get_stop())
            list3_a.append(currentRow2.get_measurement())
            list4_a.append(currentRow2.get_value())

df2["start_time"] = list1_a
df2["end_time"] = list2_a
df2["measurement"] = list3_a
df2["free_headers"] = list4_a

def printDF1():
    
    print("DataFrame for free headers, instance 1")
    print(df1)

def getCountOverall():

    countI1 = df1.count()
    print("Count (overall) for instance 1")
    print(countI1)

def getMean():

    meanI1 = df1.mean()
    meanI1 = int(meanI1)
    print("Mean for instance 1 = ", meanI1)

    # Write(s)
    # Mean for free headers, instance 1
    # meanI1

    pt2 = Point("queinfosys").tag("instanceName", "instance1").field("meanFreeHeadersSingle", int(meanI1))
    myWriteAPI.write(bucket=writeBucket, record=pt2)

def getDailyCount():

    dcGroupby = df1.groupby("start_time", as_index=False)["free_headers"].count()
    print("Daily count for free headers, instance 1")
    print(dcGroupby)

def getDailyMean():

    dmGroupby = df1.groupby("start_time", as_index=False)["free_headers"].mean()
    dmGroupbyCopy = dmGroupby.copy()

    for i in range(len(dmGroupby)):   
        dmGroupbyCopy["free_headers"].loc[i] = int(dmGroupby["free_headers"].loc[i])

    print("Daily mean for free headers, instance 1")
    print(dmGroupbyCopy)

    # Write(s)
    # Daily mean for free headers, instance 1
    # dmGroupbyCopy

    for i in range(len(dmGroupbyCopy)):
        currentTime = dmGroupbyCopy["start_time"].loc[i]
        currentMean = dmGroupbyCopy["free_headers"].loc[i]

        pt = Point("queinfosys").tag("instanceName", "instance1").field("dailymeanFreeHeaders", currentMean).time(currentTime)
        myWriteAPI.write(bucket=writeBucket, record=pt)

def getMin():

    minI1 = df1["free_headers"].min()
    print("Min value for free headers, instance 1 = ", minI1)

    # Write(s)
    # Min for free headers, instance 1
    # minI1

    ptMinI1 = Point("queinfosys").tag("instanceName", "instance1").field("minFreeHeaders", int(minI1))
    myWriteAPI.write(bucket=writeBucket, record=ptMinI1)

def getMax():

    maxI1 = df1["free_headers"].max()
    print("Max value for free headers, instance 1 = ", maxI1)
    
    # Write(s)
    # Max for free headers, instance 1
    # maxI1

    ptMaxI1 = Point("queinfosys").tag("instanceName", "instance1").field("maxFreeHeaders", int(maxI1))
    myWriteAPI.write(bucket=writeBucket, record=ptMaxI1)

def printDF2():
    print("DataFrame for free headers, instance 2")
    print(df2)

def getCountOverall2():

    count2 = df2.count()
    print("Count (overall) for free headers, instance 2")
    print(count2)

def getMean2():

    mean2 = df2["free_headers"].mean()
    mean2 = int(mean2)
    print("Mean for free headers, instance 2 = ", mean2)

    # Write(s)
    # Mean for free headers, instance 2
    # mean2

    ptMeanI2 = Point("queinfosys").tag("instanceName", "instance2").field("meanFreeHeadersSingle", mean2)
    myWriteAPI.write(bucket=writeBucket, record=ptMeanI2)

def getDailyCount2():

    groupByDC2 = df2.groupby("start_time", as_index=False)["free_headers"].count()
    print("Daily count for free headers, instance 2")
    print(groupByDC2)

def getDailyMean2():

    groupByDM2 = df2.groupby("start_time", as_index=False)["free_headers"].mean()
    groupByDM2Copy = groupByDM2.copy()

    for i in range(len(groupByDM2)):
        groupByDM2Copy["free_headers"].loc[i] = int(groupByDM2["free_headers"].loc[i])

    print("Daily mean for free headers, instance 2")
    print(groupByDM2Copy)

    # Write(s)
    # Daily mean for free headers, instance 2
    # groupByDM2Copy

    for i in range(len(groupByDM2Copy)):
        currentTimeDM2 = groupByDM2Copy["start_time"].loc[i]
        currentDailyMean2 = groupByDM2Copy["free_headers"].loc[i]

        currentPointDM2 = Point("queinfosys").tag("instanceName", "instance2").field("dailymeanFreeHeaders", currentDailyMean2).time(currentTimeDM2)
        myWriteAPI.write(bucket=writeBucket, record=currentPointDM2)

def getMin2():

    min2 = df2["free_headers"].min()
    print("Min value for free headers, instance 2 = ", min2)

    # Write(s)
    # Min for free headers, instance 2
    # min2

    ptMin2 = Point("queinfosys").tag("instanceName", "instance2").field("minFreeHeaders", int(min2))
    myWriteAPI.write(bucket=writeBucket, record=ptMin2)

def getMax2():

    max2 = df2["free_headers"].max()
    print("Max value for free headers, instance 2 = ", max2)

    # Write(s)
    # Max for free headers, instance 2
    # max2
    # Cast to int

    ptMax2 = Point("queinfosys").tag("instanceName", "instance2").field("maxFreeHeaders", int(max2))
    myWriteAPI.write(bucket=writeBucket, record=ptMax2)

printDF1()
getCountOverall()
getMean()
getDailyCount()
getDailyMean()
getMin()
getMax()
printDF2()
getCountOverall2()
getMean2()
getDailyCount2()
getDailyMean2()
getMin2()
getMax2()