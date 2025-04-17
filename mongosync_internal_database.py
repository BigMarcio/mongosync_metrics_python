import json
import logging
from pymongo import MongoClient
from bson import MinKey, MaxKey
from pymongo.errors import PyMongoError
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import pandas as pd
import datetime
from datetime import datetime
from datetime import timezone
import time

# Logging setup
logging.basicConfig(filename='sync_shard_distribution_Ranges_V5.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection details
TARGET_MONGO_URI = "mongodb+srv://poc:poc@syncmonitor.qteeb.mongodb.net/?retryWrites=true&w=majority&appName=syncMonitor&timeoutMS=10900000&connectTimeoutMS=10800000"
TARGET_dbName = "admin"
internalDb = "mongosync_reserved_for_internal_use"

# Connect to MongoDB cluster
try:
    clientDst = MongoClient(TARGET_MONGO_URI)
    admin_dbDst = clientDst[TARGET_dbName]
    internalDbDst = clientDst[internalDb]
    logging.info("Connected to target MongoDB cluster.")
except PyMongoError as e:
    logging.error(f"Failed to connect to target MongoDB: {e}")
    exit(1)

# Retrieve data from resumeData collection for Mongosync Start time
def getInitialState():
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$eq": ["$$phaseTransitions.phase", "initializing collections and indexes"]}
                }}}}
    vProject = {"$project":{"_id": 0, "ts": {"$toDate": {"$arrayElemAt": ["$phaseTransitions.ts" ,0]}}}}
    vInitialData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vProject])
    return vInitialData

# Retrieve data from resumeData collection for Mongosync Finish time
def getFinishState():
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$eq": ["$$phaseTransitions.phase", "commit completed"]}
                }}}}
    vProject = {"$project":{"_id": 0, "ts": {"$toDate": {"$arrayElemAt": ["$phaseTransitions.ts" ,0]}}}}
    vFinishData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vProject])
    return vFinishData

# Retrieve data from resumeData collection (Mongosync state)
def getResumeData():
    vResumeData = internalDbDst.resumeData.find_one({"_id": "coordinator"})
    return vResumeData

# Retrieve data from resumeData collection (Phase transition)
def getTransitionData():
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$or": [{"$eq": ["$$phaseTransitions.phase", "initializing collections and indexes"]},
                                  {"$eq": ["$$phaseTransitions.phase", "initializing partitions"]},
                                  {"$eq": ["$$phaseTransitions.phase", "collection copy"]},
                                  {"$eq": ["$$phaseTransitions.phase", "change event application"]},
                                  {"$eq": ["$$phaseTransitions.phase", "waiting for commit to complete"]},
                                  {"$eq": ["$$phaseTransitions.phase", "commit completed"]}
                                  ]
                        }
                }}}}
    vUnwind = {"$unwind": "$phaseTransitions"}
    vProject = {"$project":{"_id": 0, "phase": "$phaseTransitions.phase", "ts": {"$toDate": "$phaseTransitions.ts" }}}
    #vProject = {"$project":{"_id": 0, "phase": "$phaseTransitions.phase", "ts": "$phaseTransitions.ts" }}
    vTransitionData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vUnwind, vProject])
    return vTransitionData

# Retrieve data from statistics collection for Partitions
def getPartitionData():
    vMatch = {"$match": {"_id.fieldName": "collectionStats", "numCompletedPartitions": {"$gt": 0}, "$expr": {"$ne": ["$numCompletedPartitions", "$numPartitions"]}}}
    vLookup = {"$lookup": { "from": "uuidMap", "localField": "_id.uuid", "foreignField": "_id", "as": "collectionData"}}
    vAddFields1 = {"$addFields": {"db": {"$arrayElemAt": ["$collectionData.dstDBName",0]},"coll": {"$arrayElemAt": ["$collectionData.dstCollName",0]}}}
    vAddFields2 = {"$addFields": {"namespace": {"$concat": ["$db", ".", "$coll"]}, "PercCompleted": {"$divide": [{ "$multiply": ["$numCompletedPartitions", 100] }, "$numPartitions"]}}}
    vProject = {"$project":{"_id": 0, "namespace": 1, "PercCompleted": 1 }}
    vPartitionData = internalDbDst.statistics.aggregate([vMatch, vLookup, vAddFields1, vAddFields2, vProject])
    return vPartitionData

# Retrieve data from statistics collection for % completed
def getCompleteData():
    vMatch = {"$match": {"_id.fieldName": "collectionStats"}}
    vProject = {"$project":{"_id": 0, "estimatedTotalBytes": {"$divide": ["$estimatedTotalBytes", 1048576]}, "estimatedCopiedBytes": {"$divide": ["$estimatedCopiedBytes", 1048576]} }}
    vCompleteData = internalDbDst.statistics.aggregate([vMatch, vProject])
    return vCompleteData

# Retrieve data from statistics collection for collections data
def getCollectionData():
    vMatch = {"$match": {"_id.fieldName": "collectionStats"}}
    vAddFields = {"$addFields": {"notStarted": {"$cond": { "if": { "$eq": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}},"inProgress": {"$cond": { "if": { "$ne": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}}, "completed": {"$cond": { "if": { "$eq": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}}}}
    vGroup = {"$group": {"_id": None, "notStarted": {"$sum": "$notStarted"}, "inProgress": {"$sum": "$inProgress"}, "completed": {"$sum": "$completed"}}}
    vProject = {"$project":{"_id": 0, "notStarted": 1, "inProgress": 1,  "completed": 1}}
    vCompleteData = internalDbDst.statistics.aggregate([vMatch, vAddFields, vGroup, vProject])
    return vCompleteData

# Plot Mongosync State
def plotResumeData(vDF, axes):
    vColor =''
    newResumeData = getResumeData()
    vState = newResumeData["state"]
    
    match vState:
        case 'RUNNING':
            vColor = 'blue'
        case "IDDLE":
            vColor = "yellow"
        case "PAUSED":
            vColor = "red"
        case _:
            vColor == "green"

    axes[0,0].cla()
    axes[0,0].text(0.5, 0.5, str(vState), ha='center', va='center', fontsize=16, color=vColor)
    axes[0,0].axis("off")
    axes[0,0].set_title('Mongosync State')

# Plot Mongosync Start data
def plotInitialData(dfState, axes):
    newInitialState = getInitialState()
    for initial in list(newInitialState):
        newReversible = initial['ts']
    axes[0,1].cla()  
    axes[0,1].text(0.5, 0.5, str(newReversible), ha='center', va='center', fontsize=16, color='green')
    axes[0,1].axis("off")
    axes[0,1].set_title('Mongosync Start')

# Plot Mongosync Finish data
def plotFinishData(dfFinish, axes):
    newFinishState = getFinishState()
    newFinishState =list(newFinishState)
    for finish in newFinishState:
        newFinish = finish['ts']
    axes[0,2].cla()  
    if len(newFinishState) == 0:
        axes[0,2].text(0.5, 0.5, "NO DATA", ha='center', va='center', fontsize=16, color='green')
    else:
        axes[0,2].text(0.5, 0.5, str(newFinish), ha='center', va='center', fontsize=16, color='green')
    axes[0,2].axis("off")
    axes[0,2].set_title('Mongosync Finish')

# Plot partition data
def plotStateData(dfPartition, axes):
    newPartitionData = getPartitionData()
    newPartitionData = list(newPartitionData)
    for partition in newPartitionData:
        newRow = pd.DataFrame([{'namespace': partition["namespace"], 'percCompleted': partition["PercCompleted"]}])
        dfPartition = pd.concat([dfPartition, newRow], ignore_index=True)
    axes[0,3].cla()
    if len(newPartitionData) == 0:
            axes[0,3].text(0.5, 0.5, "NO DATA", ha='center', va='center', fontsize=16, color='green')
            axes[0,3].axis("off")
    else:        
        sns.barplot(data=dfPartition, x='namespace', y='percCompleted', hue='namespace',legend=False, palette='viridis', ax=axes[0,3])
        axes[0,3].set_xlabel("Namespace")
        axes[0,3].set_ylabel("% Completed")
        axes[0,3].tick_params(axis='x', rotation=15)
    axes[0,3].set_title('Collections Progress')

# Plot main phases for Mongosync and when they happen
def plotPhaseData(dfPhase, axes):
    newPhaseData = getTransitionData()

    for phase in list(newPhaseData):
        newRow = pd.DataFrame([{'phase': phase["phase"], 'ts': phase["ts"]}])
        dfPhase = pd.concat([dfPhase, newRow], ignore_index=True)
    axes[1,0].cla()
    sns.scatterplot(data=dfPhase, x='ts', y='phase', ax=axes[1,0])
    axes[1,0].set_xlabel("Time")
    axes[1,0].set_ylabel("Phase")
    axes[1,0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axes[1,0].tick_params(axis='x', rotation=35, labelsize=8)
    axes[1,0].tick_params(axis='y', rotation=35, labelsize=8)
    axes[1,0].set_title('Mongosync Phases')

# Plot MBytes copied and total
def plotCompleteData(dfComplete, axes):
    axes[1,1].cla()
    newCompleteData = getCompleteData()

    for comp in list(newCompleteData):
        newRow = pd.DataFrame([{'copiedBytes': comp["estimatedCopiedBytes"], 'totalBytes': comp["estimatedTotalBytes"]}])
        dfComplete = pd.concat([dfComplete, newRow], ignore_index=True)
    dfCompleteMelted = dfComplete.melt(var_name='MByte Type', value_name='MBytes')
    sns.barplot(data=dfCompleteMelted, x='MByte Type', y='MBytes', ax=axes[1,1], hue='MByte Type', palette='viridis', legend=False, errorbar=None)
    axes[1,1].set_title('MBytes Copied X Total')

# Plot statistics on collections completed, inProgress or notStarted
def plotCollectionsData(dfCollections, axes):
    axes[1,2].cla()
    newCollectionData = getCollectionData()
    newCollectionData = list(newCollectionData)
    for collec in newCollectionData:
        newRow = pd.DataFrame([{"notStarted": collec["notStarted"], "inProgress": collec["inProgress"], "completed": collec["completed"]}])
        dfCollections = pd.concat([dfCollections, newRow], ignore_index=True)
    dfCollectionsMelted = dfCollections.melt(var_name='Coll Status', value_name='Qty')
    sns.barplot(data=dfCollectionsMelted, x='Coll Status', y='Qty', ax=axes[1,2], hue='Coll Status', palette='viridis', legend=False, errorbar=None)
    axes[1,2].set_title('Collection Status')

# Main plotting loop
if __name__ == '__main__':
    logging.info("Main process started")
    
    # Create initial data frames
    dfState = pd.DataFrame([])
    dfResume = pd.DataFrame([])
    dfPartition = pd.DataFrame([])
    dfPhase = pd.DataFrame([])
    dfComplete = pd.DataFrame([])
    dfFinish = pd.DataFrame([])
    dfCollections = pd.DataFrame([])

    # Create subplots
    fig, axes = plt.subplots(2, 4, figsize=(20, 12))
    # Loop for updating plots
    while True:

        plotResumeData(dfResume, axes)
        plotInitialData(dfState, axes)
        plotFinishData(dfState, axes)
        plotStateData(dfPartition, axes)
        plotPhaseData(dfPhase, axes)
        plotCompleteData(dfPhase, axes)
        plotCollectionsData(dfCollections, axes)

        # Adjust layout and update the plot
        plt.tight_layout()
        plt.draw()
        
        # Pause for a while before updating the plot again
        plt.pause(0.5)
        time.sleep(10)  # Adjust the sleep time as needed