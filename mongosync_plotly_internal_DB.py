import plotly.graph_objects as go
import plotly.subplots as sp
from plotly.io import write_image
from plotly.utils import PlotlyJSONEncoder
from plotly.subplots import make_subplots
from tqdm import tqdm
from flask import Flask, request, redirect, url_for, render_template_string, send_from_directory
import json
from datetime import datetime
import io
import base64
from werkzeug.utils import secure_filename
import matplotlib.pyplot as plt
import os
import re
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pandas as pd


def format_byte_size(bytes):
    # Define the conversion factors
    kilobyte = 1024
    megabyte = kilobyte * 1024
    gigabyte = megabyte * 1024
    terabyte = gigabyte * 1024
    # Determine the appropriate unit and calculate the value
    if bytes >= terabyte:
        value = bytes / terabyte
        unit = 'TeraBytes'
    elif bytes >= gigabyte:
        value = bytes / gigabyte
        unit = 'GigaBytes'
    elif bytes >= megabyte:
        value = bytes / megabyte
        unit = 'MegaBytes'
    elif bytes >= kilobyte:
        value = bytes / kilobyte
        unit = 'KiloBytes'
    else:
        value = bytes
        unit = 'Bytes'
    # Return the value rounded to two decimal places and the unit separately
    return round(value, 4), unit

def convert_bytes(bytes, target_unit):
    # Define conversion factors
    kilobyte = 1024
    megabyte = kilobyte * 1024
    gigabyte = megabyte * 1024
    terabyte = gigabyte * 1024
    # Perform conversion based on target unit
    if target_unit == 'KiloBytes':
        value = bytes / kilobyte
    elif target_unit == 'MegaBytes':
        value = bytes / megabyte
    elif target_unit == 'GigaBytes':
        value = bytes / gigabyte
    elif target_unit == 'TeraBytes':
        value = bytes / terabyte
    else:
        value = bytes
    # Return the converted value rounded to two decimal places and the unit
    return round(value, 4)


# Create a Flask app
app = Flask(__name__)

@app.route('/')
def upload_form():
    # Return a simple file upload form
    return render_template_string('''
        <html>
            <head>
                <title>Mongosync Internal DB Metrics</title>
            </head>
            <body>
                <form method="post" action="/renderMetrics" enctype="multipart/form-data">
                    <input type="submit" value="Metrics Now">
                    <br/>
                    <br/>
                </form>
            </body>
        </html>
    ''')


@app.route('/get_metrics_data', methods=['POST'])
def gatherMetrics():
    logging.basicConfig(filename='mongosync_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    #TARGET_MONGO_URI = "mongodb+srv://poc:poc@syncmonitor.qteeb.mongodb.net/?retryWrites=true&w=majority&appName=syncMonitor&timeoutMS=10900000&connectTimeoutMS=10800000"
    #TARGET_MONGO_URI = "mongodb://127.0.0.1:27020,127.0.0.1:27021,127.0.0.1:27022/"
    #TARGET_MONGO_URI = "mongodb://127.0.0.1:27023,127.0.0.1:27024,127.0.0.1:27025/"
    TARGET_MONGO_URI = "mongodb://127.0.0.1:27026,127.0.0.1:27027,127.0.0.1:27028/"
    internalDb = "mongosync_reserved_for_internal_use"
    colors = ['red', 'blue', 'green', 'orange', 'yellow']
    # Connect to MongoDB cluster
    try:
        clientDst = MongoClient(TARGET_MONGO_URI)
        internalDbDst = clientDst[internalDb]
        logging.info("Connected to target MongoDB cluster.")
    except PyMongoError as e:
        logging.error(f"Failed to connect to target MongoDB: {e}")
        exit(1)
    # Create a subplot for the scatter plots and a separate subplot for the table
    fig = make_subplots(rows=3, 
                        cols=5, 
                        subplot_titles=("Current State", 
                                        "Current Phase",
                                        "Start",
                                        "Finish",
                                        "Partitions Completed %",
                                        "Total X Copied Data",
                                        "Mongosync Phases",
                                        "Collections Progress"),
                        specs=[[{}, {}, None, {}, {}],[{"colspan": 2}, None, None, {"colspan": 2}, None],[{"colspan": 2}, None, None, {"colspan": 2}, None]]                           
                        )

    #Plot mongosync State
    vResumeData = internalDbDst.resumeData.find_one({"_id": "coordinator"})
    vState = vResumeData["state"]
    match vState:
        case 'RUNNING':
            vColor = 'blue'
        case "IDDLE":
            vColor = "yellow"
        case "PAUSED":
            vColor = "red"
        case "COMMITTED":
            vColor = "green"
        case _:
            logging.warning(vState +" is not listed as an option")


    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(vState.capitalize())], mode='text', name='Mongosync State',textfont=dict(size=17, color=vColor)), row=1, col=1)
    fig.update_layout(xaxis1=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis1=dict(showgrid=False, zeroline=False, showticklabels=False))

    #Plot Mongosync Phase
    vPhase = vResumeData["syncPhase"].capitalize()
    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(vPhase)], mode='text', name='Mongosync State',textfont=dict(size=17, color="black")), row=1, col=2)
    fig.update_layout(xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False))

    #Plot Mongosync Start time
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$eq": ["$$phaseTransitions.phase", "initializing collections and indexes"]}
                }}}}
    vProject = {"$project":{"_id": 0, "ts": {"$toDate": {"$arrayElemAt": ["$phaseTransitions.ts" ,0]}}}}
    vInitialData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vProject])
    vInitialData = list(vInitialData)
    
    if len(vInitialData) > 0:
        for initial in vInitialData:
            newInitial = initial['ts']
    else:
        newInitial = 'NO DATA'

    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(newInitial)], mode='text', name='Mongosync Start',textfont=dict(size=17, color="black")), row=1, col=4)
    fig.update_layout(xaxis3=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis3=dict(showgrid=False, zeroline=False, showticklabels=False))
    
    #Plot Mongosync Finish time
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$eq": ["$$phaseTransitions.phase", "commit completed"]}
                }}}}
    vProject = {"$project":{"_id": 0, "ts": {"$toDate": {"$arrayElemAt": ["$phaseTransitions.ts" ,0]}}}}
    vFinishData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vProject])
    vFinishData = list(vFinishData)
    
    if len(vFinishData) > 0:
        for finish in vFinishData:
            newFinish = finish['ts']
    else:
        newFinish = 'NO DATA'

    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(newFinish)], mode='text', name='Mongosync Finish',textfont=dict(size=17, color="black")), row=1, col=5)
    fig.update_layout(xaxis4=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis4=dict(showgrid=False, zeroline=False, showticklabels=False))

    #Plot partition data

    vGroup1 = {"$group": {"_id": {"namespace": {"$concat": ["$namespace.db", ".", "$namespace.coll"]}, "partitionPhase": "$partitionPhase" },  "documentCount": { "$sum": 1 }}}
    vGroup2 = {"$group": {  "_id": {  "namespace": "$_id.namespace"},  "partitionPhaseCounts": {  "$push": {  "k": "$_id.partitionPhase",  "v": "$documentCount"  }  },  "totalDocumentCount": { "$sum": "$documentCount" }  }  }
    vAddFields1 = {"$addFields": {"namespace": "$_id.namespace"}}
    vProject1 = {"$project": {  "_id": 0,"namespace": 1,"totalDocumentCount": 1,  "partitionPhaseCounts":{"$arrayToObject": "$partitionPhaseCounts" }}  }
    vProject2 = {"$project": {  "_id": 0,"namespace": 1,"totalDocumentCount": 1,  "partitionPhaseCounts": {  "$mergeObjects": [  { "not started": 0, "in progress": 0, "done": 0 },  "$partitionPhaseCounts"  ]  }}  }
    vAddFields2 = {"$addFields": {"PercCompleted": {"$divide": [{ "$multiply": ["$partitionPhaseCounts.done", 100] }, "$totalDocumentCount"]}}}
    vSort1 = {"$sort": {"namespace": 1}}
    vPartitionData = internalDbDst.partitions.aggregate([vGroup1, vGroup2, vAddFields1, vProject1, vProject2, vAddFields2, vSort1])

    vPartitionData = list(vPartitionData)

    vNamespace = []
    vPercComplete = []
    if len(vPartitionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Mongosync Finish',textfont=dict(size=30, color="black")), row=2, col=1)
        fig.update_layout(xaxis5=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis5=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:        
        for partition in vPartitionData:
            vNamespace.append(partition["namespace"])
            vPercComplete.append(partition["PercCompleted"])
        fig.add_trace(go.Bar(x=vPercComplete, y=vNamespace, orientation='h', 
                             marker=dict(color=vPercComplete, colorscale='blugrn')), row=2, col=1)
        fig.update_xaxes(title_text="Completed %", row=2, col=1)
        fig.update_yaxes(title_text="Namespace", row=2, col=1)
        fig.update_layout(xaxis5=dict(range=[1, 100], dtick=5))

    #Plot complete data
    vGroup = {"$group":{"_id": None, "totalCopiedBytes": { "$sum": "$copiedByteCount" }, "totalBytesCount": { "$sum": "$totalByteCount" }  }}
    vCompleteData = internalDbDst.partitions.aggregate([vGroup])
    vCompleteData=list(vCompleteData)
    vCopiedBytes=0
    vTotalBytes=0
    vTypeByte=['Copied Data', 'Total Data']
    vBytes=[]
    if len(vCompleteData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=2, col=4)
        fig.update_layout(xaxis6=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis6=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:        
        for comp in list(vCompleteData):
            vCopiedBytes=comp["totalCopiedBytes"] + vCopiedBytes
            vTotalBytes=comp["totalBytesCount"] + vTotalBytes
        vTotalBytes, estimated_total_bytes_unit = format_byte_size(vTotalBytes)
        vCopiedBytes = convert_bytes(vCopiedBytes, estimated_total_bytes_unit)
        vBytes.append(vCopiedBytes)
        vBytes.append(vTotalBytes)
        fig.add_trace(go.Bar(x=vBytes, y=vTypeByte, orientation='h',
                             marker=dict(color=vBytes, colorscale='redor')), row=2, col=4)
        fig.update_xaxes(title_text=f"Data in {estimated_total_bytes_unit}", row=2, col=4)
        fig.update_yaxes(title_text="Copied / Total Data", row=2, col=4)
        fig.update_layout(xaxis6=dict(range=[0, vTotalBytes]))

    #Plot phase transitions
    vMatch = {"$match": {"_id": "coordinator"}}
    vAddFields = {"$addFields":{"phaseTransitions": {"$filter": {"input": "$phaseTransitions", "as": "phaseTransitions", 
                  "cond":{"$or": [{"$eq": ["$$phaseTransitions.phase", "initializing collections and indexes"]},
                                  {"$eq": ["$$phaseTransitions.phase", "initializing partitions"]},
                                  {"$eq": ["$$phaseTransitions.phase", "collection copy"]},
                                  {"$eq": ["$$phaseTransitions.phase", "waiting to start change event application"]},
                                  {"$eq": ["$$phaseTransitions.phase", "change event application"]},
                                  {"$eq": ["$$phaseTransitions.phase", "waiting for commit to complete"]},
                                  {"$eq": ["$$phaseTransitions.phase", "commit completed"]}
                                  ]
                        }
                }}}}
    vUnwind = {"$unwind": "$phaseTransitions"}
    vProject = {"$project":{"_id": 0, "phase": "$phaseTransitions.phase", "ts": {"$toDate": "$phaseTransitions.ts" }}}
    vTransitionData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vUnwind, vProject])
    vTransitionData=list(vTransitionData)
    vPhase=[]
    vTs=[]
    if len(vTransitionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=3, col=1)
        fig.update_layout(xaxis7=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis7=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:        
        for phase in list(vTransitionData):
            vPhase.append(phase["phase"])
            vTs.append(phase["ts"])
        fig.add_trace(go.Scatter(x=vTs, y=vPhase, mode='markers+text',marker=dict(color='green')), row=3, col=1)
    
    #Colection Progress
    vProject1 = {"$project": {  "namespace": {  "$concat": ["$namespace.db", ".", "$namespace.coll"]  },  "partitionPhase": 1  }}
    vGroup1 = {"$group": {  "_id": "$namespace",  "phases": { "$addToSet": "$partitionPhase" }  } }
    vProject2 = {"$project": {  "_id": 0,  "namespace": "$_id",  "phases": {  "$arrayToObject": {  "$map": {  "input": "$phases",  "as": "phase",  "in": { "k": "$$phase", "v": 1 }  }  }  }  }}
    vProject3 = {"$project": {  "_id": 0,"namespace": 1,  "phases": {  "$mergeObjects": [  { "not started": 0, "in progress": 0, "done": 0 },  "$phases"  ]  }}  }

    vCollectionData = internalDbDst.partitions.aggregate([vProject1, vGroup1, vProject2, vProject3])

    vCollectionData = list(vCollectionData)

    vTypeProc=[]
    vTypeValue=[]
    if len(vCollectionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=3, col=4)
        fig.update_layout(xaxis8=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis8=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:
        NotStarted = 0
        InProgress = 0
        Done = 0
        for collec in vCollectionData:
            if ((collec["phases"]["in progress"] == 1) or (collec["phases"]["not started"] == 1 and collec["phases"]["done"] == 1)):
                InProgress += 1
            elif (collec["phases"]["not started"] == 1 and collec["phases"]["done"] != 1):
                NotStarted += 1
            else:
                Done += 1
            
        vTypeProc.append("Not Started")
        vTypeValue.append(NotStarted)
        vTypeProc.append("In Progress")
        vTypeValue.append(InProgress)
        vTypeProc.append("Completed")
        vTypeValue.append(Done)
        xMin = min(vTypeValue)
        xMax = max(vTypeValue)

        fig.add_trace(go.Bar(x=vTypeValue, y=vTypeProc, orientation='h',
                             marker=dict(color=vTypeValue, colorscale='Oryel')), row=3, col=4)
        fig.update_xaxes(title_text=f"Totals", row=3, col=4)
        fig.update_yaxes(title_text="Process", row=3, col=4)
        fig.update_layout(xaxis8=dict(range=[0, xMax])) 
    
    # Update layout
    fig.update_layout(height=900, width=1600, autosize=False, title_text="Mongosync Replication Progress", showlegend=False, plot_bgcolor="white")
    
    # Convert the figure to JSON
    plot_json = json.dumps(fig, cls=PlotlyJSONEncoder)
    return plot_json

@app.route('/renderMetrics', methods=['POST'])
def plotMetrics():
    return render_template_string('''
        <html>
        <head>
            <title>Mongosync Internal DB Metrics</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                #plot {
                    width: 1600px;
                    height: 1800px;
                    margin: auto;
                }
                #loading {
                    text-align: center;
                    font-size: 24px;
                    margin-top: 50px;
                }
            </style>
        </head>
        <body>
            <div id="loading">Loading metrics...</div>
            <div id="plot" style="display:none;"></div>
            <script>
                async function fetchPlotData() {
                    try {
                        const response = await fetch("/get_metrics_data", { method: 'POST' });
                        const plotData = await response.json();
                        document.getElementById("loading").style.display = "none";
                        document.getElementById("plot").style.display = "block";
                        Plotly.react('plot', plotData.data, plotData.layout);
                    } catch (err) {
                        console.error("Error fetching data:", err);
                        document.getElementById("loading").innerText = "Error loading data.";
                    }
                }

                fetchPlotData(); // initial load
                setInterval(fetchPlotData, 10000); // update every 10 seconds
            </script>
        </body>
        </html>
    ''')



if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=3030)