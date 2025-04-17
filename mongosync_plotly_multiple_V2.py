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
                <title>Mongosync Metrics</title>
            </head>
            <body>
                <form method="post" action="/upload" enctype="multipart/form-data">
                    <input type="file" name="file">
                    <input type="submit" value="Upload">
                    <p>This form allows you to upload a mongosync log file. Once the file is uploaded, the application will process the data and generate plots.</p>
                    <br/>
                    <br/>
                </form>
                <form method="post" action="/renderMetrics" enctype="multipart/form-data">
                    <input type="submit" value="Metrics Now">
                    <br/>
                    <br/>
                </form>
            </body>
        </html>
    ''')

@app.route('/upload', methods=['POST'])
def upload_file():
    # Check if a file was uploaded
    if 'file' not in request.files:
        return redirect(request.url)

    file = request.files['file']

    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if file.filename == '':
        return redirect(request.url)

    if file:
        # Read the file and convert it to a list of lines
        lines = list(file)

        # Check if all lines are valid JSON
        for line in tqdm(lines, desc="Reading lines"):
            try:
                json.loads(line)
            except json.JSONDecodeError:
                print(f"Invalid JSON: {line}")
                return redirect(request.url)  # or handle the error in another appropriate way

        # Load lines with 'message' == "Replication progress."
        #data = [json.loads(line) for line in lines if json.loads(line).get('message') == "Replication progress."]
        regex_pattern = re.compile(r"Replication progress", re.IGNORECASE)
        data = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # Load lines with 'message' == "Version info"
        #version_info_list = [json.loads(line) for line in lines if json.loads(line).get('message') == "Version info"]
        regex_pattern = re.compile(r"Version info", re.IGNORECASE)
        version_info_list = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # Load lines with 'message' == "Mongosync Options"
        #mongosync_opts_list = [json.loads(line) for line in lines if json.loads(line).get('message') == "Mongosync Options"]
        regex_pattern = re.compile(r"Mongosync Options", re.IGNORECASE)
        mongosync_opts_list = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # Load lines with 'message' == "Operation duration stats."
        #mongosync_ops_stats = [json.loads(line) for line in lines if json.loads(line).get('message') == "Operation duration stats."]
        regex_pattern = re.compile(r"Operation duration stats", re.IGNORECASE)
        mongosync_ops_stats = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # Load lines with 'message' == "sent response"
        #mongosync_sent_response = [json.loads(line) for line in lines if json.loads(line).get('message') == "Sent response."]
        regex_pattern = re.compile(r"sent response", re.IGNORECASE)
        mongosync_sent_response = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # Load lines with 'message' == "Mongosync HiddenFlags"
        regex_pattern = re.compile(r"Mongosync HiddenFlags", re.IGNORECASE)
        mongosync_hiddenflags = [
            json.loads(line) 
            for line in lines 
            if regex_pattern.search(json.loads(line).get('message', ''))
        ]

        # The 'body' field is also a JSON string, so parse that as well
        #mongosync_sent_response_body = json.loads(mongosync_sent_response.get('body'))
        for response in mongosync_sent_response:
            mongosync_sent_response_body  = json.loads(response['body'])
            # Now you can work with the 'body' data

        # Create a string with all the Mongosync Options information
        mongosync_opts_text = "\n".join([json.dumps(item, indent=4) for item in mongosync_opts_list])

        # Create a string with all the version information
        version_text = "\n".join([f"MongoSync Version: {item.get('version')}, OS: {item.get('os')}, Arch: {item.get('arch')}" for item in version_info_list])

        # Extract the keys from the mongosync_hiddenflags
        # For each key, extract the corresponding values from mongosync_hiddenflags
        if mongosync_hiddenflags:
            keys = list(mongosync_hiddenflags[0].keys())
            values = [[str(item[key]).replace('{', '').replace('}', '')  for item in mongosync_hiddenflags] for key in keys]

            #.replace(', ', ' \n ')

            #print (mongosync_hiddenflags)
            #print(keys)
            #print(values)

            # Create a table trace with the keys as the first column and the corresponding values as the second column
            table_hiddenflags = go.Table(
                header=dict(values=['Key', 'Value'], font=dict(size=12, color='black')),
                cells=dict(values=[keys, values],  align=['left'], font=dict(size=10, color='darkblue')), #
                columnwidth=[0.75, 2.5]  # Adjust the column widths as needed
            )

            # Extract the data you want to plot
            times = [datetime.strptime(item['time'][:26], "%Y-%m-%dT%H:%M:%S.%f") for item in data if 'time' in item]
            totalEventsApplied = [item['totalEventsApplied'] for item in data if 'totalEventsApplied' in item]
            lagTimeSeconds = [item['lagTimeSeconds'] for item in data if 'lagTimeSeconds' in item]
        else:
            #print("mongosync_hiddenflags is empty")
            table_hiddenflags = go.Table(
                header=dict(values=['Mongosync Hidden Flags']),
                cells=dict(values=[["No Mongosync Hidden Flags found in the log file"]])
            )
        
        if mongosync_opts_list:
            keys = list(mongosync_opts_list[0].keys())
            values = [[item[key] for item in mongosync_opts_list] for key in keys]

            # Create a table trace with the keys as the first column and the corresponding values as the second column
            table_trace = go.Table(
                header=dict(values=['Key', 'Value'], font=dict(size=12, color='black')),
                cells=dict(values=[keys, values], font=dict(size=10, color='darkblue')),
                columnwidth=[0.75, 2.5]  # Adjust the column widths as needed
            )

            # Extract the data you want to plot
            times = [datetime.strptime(item['time'][:26], "%Y-%m-%dT%H:%M:%S.%f") for item in data if 'time' in item]
            totalEventsApplied = [item['totalEventsApplied'] for item in data if 'totalEventsApplied' in item]
            lagTimeSeconds = [item['lagTimeSeconds'] for item in data if 'lagTimeSeconds' in item]

            # If the key is 'hiddenFlags', extract its keys and values and add them to the keys and values lists
            for i, key in enumerate(keys):
                if key == 'hiddenFlags':
                    hidden_keys = list(values[i][0].keys())
                    hidden_values = [[item.get(key, '') for item in values[i]] for key in hidden_keys]
                    keys = keys[:i] + hidden_keys + keys[i+1:]
                    values = values[:i] + hidden_values + values[i+1:]
        else:
            #print("mongosync_opts_list is empty")
            table_trace = go.Table(header=dict(values=['Mongosync Options']),
            cells=dict(values=[["No Mongosync Options found in the log file"]]))


        # Extract the data you want to plot
        times = [datetime.strptime(item['time'][:26], "%Y-%m-%dT%H:%M:%S.%f") for item in data if 'time' in item]
        totalEventsApplied = [item['totalEventsApplied'] for item in data if 'totalEventsApplied' in item]
        lagTimeSeconds = [item['lagTimeSeconds'] for item in data if 'lagTimeSeconds' in item]
        CollectionCopySourceRead = [float(item['CollectionCopySourceRead']['averageDurationMs']) for item in mongosync_ops_stats if 'CollectionCopySourceRead' in item and 'averageDurationMs' in item['CollectionCopySourceRead']]
        CollectionCopySourceRead_maximum = [float(item['CollectionCopySourceRead']['maximumDurationMs']) for item in mongosync_ops_stats if 'CollectionCopySourceRead' in item and 'maximumDurationMs' in item['CollectionCopySourceRead']]
        CollectionCopySourceRead_numOperations = [float(item['CollectionCopySourceRead']['numOperations']) for item in mongosync_ops_stats if 'CollectionCopySourceRead' in item and 'numOperations' in item['CollectionCopySourceRead']]        
        CollectionCopyDestinationWrite = [float(item['CollectionCopyDestinationWrite']['averageDurationMs']) for item in mongosync_ops_stats if 'CollectionCopyDestinationWrite' in item and 'averageDurationMs' in item['CollectionCopyDestinationWrite']]
        CollectionCopyDestinationWrite_maximum  = [float(item['CollectionCopyDestinationWrite']['maximumDurationMs']) for item in mongosync_ops_stats if 'CollectionCopyDestinationWrite' in item and 'maximumDurationMs' in item['CollectionCopyDestinationWrite']]
        CollectionCopyDestinationWrite_numOperations = [float(item['CollectionCopyDestinationWrite']['numOperations']) for item in mongosync_ops_stats if 'CollectionCopyDestinationWrite' in item and 'numOperations' in item['CollectionCopyDestinationWrite']]
        CEASourceRead = [float(item['CEASourceRead']['averageDurationMs']) for item in mongosync_ops_stats if 'CEASourceRead' in item and 'averageDurationMs' in item['CEASourceRead']]
        CEASourceRead_maximum  = [float(item['CEASourceRead']['maximumDurationMs']) for item in mongosync_ops_stats if 'CEASourceRead' in item and 'maximumDurationMs' in item['CEASourceRead']]
        CEASourceRead_numOperations = [float(item['CEASourceRead']['numOperations']) for item in mongosync_ops_stats if 'CEASourceRead' in item and 'numOperations' in item['CEASourceRead']]
        CEADestinationWrite = [float(item['CEADestinationWrite']['averageDurationMs']) for item in mongosync_ops_stats if 'CEADestinationWrite' in item and 'averageDurationMs' in item['CEADestinationWrite']]
        CEADestinationWrite_maximum = [float(item['CEADestinationWrite']['maximumDurationMs']) for item in mongosync_ops_stats if 'CEADestinationWrite' in item and 'maximumDurationMs' in item['CEADestinationWrite']]    
        CEADestinationWrite_numOperations = [float(item['CEADestinationWrite']['numOperations']) for item in mongosync_ops_stats if 'CEADestinationWrite' in item and 'numOperations' in item['CEADestinationWrite']] 
        
        # Initialize estimated_total_bytes and estimated_copied_bytes with a default value
        estimated_total_bytes = 0
        estimated_copied_bytes = 0

        if 'progress' in mongosync_sent_response_body:
            estimated_total_bytes = mongosync_sent_response_body['progress']['collectionCopy']['estimatedTotalBytes']
            estimated_copied_bytes = mongosync_sent_response_body['progress']['collectionCopy']['estimatedCopiedBytes']
        else:
            print("Key 'progress' not found in mongosync_sent_response_body")

        estimated_total_bytes, estimated_total_bytes_unit = format_byte_size(estimated_total_bytes)
        estimated_copied_bytes = convert_bytes(estimated_copied_bytes, estimated_total_bytes_unit)
        
        #print(estimated_total_bytes)
        #print(estimated_copied_bytes)

        # Create a subplot for the scatter plots and a separate subplot for the table
        fig = make_subplots(rows=8, cols=1, subplot_titles=("MongoSync Options", 
                                                            "MongoSync Hidden Options",
                                                            "Estimated Total and Copied " + estimated_total_bytes_unit,
                                                            "Total Events Applied",
                                                            "Collection Copy Source Read",
                                                            "Collection Copy Destination Write",
                                                            "CEA Source Read",
                                                            "CEA Destination Write",),
                            specs=[[{"type": "table"}], [{"type": "table"}], [{}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]])

        # Add the version information as an annotation to the plot
        #fig.add_annotation( x=0.5, y=1.05, xref="paper", yref="paper", text=version_text, showarrow=False, font=dict(size=12))

        #Add the Mongosync options
        fig.add_trace(table_trace, row=1, col=1)

        #Add the Mongosync options
        fig.add_trace(table_hiddenflags, row=2, col=1)

        # Create a bar chart
        #fig = go.Figure(data=[go.Bar(name='Estimated Total Bytes', x=['Bytes'], y=[estimated_total_bytes], row=1, col=1), go.Bar(name='Estimated Copied Bytes', x=['Bytes'], y=[estimated_copied_bytes])], row=1, col=1)
        fig.add_trace( go.Bar( name='Total - ' + estimated_total_bytes_unit,  x=[estimated_total_bytes_unit],  y=[estimated_total_bytes] ), row=3, col=1)
        fig.add_trace( go.Bar( name='Copied - ' + estimated_total_bytes_unit, x=[estimated_total_bytes_unit],  y=[estimated_copied_bytes]), row=3, col=1)

        # Add traces

        # Total Events Applied
        fig.add_trace(go.Scatter(x=times, y=totalEventsApplied, mode='lines', name='Total Events Applied'), row=3, col=1)
        fig.add_trace(go.Scatter(x=times, y=lagTimeSeconds, mode='lines', name='Lag Time Seconds'), row=3, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Total Events Applied", secondary_y=False, row=3, col=1)
        fig.update_yaxes(title_text="Lag Time Seconds", secondary_y=True, row=3, col=1)

        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead, mode='lines', name='Average (ms) - Collection Copy Source Read'), row=3, col=2)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead_maximum, mode='lines', name='Maximum (ms) - Collection Copy Source Read'), row=3, col=2)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead_numOperations, mode='lines', name='Operations - Collection Copy Source Read'), row=3, col=2, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max (ms)", secondary_y=False, row=3, col=2)
        fig.update_yaxes(title_text="Number of Operations", secondary_y=True, row=3, col=2)

        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite, mode='lines', name='Average (ms) - Collection Copy Destination Write'), row=4, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite_maximum, mode='lines', name='Maximum (ms) - Collection Copy Destination Write'), row=4, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite_numOperations, mode='lines', name='Operations - Collection Copy Destination Write'), row=4, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max (ms)", secondary_y=False, row=4, col=1)
        fig.update_yaxes(title_text="Number of Operations", secondary_y=True, row=4, col=1)

        fig.add_trace(go.Scatter(x=times, y=CEASourceRead, mode='lines', name='Average (ms) - CEA Source Read'), row=7, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEASourceRead_maximum, mode='lines', name='Maximum (ms) - CEA Source Read'), row=7, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEASourceRead_numOperations, mode='lines', name='Operations - CEA Source Read'), row=7, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max (ms)", secondary_y=False, row=7, col=1)
        fig.update_yaxes(title_text="Number of Operations", secondary_y=True, row=7, col=1)

        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite, mode='lines', name='Average (ms) - CEA Destination Write'), row=8, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite_maximum, mode='lines', name='Maximum (ms) - CEA Destination Write'), row=8, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite_numOperations, mode='lines', name='Operations - CEA Destination Write'), row=8, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max (ms)", secondary_y=False, row=8, col=1)
        fig.update_yaxes(title_text="Number of Operations", secondary_y=True, row=8, col=1)
        

        # Update layout
        fig.update_layout(height=1800, width=1250, title_text="Replication Progress - " + version_text)

        # Convert the figure to JSON
        plot_json = json.dumps(fig, cls=PlotlyJSONEncoder)

        # Render the plot in the browser
        return render_template_string('''
            <html>
                <head>
                    <title>Mongosync Metrics</title>
                </head>
            <body>
                    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                    <div id="plot"></div>
                    <script>
                    var plot = {{ plot_json | safe }};
                    Plotly.newPlot('plot', plot.data, plot.layout);
                    </script>
            </body>
        ''', plot_json=plot_json)
    
@app.route('/plot')
def serve_plot():
    file_path = os.path.join(app.static_folder, 'plot.png')
    print(file_path)  # print the file path

    if os.path.exists(file_path):
        return send_from_directory(app.static_folder, 'plot.png')
    else:
        return "File not found", 404

@app.route('/get_metrics_data', methods=['POST'])
def gatherMetrics():
    logging.basicConfig(filename='mongosync_monitor.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    TARGET_MONGO_URI = "mongodb+srv://poc:poc@syncmonitor.qteeb.mongodb.net/?retryWrites=true&w=majority&appName=syncMonitor&timeoutMS=10900000&connectTimeoutMS=10800000"
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
    fig = make_subplots(rows=8, cols=2, subplot_titles=("MongoSync State", 
                                                        "MongoSync Phase",
                                                        "MongoSync Start",
                                                        "MongoSync Finish",
                                                        "Collection Completed %",
                                                        "Total X Copied Data",
                                                        "Mongosync Phases",
                                                        "Collections Progress",))
                        #specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]])

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
        case _:
            vColor == "green"

    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(vState)], mode='text', name='Mongosync State',textfont=dict(size=30, color=vColor)), row=1, col=1)

    #Plot Mongosync Phase
    vPhase = vResumeData["syncPhase"]
    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(vPhase)], mode='text', name='Mongosync State',textfont=dict(size=30, color="black")), row=1, col=2)

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

    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(newInitial)], mode='text', name='Mongosync Start',textfont=dict(size=30, color="black")), row=2, col=1)
    
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

    fig.add_trace(go.Scatter(x=[0], y=[0], text=[str(newFinish)], mode='text', name='Mongosync Finish',textfont=dict(size=30, color="black")), row=2, col=2)

    #Plot partition data
    vMatch = {"$match": {"_id.fieldName": "collectionStats", "numCompletedPartitions": {"$gt": 0}, "$expr": {"$ne": ["$numCompletedPartitions", "$numPartitions"]}}}
    vLookup = {"$lookup": { "from": "uuidMap", "localField": "_id.uuid", "foreignField": "_id", "as": "collectionData"}}
    vAddFields1 = {"$addFields": {"db": {"$arrayElemAt": ["$collectionData.dstDBName",0]},"coll": {"$arrayElemAt": ["$collectionData.dstCollName",0]}}}
    vAddFields2 = {"$addFields": {"namespace": {"$concat": ["$db", ".", "$coll"]}, "PercCompleted": {"$divide": [{ "$multiply": ["$numCompletedPartitions", 100] }, "$numPartitions"]}}}
    vProject = {"$project":{"_id": 0, "namespace": 1, "PercCompleted": 1 }}
    vPartitionData = internalDbDst.statistics.aggregate([vMatch, vLookup, vAddFields1, vAddFields2, vProject])
    
    vPartitionData = list(vPartitionData)
    vNamespace = []
    vPercComplete = []
    if len(vPartitionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Mongosync Finish',textfont=dict(size=30, color="black")), row=3, col=1)
    else:        
        for partition in vPartitionData:
            vNamespace.append(partition["namespace"])
            vPercComplete.append(partition["PercCompleted"])
        fig.add_trace(go.Bar(x=vPercComplete, y=vNamespace, orientation='h', 
                             marker=dict(color=vPercComplete, colorscale='blugrn')), row=3, col=1)
        fig.update_xaxes(title_text="Completed %", row=3, col=1)
        fig.update_yaxes(title_text="Namespace", row=3, col=1)

    #Plot complete data
    vMatch = {"$match": {"_id.fieldName": "collectionStats"}}
    vProject = {"$project":{"_id": 0, "estimatedTotalBytes": 1, "estimatedCopiedBytes": 1 }}
    vCompleteData = internalDbDst.statistics.aggregate([vMatch, vProject])
    vCompleteData=list(vCompleteData)
    vCopiedBytes=0
    vTotalBytes=0
    vTypeByte=['Copied Data', 'Total Data']
    vBytes=[]
    if len(vCompleteData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=3, col=2)
    else:        
        for comp in list(vCompleteData):
            vCopiedBytes=comp["estimatedCopiedBytes"] + vCopiedBytes
            vTotalBytes=comp["estimatedTotalBytes"] + vTotalBytes
        vTotalBytes, estimated_total_bytes_unit = format_byte_size(vTotalBytes)
        vCopiedBytes = convert_bytes(vCopiedBytes, estimated_total_bytes_unit)
        vBytes.append(vCopiedBytes)
        vBytes.append(vTotalBytes)
        fig.add_trace(go.Bar(x=vBytes, y=vTypeByte, orientation='h',
                             marker=dict(color=vBytes, colorscale='redor')), row=3, col=2)
        fig.update_xaxes(title_text=f"Data in {estimated_total_bytes_unit}", row=3, col=2)
        fig.update_yaxes(title_text="Copied / Total Data", row=3, col=2)

    #PLOT LALALA
    vMatch = {"$match": {"_id.fieldName": "collectionStats"}}
    vAddFields = {"$addFields": {"notStarted": {"$cond": { "if": { "$eq": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}},"inProgress": {"$cond": { "if": { "$ne": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}}, "completed": {"$cond": { "if": { "$eq": ["$estimatedCopiedBytes", 0] }, "then": 1, "else": 0}}}}
    vGroup = {"$group": {"_id": None, "notStarted": {"$sum": "$notStarted"}, "inProgress": {"$sum": "$inProgress"}, "completed": {"$sum": "$completed"}}}
    vProject = {"$project":{"_id": 0, "notStarted": 1, "inProgress": 1,  "completed": 1}}
    vCollectionData = internalDbDst.statistics.aggregate([vMatch, vAddFields, vGroup, vProject])
    vCollectionData = list(vCollectionData)
    vTypeProc=[]
    vTypeValue=[]
    if len(vCollectionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=4, col=2)
    else:        
        for collec in vCollectionData:
            vTypeProc.append("notStarted")
            vTypeValue.append(collec["notStarted"])
            vTypeProc.append("inProgress")
            vTypeValue.append(collec["inProgress"])
            vTypeProc.append("completed")
            vTypeValue.append(collec["completed"])
        xMin = min(vTypeValue)
        xMax = max(vTypeValue)
        padding = int((xMax - xMin) * 0.2) if xMin != xMax else int(xMax * 0.2)
        if padding == 0:
            padding = 5
        fig.add_trace(go.Bar(x=vTypeValue, y=vTypeProc, orientation='h',
                             marker=dict(color=vTypeValue, colorscale='OrRd')), row=4, col=2)
        fig.update_xaxes(title_text=f"Totals", row=4, col=2)
        fig.update_yaxes(title_text="Process", row=4, col=2)

    #Plot phase transitions
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
    vTransitionData = internalDbDst.resumeData.aggregate([vMatch, vAddFields, vUnwind, vProject])
    vTransitionData=list(vTransitionData)
    vPhase=[]
    vTs=[]
    if len(vTransitionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=30, color="black")), row=4, col=1)
    else:        
        for phase in list(vTransitionData):
            vPhase.append(phase["phase"])
            vTs.append(phase["ts"])
        fig.add_trace(go.Scatter(x=vTs, y=vPhase, mode='markers+text',marker=dict(color='green')), row=4, col=1)


    fig.update_layout(xaxis1=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis1=dict(showgrid=False, zeroline=False, showticklabels=False),
                      xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False),
                      xaxis3=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis3=dict(showgrid=False, zeroline=False, showticklabels=False),
                      xaxis4=dict(showgrid=False, zeroline=False, showticklabels=False), 
                      yaxis4=dict(showgrid=False, zeroline=False, showticklabels=False),
                      xaxis5=dict(range=[1, 100], dtick=5), 
                      xaxis6=dict(range=[1, 100], dtick=10),
                      xaxis8=dict(range=[0, xMax + padding]), 
                      showlegend=False,
                      plot_bgcolor="white")
    
    # Update layout
    fig.update_layout(height=1800, width=1600, title_text="Replication Progress")
    
    # Convert the figure to JSON
    plot_json = json.dumps(fig, cls=PlotlyJSONEncoder)
    return plot_json

@app.route('/renderMetrics', methods=['POST'])
def plotMetrics():
    return render_template_string('''
        <html>
        <head>
            <title>Mongosync Metrics</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                #plot {
                    width: 1500px;
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