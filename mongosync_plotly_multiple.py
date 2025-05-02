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
                    <img src="static/mongosync_log_analyzer.png" width="624" height="913">
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

            # Create a table trace with the keys as the first column and the corresponding values as the second column
            table_hiddenflags = go.Table(
                header=dict(values=['Key', 'Value'], font=dict(size=12, color='black')),
                cells=dict(values=[keys, values],  align=['left'], font=dict(size=10, color='darkblue')), #
                columnwidth=[0.75, 2.5]  # Adjust the column widths as needed
            )
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

        #Getting the Timezone
        #print (data[0]['time'])
        datetime_with_timezone = datetime.fromisoformat(data[0]['time'])  
        timeZoneInfo = datetime_with_timezone.strftime("%Z")

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

        # Create a subplot for the scatter plots and a separate subplot for the table
        fig = make_subplots(rows=8, cols=1, subplot_titles=("Estimated Total and Copied " + estimated_total_bytes_unit,
                                                            "Events Applied x Lag Time",
                                                            "Collection Copy Source Read",
                                                            "Collection Copy Destination Write",
                                                            "CEA Source Read",
                                                            "CEA Destination Write",
                                                            "MongoSync Options", 
                                                            "MongoSync Hidden Options",),
                            specs=[ [{}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"type": "table"}], [{"type": "table"}] ])

        # Add traces

        # Create a bar chart
        #fig = go.Figure(data=[go.Bar(name='Estimated Total Bytes', x=['Bytes'], y=[estimated_total_bytes], row=1, col=1), go.Bar(name='Estimated Copied Bytes', x=['Bytes'], y=[estimated_copied_bytes])], row=1, col=1)
        fig.add_trace( go.Bar( name='Estimated ' + estimated_total_bytes_unit + ' to be Copied',  x=[estimated_total_bytes_unit],  y=[estimated_total_bytes], legendgroup="groupTotalCopied" ), row=1, col=1)
        fig.add_trace( go.Bar( name='Estimated Copied ' + estimated_total_bytes_unit, x=[estimated_total_bytes_unit],  y=[estimated_copied_bytes], legendgroup="groupTotalCopied"), row=1, col=1)

        # Total Events Applied
        fig.add_trace(go.Scatter(x=times, y=totalEventsApplied, mode='lines', name='Change Events Applied', legendgroup="groupEventsAndLags"), row=2, col=1)
        fig.add_trace(go.Scatter(x=times, y=lagTimeSeconds, mode='lines', name='Lag Time (seconds)', legendgroup="groupEventsAndLags"), row=2, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Change Events Applied", secondary_y=False, row=2, col=1)
        fig.update_yaxes(title_text="Lag Time (seconds)", secondary_y=True, row=2, col=1)

        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead, mode='lines', name='Average read time (ms) during Collection Copy', legendgroup="groupCCSourceRead"), row=3, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead_maximum, mode='lines', name='Maximum read time (ms) during Collection Copy', legendgroup="groupCCSourceRead"), row=3, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopySourceRead_numOperations, mode='lines', name='Reads during Collection Copy', legendgroup="groupCCSourceRead"), row=3, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max time (ms)", secondary_y=False, row=3, col=1)
        fig.update_yaxes(title_text="Number of Reads", secondary_y=True, row=3, col=1)

        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite, mode='lines', name='Average write time (ms) during Collection Copy', legendgroup="groupCCDestinationWrite"), row=4, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite_maximum, mode='lines', name='Maximum (ms) during Collection Copy', legendgroup="groupCCDestinationWrite"), row=4, col=1)
        fig.add_trace(go.Scatter(x=times, y=CollectionCopyDestinationWrite_numOperations, mode='lines', name='Writes during Collection Copy', legendgroup="groupCCDestinationWrite"), row=4, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max time (ms)", secondary_y=False, row=4, col=1)
        fig.update_yaxes(title_text="Number of Writes", secondary_y=True, row=4, col=1)

        fig.add_trace(go.Scatter(x=times, y=CEASourceRead, mode='lines', name='Average read time (ms) during CEA', legendgroup="groupCEASourceRead"), row=5, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEASourceRead_maximum, mode='lines', name='Maximum read time (ms) during CEA', legendgroup="groupCEASourceRead"), row=5, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEASourceRead_numOperations, mode='lines', name='Reads during CEA', legendgroup="groupCEASourceRead"), row=5, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max time (ms)", secondary_y=False, row=5, col=1)
        fig.update_yaxes(title_text="Number of Reads", secondary_y=True, row=5, col=1)

        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite, mode='lines', name='Average write time (ms) during CEA', legendgroup="groupCEADestinationWrite"), row=6, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite_maximum, mode='lines', name='Maximum write time (ms) during CEA', legendgroup="groupCEADestinationWrite"), row=6, col=1)
        fig.add_trace(go.Scatter(x=times, y=CEADestinationWrite_numOperations, mode='lines', name='Writes during CEA', legendgroup="groupCEADestinationWrite"), row=6, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Avg and Max time (ms)", secondary_y=False, row=6, col=1)
        fig.update_yaxes(title_text="Number of Writes", secondary_y=True, row=6, col=1)

        #Add the Mongosync options
        fig.add_trace(table_trace, row=7, col=1)

        #Add the Mongosync options
        fig.add_trace(table_hiddenflags, row=8, col=1)

        # Update layout
        fig.update_layout(height=1800, width=1250, title_text="Mongosync Replication Progress - " + version_text + " - Timezone info: " + timeZoneInfo, legend_tracegroupgap=170)

        fig.update_layout(
            legend=dict(
                y=1
            )
        )


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

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=3030)