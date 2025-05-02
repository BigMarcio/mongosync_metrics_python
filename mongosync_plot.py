from flask import Flask, request, redirect, url_for, render_template_string, send_from_directory
from mongosync_plot_logs import upload_file

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
def uploadLogs():
    return upload_file()

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=3030)
