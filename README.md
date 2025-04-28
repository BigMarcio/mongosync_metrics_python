# mongosync_metrics_python

This project contains two Python scripts: `mongosync_plotly_multiple.py` and `mongosync_plotly_internal_DB.py`, which process the `mongosync` data and generate various plots using Plotly on port **3030**. The script also includes a Dockerfile for containerizing the application and a `requirements.txt` file listing the Python dependencies.

## mongosync_plotly_multiple.py

This Python script processes Mongosync logs and generates various plots using Plotly. The plots include scatter plots and tables, and they visualize different aspects of the data, such as `CEA Destination Write`, `Collection Copy Source Read`, and `Collection Copy Destination Write`.

The script uses the Plotly library for creating the plots and the pandas library for data manipulation. It also uses the datetime library for handling time data.

![Alt text for image 1](static/mongosync_log_analyzer.png)

## mongosync_plotly_internal_DB.py

This Python script processes Mongosync metadata and generates various plots using Plotly. The plots include scatter plots, and they visualize different aspects of the data, such as `Partitions Completed`, `Data Copied`, `Phases`, and `Collection Progress`.

![Alt text for image 1](static/mongosync_metadata.png)

## requirements.txt

The `requirements.txt` file lists the Python packages that the scripts depend on. The packages are specified with their version numbers to ensure compatibility.          

To install the dependencies, use the following command:

```bash
pip install -r requirements.txt
```

This command should be run in the Python environment where you want to run the script. If you're using a virtual environment, make sure to activate it first.

## Getting Started

1. Clone the repository to your local machine.
2. Navigate to the directory containing the Python script and the `requirements.txt` file.
3. Install the dependencies with `pip install -r requirements.txt`.
4. Run one of the Python scripts.

Please note that you need to have Python and pip installed on your machine to run the script and install the dependencies. If you want to use Docker, you also need to have Docker installed.

## Accessing the Application and Viewing Plots

Once the application is running, you can access it by opening a web browser and navigating to `http://localhost:3030`. This assumes that the application is running on the same machine where you're opening the browser, and that it's configured to listen on port 3030.

![Mongosync Logs Analyzer](static/mongosync_logs_home.png)

## Uploading the mongosync Log File

The application provides a user interface for uploading the `mongosync` log file. Clicking a "Browse" or "Choose File" button, select the file from your file system, and then click an "Open" or "Upload" button.

## Reading the Metadata

Before running the script `mongosync_plotly_internal_DB.py`, change the variable `TARGET_MONGO_URI` to use the target's connection string. 
Once the script is running, click the "Metrics Now" button and wait for the page to refresh.

## Viewing the Plot Information

Once the `mongosync` data is loaded, the application processes the data and generates the plots. 

If the plots aren't immediately visible after uploading the file, you may need to refresh the page. If the plots still aren't visible, check for any error messages or notifications from the application.

## Dockerfile

The Dockerfile is used to create a Docker image of the application. The Docker image includes the Python environment with all the necessary dependencies installed, as well as the Python script itself.

To build the Docker image, navigate to the directory containing the Dockerfile and run the following command:

```bash
docker build -t my-python-app .
```

To run the Docker container, use the following command:

```bash
docker run -it --rm --name my-running-app my-python-app
```
