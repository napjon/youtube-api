# Overview

There are several files in this directory

* `analysis.ipynb` is the Jupyter Notebook that built the actual script
* `analysis.html` is the HTML exported version, so you can just read static HTML, instead of spawn Jupyter server. The analysis and questions answered is listed in the HTML file
* `tags.xlsx` The tags file
* `analysis.py` the actual script, please read below.
* `requirements.txt` Python packages dependencies used


## Actual Script

This script can be used run continuously ETL pipeline.

Before running the script, install the python packages dependencies

```
$pip install -r requirements.txt
```

Change the global variables, including

* `API_KEY` : Create the api key credentials in GCP. Go to Youtube Data V3 API -> Enable API -> API & Credentials -> Create API Key -> copy the api key
* `MySQL_URI`: Change the URI to reflect MySQL destination table
* `KEYWORD`: Change the keyword

Run the script

```
$python analysis.py
```