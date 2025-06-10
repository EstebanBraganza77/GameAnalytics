# GameAnalytics

This repository contains an end-to-end game analytics pipeline from data generation to visualization using AWS services including Kinesis, S3, Glue, Athena, and Quicksight. This project is the final assignment for the Big Data course in the Data Science master's degree at UOC (Universitat Oberta de Catalunya).

The project presentation can be watched at the following link:  
[https://www.youtube.com/watch?v=23LWfXlJ5hc&t=363s](https://www.youtube.com/watch?v=23LWfXlJ5hc&t=363s)

---

## Project Overview

![Architecture](docs/arquitectura.png)

The pipeline consists of three main components/scripts:

1. **Ingestion Layer**  
   Generates fake gaming events data and streams it into AWS Kinesis.

2. **Glue Job (PySpark)**  
   ETL job that processes data from the Bronze layer (raw events) to the Silver layer (cleaned and filtered datasets).  
   *(The code for this script is included in this repository.)*

3. **Submission Jupyter Notebook**  
   Demonstrates analysis and querying of the Silver layer datasets, showcasing insights and examples.

Additionally, a PowerPoint presentation summarizing the project is included.

---

## Repository Structure

- `ingestion.py`  
  Code to generate and stream fake game events data.

- `glue_jobs.py`  
  PySpark Glue job script to transform Bronze data into Silver tables.

- `notebooks/`  
  Jupyter notebook for querying and analyzing Silver layer data.

- `presentation/`  
  PowerPoint slides for the project presentation.

---

## How to Use

1. **Run the ingestion script** to simulate event generation into the Kinesis stream.

2. **Deploy and run the Glue job** to transform and clean raw data from the Bronze layer to the Silver layer stored in S3.

3. **Use Athena or the Jupyter notebook** to query and analyze the cleaned data.

4. **Visualize insights** with Amazon QuickSight connected to your Silver layer tables.

---

## Technologies Used

- AWS Kinesis  
- AWS S3  
- AWS Glue (PySpark)  
- AWS Athena  
- AWS QuickSight  
- Apache Spark  
- Python / PySpark  

---

## Contact

For questions or support, please contact:  
[Your Name] - [Your Email]

---

*This project was completed as part of the Data Science master’s degree at UOC.*




