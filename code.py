import sys
import time
import traceback
import logging
import json
from datetime import datetime, timedelta
from os.path import abspath

from pyspark.sql import SparkSession

# Add the path to import sendEmail module
sys.path.insert(0, '/users/prieappiem/IEM/conf')
from sendEmail import sendErrorEmail

# Constants
WAREHOUSE_LOCATION = abspath('hdfs://prod0-edl/prod/01559/app/RIE0/hivedb')
RECIPIENTS = ['abul.fahad@rbc.com']

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Spark Session
spark = SparkSession.builder \
    .appName("IEM Hive Spark SQL") \
    .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION) \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.parquet.binaryAsString", "true")
spark.conf.set("spark.sql.caseSensitive", "true")

def get_config():
    with open('config.json', 'r') as f:
        return json.load(f)

def get_dates(iem_days=9, upper_bound_days=1, scm_days=7):
    return {
        'iem_date': (datetime.today() - timedelta(days=iem_days)).strftime("%Y%m%d"),
        'upper_bound_date': (datetime.today() - timedelta(days=upper_bound_days)).strftime("%Y%m%d"),
        'scm_date': (datetime.today() - timedelta(days=scm_days)).strftime("%Y%m%d")
    }

def get_iem_report_df(iem_date, upper_bound_date):
    return spark.sql(f"""
    SELECT partition_date, COUNT(*) AS count
    FROM lai0___intelligent_email_management_system
    WHERE partition_date >= '{iem_date}' AND partition_date <= '{upper_bound_date}'
    GROUP BY partition_date
    ORDER BY partition_date ASC
    """)

def get_SCMFReportDF(scm_date):
    return spark.sql(f"""
    SELECT to_date(scm.eventattributes['modifiedon']) as modified_date, partition_date, COUNT(*) as count_email_modifiedon 
    FROM wrv0___sales_and_contact_management_scm AS scm 
    WHERE scm.partition_date >= '{scm_date}' and to_date(scm.eventattributes['modifiedon']) is not null 
    GROUP BY to_date(scm.eventattributes['modifiedon']), partition_date 
    ORDER BY to_date(scm.eventattributes['modifiedon']) DESC
    """)

def get_message(iem_report_df, SCMFReportDF):
    message_iem = f"IEM ALERT: there are missing partition_dates: {iem_report_df}" if iem_report_df.count() < 9 else f"IEM: {iem_report_df}"
    message_scm = f"SCM: {SCMFReportDF}"
    return f"{message_iem}\n\n{message_scm}"

def send_email(subject, message, dry_run):
    logging.info(message)
    if not dry_run:
        sendErrorEmail(RECIPIENTS, subject, message)
    logging.info("Email Sent")

def main():
    config = get_config()

    try:
        start_time = time.time()

        logging.info("Starting Spark SQL session...")
        spark.sql("use prod_brt0_ess")

        logging.info("Getting dates...")
        dates = get_dates(config['iem_days'], config['upper_bound_days'], config['scm_days'])

        logging.info("Getting IEM report dataframe...")
        iem_report_df = get_iem_report_df(dates['iem_date'], dates['upper_bound_date'])

        logging.info("Getting SCMF report dataframe...")
        SCMFReportDF = get_SCMFReportDF(dates['scm_date'])

        logging.info("Preparing email message...")
        subject = f"[Data Engineering] SCM-ESS Hive Report : lai0___intelligent_email_management_system and wrv0___sales_and_contact_management_scm - starting date: {dates['scm_date']}"
        message = get_message(iem_report_df, SCMFReportDF)

        logging.info("Sending email...")
        send_email(subject, message, config['dry_run'])

        logging.info(f'Job finished in {time.time()-start_time:.2f} seconds')

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")

        subject = "Error in SCM-ESS Hive Report"
        message = f"An error occurred: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        send_email(subject, message, config['dry_run'])

if __name__ == "__main__":
    main()
