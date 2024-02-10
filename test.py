import unittest
import json
from pyspark.sql import SparkSession
from your_script import get_dates, get_iem_report_df, get_SCMFReportDF, get_message, send_email

class TestScript(unittest.TestCase):
    def setUp(self):
        with open('config.json', 'r') as f:
            self.config = json.load(f)
        self.spark = SparkSession.builder \
            .appName("Test") \
            .getOrCreate()

    def test_get_dates(self):
        dates = get_dates(self.config['iem_days'], self.config['upper_bound_days'], self.config['scm_days'])
        self.assertEqual(len(dates), 3)
        self.assertIn('iem_date', dates)
        self.assertIn('upper_bound_date', dates)
        self.assertIn('scm_date', dates)

    def test_get_iem_report_df(self):
        dates = get_dates(self.config['iem_days'], self.config['upper_bound_days'], self.config['scm_days'])
        df = get_iem_report_df(self.spark, dates['iem_date'], dates['upper_bound_date'])
        self.assertTrue(df.count() > 0)

    def test_get_SCMFReportDF(self):
        dates = get_dates(self.config['iem_days'], self.config['upper_bound_days'], self.config['scm_days'])
        df = get_SCMFReportDF(self.spark, dates['scm_date'])
        self.assertTrue(df.count() > 0)

    def test_get_message(self):
        dates = get_dates(self.config['iem_days'], self.config['upper_bound_days'], self.config['scm_days'])
        iem_report_df = get_iem_report_df(self.spark, dates['iem_date'], dates['upper_bound_date'])
        SCMFReportDF = get_SCMFReportDF(self.spark, dates['scm_date'])
        message = get_message(iem_report_df, SCMFReportDF)
        self.assertIsNotNone(message)

    def test_send_email(self):
        subject = "Test Email"
        message = "This is a test email."
        send_email(subject, message, self.config['dry_run'])

if __name__ == '__main__':
    unittest.main()
