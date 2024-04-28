import pytest
import boto3
from botocore.exceptions import ClientError

class TestGlueJob:
    @pytest.fixture(scope="class", autouse=True)
    def setup_glue_job(self):
        # Initialize the AWS Glue client
        self.glue_client = boto3.client('glue', region_name='your-region')

        # Define the Glue job name
        self.job_name = "your-glue-job-name"

        # Start the Glue job
        try:
            response = self.glue_client.start_job_run(JobName=self.job_name)
            self.job_run_id = response['JobRunId']
            print(f"Started Glue job: {self.job_name} with run ID: {self.job_run_id}")

            # Wait for the job to complete (simplistic approach)
            waiter = self.glue_client.get_waiter('job_run_succeeded')
            waiter.wait(JobName=self.job_name, RunId=self.job_run_id)
        except ClientError as e:
            pytest.fail(f"Failed to start or monitor Glue job: {e}")

    def test_glue_job_successful(self):
        # Check the job status
        try:
            job_status = self.glue_client.get_job_run(JobName=self.job_name, RunId=self.job_run_id)
            assert job_status['JobRun']['JobRunState'] == 'SUCCEEDED'
        except ClientError as e:
            pytest.fail(f"Failed to get job run details: {e}")

