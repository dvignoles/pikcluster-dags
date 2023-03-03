from datetime import timedelta
import pendulum
from pathlib import Path
import requests
import gzip

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

@dag(
    schedule=timedelta(minutes=15),
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=['s3', 'citibike', 'sdss'],
)
def gbfs_to_s3():
    """Download station status from GBFS and upload to s3"""
    @task(multiple_outputs=True)
    def download() -> dict:
        now = pendulum.now('America/New_York').strftime('%Y-%m-%d_%H:%M:%S')
        out = Path(f'/tmp/{now}.json.gz')
        status = requests.get('https://gbfs.citibikenyc.com/gbfs/en/station_status.json')
        print(status.status_code)

        with gzip.open(out, 'wt') as f:
            f.write(status.text)

        s3_name = f's3://citibike-sdss/data/raw/station_status/{out.name}'

        return {'path': str(out), 's3_name':s3_name}

    
    downloaded_file = download()

    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        aws_conn_id='aws_sdss',
        filename=downloaded_file['path'],
        dest_key=downloaded_file['s3_name'],
        replace=True,
    )

    @task()
    def cleanup(dummy):
        to_del = Path('/tmp').glob('*.json.gz')
        for f in to_del:
            f.unlink()
            print(f'{f} removed')

    _cleaned = cleanup(create_local_to_s3_job.output)

gbfs_to_s3()
