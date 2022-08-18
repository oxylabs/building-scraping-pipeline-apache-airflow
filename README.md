# building-scraping-pipeline-apache-airflow
Using Apache Airflow to Build a Pipeline for Scraped Data 


```python
import requests

JOB_STATUS_DONE = 'done'

HTTP_NO_CONTENT = 204


class Client:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def create_jobs(self, urls):
        payload = {
            'source': 'universal_ecommerce',
            'url': urls
        }

        response = requests.request(
            'POST',
            'https://data.oxylabs.io/v1/queries/batch',
            auth=(self.username, self.password),
            json=payload,
        )

        return response.json()

    def is_status_done(self, job_id):
        job_status_response = requests.request(
            method='GET',
            url='http://data.oxylabs.io/v1/queries/%s' % job_id,
            auth=(self.username, self.password),
        )

        job_status_data = job_status_response.json()

        return job_status_data['status'] == JOB_STATUS_DONE

    def fetch_content_list(self, job_id):
        job_result_response = requests.request(
            method='GET',
            url='http://data.oxylabs.io/v1/queries/%s/results' % job_id,
            auth=(self.username, self.password),
        )
        if job_result_response.status_code == HTTP_NO_CONTENT:
            return None

        job_results_json = job_result_response.json()

        return job_results_json['results']
```

```sql
create sequence queue_seq;

create table queue (
  id int check (id > 0) primary key default nextval ('queue_seq'),
  created_at timestamp(0) not null DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp(0) not null DEFAULT CURRENT_TIMESTAMP,
  status varchar(255) not null DEFAULT 'pending',
  job_id varchar(255)
)

```

```python
import atexit

import psycopg2.extras

STATUS_PENDING = 'pending'
STATUS_COMPLETE = 'complete'
STATUS_DELETED = 'deleted'


class Queue:
    def __init__(self, connection):
        self.connection = connection

        atexit.register(self.cleanup)

    def setup(self):
        cursor = self.connection.cursor()

        cursor.execute('''
            select table_name
              from information_schema.tables
            where table_schema='public'
              and table_type='BASE TABLE'
        ''')
        for cursor_result in cursor:
            if cursor_result[0] == 'queue':
                print('Table already exists')
                return False

        cursor.execute('''
            create sequence queue_seq;

            create table queue (
              id int check (id > 0) primary key default nextval ('queue_seq'),
              created_at timestamp(0) not null DEFAULT CURRENT_TIMESTAMP,
              updated_at timestamp(0) not null DEFAULT CURRENT_TIMESTAMP,
              status varchar(255) not null DEFAULT 'pending',
              job_id varchar(255)
            )
        ''')

        return True

    def push(self, job_id):
        self.__execute_and_commit(
            'insert into queue (job_id) values (%s)',
            [job_id]
        )

    def pull(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute('start transaction')
        cursor.execute(
            '''
            select * from queue where status = %s and
            updated_at < now() - interval '10 second'
            order by random()
            limit 1
            for update
            ''',
            [STATUS_PENDING]
        )
        return cursor.fetchone()

    def delete(self, job_id):
        self.__change_status(job_id, STATUS_DELETED)

    def complete(self, job_id):
        self.__change_status(job_id, STATUS_COMPLETE)

    def touch(self, job_id):
        self.__execute_and_commit(
            'update queue set updated_at = now() where job_id = %s',
            [job_id]
        )

    def __change_status(self, job_id, status):
        self.__execute_and_commit(
            'update queue set status = %s where job_id = %s',
            [status, job_id]
        )

    def __execute_and_commit(self, sql, val):
        cursor = self.connection.cursor()
        cursor.execute(sql, val)

        self.connection.commit()

    def cleanup(self):
        self.connection.commit()
```

```sql
select * from queue where status = 'pending' and
updated_at < now() - interval '10 second'
order by random()
limit 1
for update
```

```python
    def __init__(self, connection):
        self.connection = connection

        atexit.register(self.cleanup)
    
    # ...
        
    def cleanup(self):
        self.connection.commit()
```

```python
import os

import psycopg2

from messenger import Queue
from oxylabs import Client

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASS = os.getenv('DB_PASS', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'scraper')
OXYLABS_USERNAME = os.getenv('OXYLABS_USERNAME', 'your-oxylabs-username')
OXYLABS_PASSWORD = os.getenv('OXYLABS_PASSWORD', 'your-oxylabs-password')

connection = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)

queue = Queue(
    connection
)

client = Client(
    OXYLABS_USERNAME,
    OXYLABS_PASSWORD,
)
```

```python
from bootstrap import queue

success = queue.setup()
if not success:
    exit(1)
```

```python
from bootstrap import queue

success = queue.setup()
if not success:
    exit(1)
```

The `exit(1)` on failure is extremely important, as it signifies that the process has not completed successfully.

Once the schema is created, we can **push** a collection of jobs in the Oxylabs Batch Query endpoint. 

```python
from bootstrap import queue, client

jobs = client.create_jobs([
    'https://books.toscrape.com/catalogue/sapiens-a-brief-history-of-humankind_996/index.html',
    'https://books.toscrape.com/catalogue/sharp-objects_997/index.html',
    'https://books.toscrape.com/catalogue/soumission_998/index.html',
    'https://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html',
    'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html',
])

for job in jobs['queries']:
    queue.push(job['id'])
    print('job id: %s' % job['id'])
```

```python
from pprint import pprint
from bootstrap import queue, client

queue_item = queue.pull()
if not queue_item:
    print('No jobs left in the queue, exiting')
    exit(0)

if not client.is_status_done(queue_item['job_id']):
    queue.touch(queue_item['job_id'])
    print('Job is not yet finished, skipping')
    exit(0)

content_list = client.fetch_content_list(queue_item['job_id'])
if content_list is None:
    print('Job no longer exists in oxy')
    queue.delete(queue_item['job_id'])
    exit(0)

queue.complete(queue_item['job_id'])

for content in content_list:
    pprint(content)
```

```python
queue_item = queue.pull()
if not queue_item:
    print('No jobs left in the queue, exiting')
    exit(0)
```

```python
if not client.is_status_done(queue_item['job_id']):
    queue.touch(queue_item['job_id'])
    print('Job is not yet finished, skipping')
    exit(0)
```

```python
content_list = client.fetch_content_list(queue_item['job_id'])
if content_list is None:
    print('Job no longer exists in oxy')
    queue.delete(queue_item['job_id'])
    exit(0)
```

```python
queue.complete(queue_item['job_id'])

for content in content_list:
    pprint(content)
```

```yaml
|-- src
|   |-- bootstrap.py
|   |-- messenger.py
|   |-- oxylabs.py
|   |-- puller.py
|   |-- pusher.py
|   |-- setup.py

```

```yaml
volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
```

```yaml
volumes:
    - ./src:/opt/airflow/src
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
```

```yaml
|-- dags
|-- docker-compose.yaml
|-- .env
|-- logs
|-- plugins
|-- src
|   |-- bootstrap.py
|   |-- messenger.py
|   |-- oxylabs.py
|   |-- puller.py
|   |-- pusher.py
|   |-- setup.py
```

```yaml
|-- dags
|   |-- scrape.py
|-- docker-compose.yaml
|-- .env
|-- logs
|-- plugins
|-- src
|   |-- bootstrap.py
|   |-- messenger.py
|   |-- oxylabs.py
|   |-- puller.py
|   |-- pusher.py
|   |-- setup.py

```

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(hours=3),
}
with DAG(
        'setup',
        default_args=default_args,
        schedule_interval='@once',
        description='Setup',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        dagrun_timeout=timedelta(minutes=1),
        tags=['scrape', 'database'],
        catchup=False
) as dag:
    setup_task = BashOperator(
        task_id='setup',
        bash_command='python /opt/airflow/src/setup.py',
    )
```

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(hours=3),
}
```

```python
with DAG(
        'setup',
        default_args=default_args,
        schedule_interval='@once',
        description='Setup',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        dagrun_timeout=timedelta(minutes=1),
        tags=['scrape', 'database'],
        catchup=False
) as dag:
```

```python
    setup_task = BashOperator(
        task_id='setup',
        bash_command='python /opt/airflow/src/setup.py',
    )
```

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(hours=3),
}
with DAG(
        'push_pull',
        default_args=default_args,
        schedule_interval='@daily',
        description='Push-Pull workflow',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        dagrun_timeout=timedelta(minutes=1),
        tags=['scrape', 'database'],
        catchup=False
) as dag:
    task_push = BashOperator(
        task_id='push',
        bash_command='python /opt/airflow/src/pusher.py',
    )

    task_pull = BashOperator(
        task_id='pull',
        bash_command='python /opt/airflow/src/puller.py'
    )

    task_push.set_downstream(task_pull)
```

```python
    task_push = BashOperator(
        task_id='push',
        bash_command='python /opt/airflow/src/pusher.py',
        schedule_interval='daily',  # not allowed!
    )

    task_pull = BashOperator(
        task_id='pull',
        bash_command='python /opt/airflow/src/puller.py',
        schedule_interval='@hourly', # not allowed!
    )

```

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(hours=3),
}
with DAG(
        'push-pull-reworked',
        default_args=default_args,
        schedule_interval='* * * * *',
        description='Scrape the website',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        dagrun_timeout=timedelta(minutes=1),
        tags=['scrape', 'oxylabs', 'push', 'pull'],
        catchup=False
) as dag:
    def is_midnight(logical_date):
        return logical_date.hour == 0 and logical_date.minute == 0

    trigger_once_per_day = ShortCircuitOperator(
        task_id='once_per_day',
        python_callable=is_midnight,
        provide_context=True,
        dag=dag
    )

    task_push = BashOperator(
        task_id='push',
        bash_command='python /opt/airflow/src/pusher.py',
    )
    trigger_once_per_day.set_downstream(task_push)

    task_pull = BashOperator(
        task_id='pull',
        bash_command='python /opt/airflow/src/puller.py'
    )
```

```python
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(hours=3),
}
with DAG(
        'scrape',
        default_args=default_args,
        schedule_interval='* * * * *',
        description='Scrape the website',
        start_date=pendulum.datetime(2022, 5, 1, tz='UTC'),
        dagrun_timeout=timedelta(minutes=1),
        tags=['scrape', 'oxylabs', 'push', 'pull'],
        catchup=False
) as dag:
    trigger_always = ShortCircuitOperator(
        task_id='always',
        python_callable=lambda prev_start_date_success: prev_start_date_success is not None,
        provide_context=True,
        dag=dag
    )

    trigger_once = ShortCircuitOperator(
        task_id='once',
        python_callable=lambda prev_start_date_success: prev_start_date_success is None,
        provide_context=True,
        dag=dag
    )

    setup_task = BashOperator(
        task_id='setup',
        bash_command='python /opt/airflow/src/setup.py',
    )

    trigger_once.set_downstream(setup_task)
def is_midnight(logical_date):
        return logical_date.hour == 0 and logical_date.minute == 0

    trigger_once_per_day = ShortCircuitOperator(
        task_id='once_per_day',
        python_callable=is_midnight,
        provide_context=True,
        dag=dag
    )

    task_push = BashOperator(
        task_id='push',
        bash_command='python /opt/airflow/src/pusher.py',
    )
    trigger_once_per_day.set_downstream(task_push)

    task_pull = BashOperator(
        task_id='pull',
        bash_command='python /opt/airflow/src/puller.py'
    )

    trigger_always.set_downstream(task_pull)
    trigger_always.set_downstream(trigger_once_per_day)

```
