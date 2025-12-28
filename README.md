# Rush

A distributed pipeline execution system for managing and executing multi-step data processing pipelines with job orchestration, retry logic, and worker coordination.

## Overview

Rush is a Python-based distributed job execution framework that manages complex data pipelines across multiple worker nodes. It provides robust job scheduling, automatic retries, timeout handling, and master-worker coordination through a PostgreSQL-backed state management system.

## Architecture

| ![Rush Architecturn](resource/rush.pdf) |
| :-------------------------------------: |

### Key Components

- **Workers**: Distributed processes that execute jobs assigned to them
- **Master Election**: One worker becomes the master to coordinate job assignment
- **Pipeline Manager**: Manages pipeline definitions and job orchestration
- **State Machine**: Tracks job lifecycle through states (Waiting → Ready → Running → Complete/Failed)
- **PostgreSQL Database**: Central store for pipelines, jobs, workers, and execution state

## Features

- **Distributed Execution**: Multiple workers can run in parallel across different machines
- **Pipeline Orchestration**: Define multi-step pipelines where jobs execute sequentially
- **Automatic Retry**: Failed jobs are automatically retried with configurable limits and delays
- **Timeout Handling**: Jobs that exceed their timeout are automatically marked as failed
- **Job Parameters**: Pass configuration parameters to jobs via the database
- **Master Coordination**: Master worker assigns ready jobs to free workers
- **Persistent State**: All execution state stored in PostgreSQL for resilience
- **Dynamic Job Loading**: Jobs are loaded dynamically by class name

## Project Structure

```
rush/
├── rush-worker/          # Worker application
│   ├── main.py          # Entry point
│   ├── worker.py        # Worker execution loop
│   ├── scheduler.py     # Periodic job scheduler
│   ├── configurator.py  # Logging and worker setup
│   ├── domain/          # Domain models
│   │   ├── pipeline.py     # Pipeline and job definitions
│   │   ├── state.py        # State models (RunState, Worker, etc.)
│   │   └── pipeline_manager.py  # Pipeline orchestration logic
│   ├── repository/      # Data access layer
│   │   ├── sql_repo.py      # Base SQL repository
│   │   └── pipeline_repo.py # Pipeline-specific queries
│   ├── executionsteps/  # Worker execution steps
│   │   ├── master.py           # Master tasks (job assignment)
│   │   ├── get_allocated_job.py # Retrieve assigned job
│   │   ├── run_job.py          # Execute the job
│   │   └── complete_job.py     # Update job status
│   ├── jobs/            # Job implementations
│   │   ├── job.py          # Base job class
│   │   ├── dummy_job.py    # Example job
│   │   └── move_to_db.py   # Database job example
│   └── scripts/         # Data processing scripts
└── rush-db/             # Database schema
    ├── pipeline.sql
    ├── run_state.sql
    ├── worker.sql
    ├── master_token.sql
    ├── run_request.sql
    ├── job_parameter.sql
    └── archive.sql
```

## Installation

### Prerequisites

- Python 3.13+
- PostgreSQL database

### Setup

1. **Clone the repository**

   ```bash
   cd rush
   ```

2. **Install dependencies**

   ```bash
   cd rush-worker
   pip install -r requirements.txt
   ```

3. **Setup the database**

   Create a PostgreSQL database:

   ```bash
   createdb rushdb
   ```

   Run the schema scripts:

   ```bash
   cd rush-db
   psql -d rushdb -f pipeline.sql
   psql -d rushdb -f run_state.sql
   psql -d rushdb -f worker.sql
   psql -d rushdb -f master_token.sql
   psql -d rushdb -f run_request.sql
   psql -d rushdb -f job_parameter.sql
   psql -d rushdb -f archive.sql
   ```

4. **Configure database connection**

   Update `rush-worker/repository/sql_repo.py` with your database credentials:

   ```python
   self.connection = psycopg2.connect(
       host="localhost",
       database="rushdb",
       user="your_username",
       password="your_password"
   )
   ```

## Usage

### Starting a Worker

```bash
cd rush-worker
python main.py <worker_name>
```

Example:

```bash
python main.py worker1
```

The worker name is optional and defaults to `thread1`. Multiple workers can be started with different names.

### Defining a Pipeline

Pipelines are defined as JSON in the `pipeline` table:

```sql
INSERT INTO pipeline (name, definition) VALUES (
  'example_pipeline',
  '{
    "jobs": [
      {
        "name": "extract_data",
        "class_name": "jobs.dummy_job.DummyJob",
        "timeout_minutes": 30,
        "retry_gap_minutes": 5,
        "retry_limit": 3
      },
      {
        "name": "transform_data",
        "class_name": "jobs.move_to_db.MoveToDb",
        "timeout_minutes": 60,
        "retry_gap_minutes": 10,
        "retry_limit": 2
      }
    ]
  }'
);
```

### Creating a Job

Jobs inherit from the `Job` base class:

```python
from jobs.job import Job

class MyCustomJob(Job):
    def run(self):
        # Access parameters
        param_value = self.params.get('my_param')

        # Your job logic here
        self.logger.info(f"Executing job with param: {param_value}")

        # Job completes successfully when run() finishes
        # Exceptions will mark the job as failed
```

### Triggering a Pipeline

Insert a run request:

```sql
INSERT INTO run_request (pipeline, job, jobinstance, status)
VALUES ('example_pipeline', 'extract_data', 1, 'Pending');
```

The master worker will pick up the request and start the pipeline.

## Job Lifecycle

Jobs progress through the following states:

1. **Waiting**: Job is waiting for its scheduled ready time
2. **Ready**: Job is ready to be assigned to a worker
3. **Running**: Job is currently executing on a worker
4. **Complete**: Job finished successfully
5. **Failed**: Job failed or timed out (will be retried if retries remain)
6. **Stuck**: Job has exhausted all retries

## Worker Execution Flow

Each worker runs a periodic loop every 5 seconds:

1. **Master Tasks** (if master):
   - Process completed jobs (trigger next job in pipeline)
   - Handle timed out jobs
   - Retry failed jobs
   - Process run requests
   - Move waiting jobs to ready
   - Assign ready jobs to free workers

2. **Get Allocated Job**: Retrieve the job assigned to this worker

3. **Run Job**: Execute the job class with its parameters

4. **Complete Job**: Update the job status in the database

## Database Schema

### Core Tables

- **pipeline**: Stores pipeline definitions (JSON)
- **run_state**: Current state of running jobs
- **worker**: Registered workers and their status
- **run_request**: Pending pipeline execution requests
- **job_parameter**: Key-value parameters for jobs
- **archive**: Historical record of completed/failed jobs
- **master_token**: Master election coordination

## Configuration

- **Worker polling interval**: 5 seconds (configured in `scheduler.py`)
- **Master token expiry**: 2 minutes (configured in `pipeline_repo.py`)
- **Database connection**: Configured in `sql_repo.py`
- **Logging**: Logs written to `logs/<worker_name>.log`

## Dependencies

- **mrjob** (0.7.4): MapReduce framework
- **psycopg2** (2.8.6): PostgreSQL adapter
- **PyYAML** (5.4.1): YAML parser

## Development

### Adding a New Job

1. Create a new Python file in `rush-worker/jobs/`
2. Extend the `Job` base class
3. Implement the `run()` method
4. Reference it in your pipeline definition by its full class path

### Running Multiple Workers

Start multiple workers to enable distributed execution:

```bash
# Terminal 1
python main.py worker1

# Terminal 2
python main.py worker2

# Terminal 3
python main.py worker3
```

One worker will become the master and coordinate job assignments.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
