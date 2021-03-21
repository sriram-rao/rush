# Rush
A pipeline execution system named after [these musical geniuses](https://www.rush.com).

### Defining a pipeline
Pipelines are a combination of sub-tasks referred to as "jobs". Each job (or sub-task) may have settings or parameters as well. 

Pipelines are defined in JSON. Here is an example pipeline JSON:
```
{
  "jobs": [
    {
      "name": "A readable name",
      "class_name": "fully.classified.class_name",
      "timeout_minutes": 10,
      "retry_gap_minutes": 5,
      "retry_limit": 2
    },
    {
      "name": "BlahJob",
      "class_name": "jobs.dummyJob.DummyJob",
      "timeout_minutes": 10,
      "retry_gap_minutes": 5,
      "retry_limit": 2
    }
  ]
}
```

### Writing a job
A job (i.e. a sub-task of the pipeline) is expected to be a class and needs to have a callable run() method. It can inherit from jobs.job.Job (could that BE any more redundant?) to take advantage of configured job settings being passed in and the file logger.  

### Setting up to run the engine
The code has been written and tested using python3. Make sure the packages are installed. 

Create a database with the tables from the rush-db directory. Only the master_token table needs seed data (just one row, actually).

### Running the engine
Configure the PostgreSQL information and run the main.py file, with an optional "worker name" command line argument.

```python main.py neil_peart```
