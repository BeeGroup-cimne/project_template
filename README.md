# CREATE A DATA ANALYSIS PROJECT

## Follow this 10 steeps:

1- Copy the project_template in the "celery" folder.

2- Delete the `.git` folder of the template project and start a new git for the project
```bash
rm -rf .git
git init
```
3- Set the `config.json` with the proper configuration
```json
{
	"mongodb" : {
		"host": "hostname",
		"port": 1234,
		"username": "username",
		"password": "password",
		"db": "database"
	},
	"hdfs" : {
		"host": "hostname",
		"port": 1234
	},
	"hive" : {
		"host": "hostname",
		"port": 1234,
		"username": "username"
	},
	"hbase" : {
		"host": "hostname",
		"port": 1234
	},
	"report": {
		"collection": "module_task_reports",
		"etl_collection": "etl_task_reports",
		"timeline_collection": "etl_automatic_launch_timeline"
	},
	"config_execution": {
		"path": "tmp_path/config_execution",
		"tertiary_periods" : "tertiary_periods",
		"tou_periods": "tou_periods",
		"last_contracts_ETL": "last_execution_contracts_ETL",
		"streaming_jar": "/usr/lib/hadoop/hadoop-streaming.jar"
	}
}
```

4- Set the variables in `general_variables.sh`
```bash
#! /bin/bash

# virtualenv exec path
export virtualenv_path=/path/to/virtualenv

# pip server certificate
export cert=/path/to/devpi/certificate.pem

```

5- Copy and edit the example module folder `_module_template`. If some common libraries has to be included in the project, they can be included in a folder starting with "_" and then setting it's path to the `python_path` config variable.

6- Write the code of the new module. All mapreduce required files must be in the same folder or installed in the virtualenv through the `requirements.txt` file

7- Set the module `config.json`if required(if it includes the same information as parent `config.json` it will override this information)

8- Set the variables in `module_variables.sh`
```bash
#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=<task file executable>

# module name
task_name=<name of the module(dir name)>

#python to use
python_v=<python executable>

#pythonpath to add (list)
# If an external folder outside the module has to be included in the mrjob
# all folders in te project that are not modules must start with _
python_path=(
    <path_to_add_1>
    <path_to_add_2>
)

# export variables
# If some environtments variables are needed for the execution: EX: R_HOME for rpy2
to_export=(
    <variable_to_add>=<value>
)

# celery queue to add this task
queue=<queue: (etl, module)>

#whether the virtualenv should be (re)installed each execution or not
debug=<(0,1)>

#current dir path
pwd=`pwd`
```

9- Install the modules by running `. install.sh` in the parent directory.

10- Add the module/tasks.py to celery router.
