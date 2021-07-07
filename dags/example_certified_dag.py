from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta


"""
This DAG is intended to be an example of what a Certified DAG could look like. It is
based on the `example-dag.py` file that is included with projects initialized with the
`astro dev init` Astro CLI command.
"""

DAG_ARGS = dict(
    start_date=datetime(2021, 6, 11),
    max_active_runs=3,
    schedule_interval="@daily",
    # Default settings applied to all tasks within the DAG.
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=3),
    ),
    catchup=False,
)

# Using the `@dag` decorator, the `dag` argument doesn't need to be specified for each
# task.  This functions the same way as the `with DAG()...` context manager.
@dag(**DAG_ARGS)
def certified_dag():
    t_start = DummyOperator(task_id="start")
    t_end = DummyOperator(task_id="end")

    with TaskGroup("group_bash_tasks") as group_bash_tasks:
        sleep = "sleep $[ ( $RANDOM % 30 )  + 1 ]s && date"

        t1 = BashOperator(
            task_id="bash_begin",
            bash_command="echo begin bash commands",
        )
        t2 = BashOperator(
            task_id="bash_print_date2",
            bash_command=sleep,
        )
        t3 = BashOperator(
            task_id="bash_print_date3",
            bash_command=sleep,
        )

        # Lists can be used to specify tasks to execute in parallel.
        t1 >> [t2, t3]

    t_start >> group_bash_tasks >> t_end

    # Generate tasks with a loop.
    for task_number in range(5):
        # The `task_id` must be unique across all tasks in a DAG.
        @task(task_id=f"python_print_date_{task_number}")
        def custom_function(task_number: int, **context) -> None:
            """
            This can be any Python code you want and is called in a similar manner to
            the `PythonOperator`. The code is not executed until the task is run by the
            Airflow Scheduler.
            """

            print(
                f"I am task number {task_number}. "
                f"This DAG Run execution date is {context['ts']} and the current time is {datetime.now()}."
            )
            print(f"Here is the full DAG Run context: {context}")

        # When setting the dependencies, make sure they are indented inside the loop so
        # each task is added downstream of `t_start`.
        t_start >> custom_function(task_number) >> t_end


dag = certified_dag()
