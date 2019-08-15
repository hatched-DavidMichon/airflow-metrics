from airflow.models import DagRun
from airflow.models import TaskInstance
from airflow.settings import Stats

from airflow_metrics.utils.event_utils import EventManager
from airflow_metrics.utils.fn_utils import once


def dag_duration(target=None, **kwargs):
    if target.start_date and target.end_date:
        duration = (target.end_date - target.start_date).total_seconds()
        tags = {
            'dag': target.dag_id,
        }
        Stats.timing('dag.duration', duration * 1000, tags=tags)


def task_duration(target=None, **kwargs):
    if target.duration:
        tags = {
            'dag': target.dag_id,
            'task': target.task_id,
            'state': target.state,
            'operator': target.operator,
        }
        Stats.timing('task.duration', target.duration * 1000, tags=tags)


def task_inqueue_duration(target=None, **kwargs):
    if target.queued_dttm and target.start_date:
        duration = (target.start_date - target.queued_dttm).total_seconds()
        tags = {
            'dag': target.dag_id,
            'task': target.task_id,
            'state': target.state,
            'operator': target.operator,
        }
        Stats.timing('task.inqueue_duration', duration * 1000, tags=tags)

@once
def patch_tasks():
    dag_run_after_update_manager = EventManager(DagRun, 'after_update')
    dag_run_after_update_manager.register_callback('end_date', dag_duration)

    task_instance_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_instance_after_update_manager.register_callback('duration', task_duration)
    
    task_inqueue_duration_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_inqueue_duration_after_update_manager.register_callback('start_date', task_inqueue_duration)
