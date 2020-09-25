from data_extractor_dao import executeQuery, executeQueryAndReturnDF, execute_proc
from audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask
import db_queries as dq
import psycopg2


def updateSprintMetrics():
    """
    This method trigger metrics update queries for a given sprint for given team
    mertics includes estimated_points,completed points, active team members working on team etc.

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = createSubTask("update metrics for newly added sprints", getMaintaskId())
    
    executeQuery(dq.update_estimate_sprint_points)
    executeQuery(dq.update_actual_completed_sprint_points)
    executeQuery(dq.update_completed_sprint_points)
    executeQuery(dq.update_team_members)
    executeQuery(dq.update_actual_sprint_date)
    executeQuery(dq.update_sprint_days)
    updateSubTask(subtaskid, "SUCCESS")


def initUpdateSprintMetrics():
    """
    this method initialize metrics update queries for all newly loaded sprints whose metrics is not 

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = createSubTask(
        "update sprint merics in idw.jira_sprint_summary", getMaintaskId())
    try:
        updateSprintMetrics()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)


def generateTimeSpentMetrics():
    """
    this method initialize metrics update queries for all newly loaded sprints

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = createSubTask(
        "update time-spent merics by project feature in idw.task_time_spent_fact",
        getMaintaskId())
    try:
        execute_proc('idw.feature_time_spent_calculation_proc')
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)
