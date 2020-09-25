import psycopg2
from data_extractor_dao import dbConnection, executeQuery
from audit_logger import getMaintaskId, createSubTask, updateSubTask, insertErrorLog
from send_notification import send_custom_mail


def sanity_check_counts():
    """ Execute the sanity check queries and report the failed cases
    Args:
    No Arguments
    Returns:
    No return Arguments
    """
    subtaskid = createSubTask("sanity check queries", getMaintaskId())
    try:
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("select query, name, expected_value from birepusr.etl_data_sanity_queries where project_name='planview'")
        rows = cur.fetchall()
        for row in rows:
            conn = dbConnection()
            cur = conn.cursor()
            cur.execute(row[0])
            x = cur.fetchall()
            if x == []:
                actual_value = 0
            else:
                actual_value = x[0][0]

            condition = row[1]
            expected_value = row[2]
            status = 'Success'
            if not expected_value == actual_value:
                status = 'Fail'
                message = '''Expected vs Actual values not matching for check '{0}': expected {1} rows but found {2}'''.format(condition, expected_value, actual_value)
                send_custom_mail('Service-Desk ETL : DATA SANITY TEST FAILED', message)

            executeQuery('''insert into birepusr.etl_data_sanity_test_results(condition, created_date, expected_value, actual_value, status, task_id,project_name) values ('{0}', now(), {1}, {2}, '{3}', '{4}','planview')'''.format(condition, expected_value, actual_value, status, subtaskid))
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception, psycopg2.Error) as error:
        insertErrorLog(subtaskid, error)

if __name__ == "__main__":
    sanity_check_counts()