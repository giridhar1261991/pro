import variables as var
from planview_login import getSession
import json
import pandas as pd
import zeep


def getEntityFields(entityTypeId):
    """
    method call planview service to get schema definition for given entity

    Args:
    integer: entityTypeId is predefined identifier in plan view to pull entity information

    Returns:
    json:  send json response given by planview service call
    """
 
    client, sessionId  = getSession()
    req_data_fe = {'sessionId': sessionId,
        'entityTypeId': entityTypeId, 'mode' : '0' }
    
    res = client.service.getEntityFields(**req_data_fe)
    serialized_res = zeep.helpers.serialize_object(res)
    return str(json.dumps(serialized_res)).replace("\'","")


def getData(entityTypeId, fieldList):
    """
    method calls planview service to get data fro given entity type

    Args:
    integer: entityTypeId is predefined identifier in plan view to pull respective data

    Returns:
    json:  send json response given by planview service call
    """
    client, sessionId  = getSession()
    req_data_fe = {'sessionId': sessionId,
        'entityTypeId': entityTypeId, 'fieldsRequest': fieldList}
    
    res = client.service.findEntity(**req_data_fe)
    serialized_res = zeep.helpers.serialize_object(res)
    project_df = pd.DataFrame(serialized_res)
    return project_df

def getTimesheet(userId,start_date,end_date):
    """
    method call planview service to get timesheet entry for an user

    """
 
    client, sessionId  = getSession()
    req_data_fe = {'sessionId': sessionId,
        'userId': userId, 'startYYYYMMDD': start_date,'endYYYYMMDD': end_date}
    
    res = client.service.getTimesheetForUser(**req_data_fe)
    serialized_res = zeep.helpers.serialize_object(res)
    timesheet_entry = pd.DataFrame(serialized_res)
    return timesheet_entry

def getTimesheet_users(userId,start_date):
    """
    method call planview service to get timesheet entry for multiple users

    """
    print(start_date)
    client, sessionId  = getSession()
    req_data_fe = {'sessionId': sessionId,
        'userIds': userId, 'startYYYYMMDD': start_date}
    
    res = client.service.getTimesheetForUsers(**req_data_fe)
    serialized_res = zeep.helpers.serialize_object(res)
    timesheet_entry = pd.DataFrame(serialized_res)
    return timesheet_entry


def insertTimesheet(user_ts):
    """
    method call planview service to put timesheet entry for an user
    
    Args:
    List: list of timesheet entries 

    Returns:
    List: on usccess it return list of timesheet entries inserted to planview
    """
 
    client, sessionId  = getSession()
    ordered_dict_ts = {'sessionId': sessionId} 
    #TODO: replace above line after region issue is resolved
    # {'sessionId': sessionId, 'autoSubmit':'True'}
    ordered_dict_ts['timesheet'] = user_ts

    res = client.service.createOrReplaceTimesheetEntries(**ordered_dict_ts)
    serialized_res = zeep.helpers.serialize_object(res)
    timesheet_entry = pd.DataFrame(serialized_res)
    return timesheet_entry

def deleteTimesheet_entries(timesheetId,entryId):
    """
    method call planview service to delete multiple timesheet entries

    """
    print(timesheetId)
    client, sessionId  = getSession()
    req_data_fe = {'sessionId': sessionId,
        'timesheetId': timesheetId, 'timesheetEntryId': entryId}
    
    client.service.deleteTimesheetEntries(**req_data_fe)

if __name__ == "__main__":
    x1 = deleteTimesheet_entries(16517100762,[16517913016, 16517913017, 16517913018])
    print(x1)