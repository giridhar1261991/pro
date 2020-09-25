from zeep import Client
import variables as var


def getClient():
    """
    initialize planview client by calling wsdl url ,

    Args:
    No Arguments

    Returns:
    client: return client object for planview connection
    """
    cl = Client(var.planview_url)
    return cl


def getSession():
    """
    initialize planview client by calling wsdl url ,

    Args:
    No Arguments

    Returns:
    object: return client object for planview connection
    guid: return session id which is required for each plan view service request
    """
    req_data = {'username': '{0}'.format(var.planview_service_user),
        'password': '{0}'.format(var.planview_service_user_pwd)}
    
    client = getClient()
    sessionId = client.service.login(**req_data)
    return client, sessionId