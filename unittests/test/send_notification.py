import boto3
from botocore.exceptions import ClientError
import os

AWS_REGION = "us-east-1"
lifecycle = os.getenv('lifecycle')
TEAM_EMAIL = "{0}".format(os.getenv('TEAM_EMAIL_ID'))


def send_start_mail(taskname):

    BODY_TEXT = taskname + ' ETL started'

    BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>SERVICE-DESK JIRA ETL started</h1>
    <p>contact <a href='{0}'>Team Vega</a> in case of any issues or queries</p>
    </body>
    </html>""".format(TEAM_EMAIL)
    if not lifecycle == 'Build':
        send_mail(BODY_TEXT, BODY_HTML)


def send_completion_mail(message, status, taskname):
    BODY_TEXT = taskname + " ETL completed with status: " + status
    ERROR_MESSAGE = ""

    if status != "SUCCESS":
        if not type(message) == str:
            ERROR_MESSAGE = message
        else:
            ERROR_MESSAGE = " Error Message: " + message

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>{0}</h1>
    <p>{2}</p>
    <p>contact <a href='{1}'>Team Vega</a> in case of any issues or queries</p>
    </body>
    </html>""".format(BODY_TEXT, TEAM_EMAIL, ERROR_MESSAGE)
    if not lifecycle == 'Build':
        send_mail(BODY_TEXT, BODY_HTML)


def send_custom_mail(subject, message):
    BODY_TEXT = subject

    MESSAGE = message

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>{0}</h1>
    <p>{2}</p>
    <p>contact <a href='{1}'>Team Vega</a> in case of any issues or queries</p>
    </body>
    </html>""".format(BODY_TEXT, TEAM_EMAIL, MESSAGE)
    if not lifecycle == 'Build':
        send_mail(BODY_TEXT, BODY_HTML)


def send_mail(BODY_TEXT, BODY_HTML):
    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)
    SUBJECT = '[' + lifecycle + '] ' + BODY_TEXT
    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    TEAM_EMAIL,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=TEAM_EMAIL
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
