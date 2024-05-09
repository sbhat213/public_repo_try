import boto3
from job_steps import injector
from job_steps.send_failure_notification import STEP_NAME
from settings import Settings
from shared.logging.logger import Logger
from job_steps.dto.check_spark_state import CheckSparkState


def lambda_handler(event, context):
    logger = injector.get(Logger)
    settings = injector.get(Settings)
    check_spark_state = CheckSparkState()
    logger.info(f"For {STEP_NAME} Input event is : {event}")
    check_spark_state.set_values(event)
    # data = event["in_progress_watcher_result"]
    logger.info(f"For {STEP_NAME} Input event is : {event}")

    STATE_NOTIFICATION = str(check_spark_state.__dict__)

    CHARSET = "UTF-8"
    SENDER = "shiwangi.bhatia@nagarro.com"
    RECIPIENT = "shiwangi.bhatia@nagarro.com"
    AWS_REGION = "ap-south-1"
    SUBJECT = "SES EMAIL TEST"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Amazon SES Test (Python)\r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Failure Notification </h1>
      <p>""" + STATE_NOTIFICATION + """</p>
    </body>
    </html>"""

    client = boto3.client('ses', region_name=AWS_REGION)

    try:
        print(f"BODY_HTML - {BODY_HTML}")
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
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
            Source=SENDER,
        )
    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.response['Error']['Message']}")
        raise e
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

#
# input_json = {
#     "model": "fct_pres_opens_clicks_dly",
#     "response_id": 11,
#     "livy_url": "http://ip-10-0-1-210.ap-south-1.compute.internal:8998/batches",
#     "task_id": "fct_pres_opens_clicks_dly_daily",
#     "task_type": "Transform",
#     "job_type": 1,
#     "updated_on": "2021-02-16 11:04:53 UTC",
#     "status" : "",
#     "error_code" :"",
#     "error" : "",
#     "application_id" : ""
# }
#
# lambda_handler(input_json,'')
