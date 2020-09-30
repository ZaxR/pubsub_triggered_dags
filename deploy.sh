#!/bin/sh

# Deploy a PubSub push subscription and associated HTTP Cloud function
# The push subscription pushes to the endpoint URL associated with the HTTP Cloud function,
# which triggers the function that runs the Airflow DAG on Google Composer.

# Parameterization
# When set to `true`, cloud function will operate normally.
# When set to anything else (e.g. `false`), nothing will happen when the cloud function is triggered.
ENABLED=true

GOOGLE_PROJECT_NAME=project
SERVICE_ACCOUNT=name@project.iam.gserviceaccount.com

DEV_OR_PROD=prod
SUBSCRIPTION_NAME=your-subscription-${DEV_OR_PROD}
TRIGGER_TOPIC=projects/your-project-${DEV_OR_PROD}/topics/topic-name

CLOUD_FUNCTION_NAME=trigger-my-dag
FUNCTION_NAME=trigger_dag
DAG_NAME="file-email-delivery.email-weekly-reports"


MEMORY=256MB  # 256MB is the default; Can be one of [128, 256, 512, 1024, 2048]
ACK_TIMEOUT=100  # should be greater than CLOUD_FUNC_TIMEOUT
CLOUD_FUNC_TIMEOUT=95s  # 60s is the default; should be greater than IAP_TIMEOUT
IAP_TIMEOUT=90

# Set the project
gcloud config set project ${GOOGLE_PROJECT_NAME}

# Delete and recreate subscription
gcloud pubsub subscriptions delete ${SUBSCRIPTION_NAME}

# Beta currently required for use of message-filters
# update does not have topic or message-filter
# --message-filter='attributes.data_type = "sales_data" AND attributes.process = "cleaning" AND attributes.status = "completed"' \
gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} \
--topic=${TRIGGER_TOPIC} \
--ack-deadline=${ACK_TIMEOUT} \
--expiration-period=31d \
--message-retention-duration=7d \
--push-auth-service-account=${SERVICE_ACCOUNT} \
--push-endpoint=https://us-central1-${GOOGLE_PROJECT_NAME}.cloudfunctions.net/${CLOUD_FUNCTION_NAME} \
--min-retry-delay=600s \
--max-retry-delay=600s \
\
&& \
\
gcloud functions deploy ${CLOUD_FUNCTION_NAME} \
--service-account ${SERVICE_ACCOUNT} \
--entry-point ${FUNCTION_NAME} \
--memory ${MEMORY} \
--runtime python37 \
--timeout ${CLOUD_FUNC_TIMEOUT} \
--trigger-http \
--update-env-vars ENABLED=${ENABLED},DAG_NAME=${DAG_NAME},IAP_TIMEOUT=${IAP_TIMEOUT}
# --source  # the location of the source code to deploy; optional

# To test this process, you can use the following to publish to the topic
# Note that attributes/values can be whatever you want
# gcloud pubsub topics publish topic-name \
# --project your-project-dev \
# --message='Test #1' \
# --attribute='data_type=sales_data,process=cleaning,status=completed'
