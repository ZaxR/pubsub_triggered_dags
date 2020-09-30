# pubsub_triggered_dags

## About
There are several different ways to trigger an Airflow DAG from a Pub/Sub topic subscription. This code will focus on using a Google Pub/Sub push subscription to an endpoint URL associated with a Google HTTP Cloud function. The Cloud Function will then trigger an Airflow DAG on Google Composer. Metadata about the Pub/Sub messages received will be stores in a Posthres Google CloudSQL DB via SQLAlchemy.

While Google has basic tutorials for each of triggering Cloud Functions from Pub/Sub and triggering a DAG from a Cloud Function, those tutorials assume all of the parts live in the same Google Project. In practice, is might be common to need to trigger workflows based on other teams' processes, which are outside your control. Additionally, this code has a few bonuses:
- it has Python code to interact with secrets managed in Google Secrets Manager,
- it stores metadata about triggers, and
- it enforces exactly-once triggering of the associated DAG (Pub/Sub guarantees at-least-once message delivery, which means that occasional duplicates should otherwise be expected)

## Using this code
0. This project assumes you already have a Composer instance
   and a Google service account with access to the required services.
   If not, Google already has thorough guides for all of the above.
1. Fill in the variables in deploy.sh with your actual information
2. Tweak `main.trigger_dag` to account for the Pub/Sub message attributes
   the topic you're subscribed to will post/
3. Run `bash deploy.sh` to deploy your Pub/Sub subscription and Cloud Function
4. Test the process by sending a dummy Pub/Sub message (adjusting attributes):

    ```
    gcloud pubsub topics publish topic-name \
    --project your-project-dev \
    --message='Test #1' \
    --attribute='data_type=sales_data,process=cleaning,status=completed'
    ```
5. Sit back and relax.

### Some important notes
- There are a LOT of comments in the code, some of which include useful links.
  If something isn't clear in the readme, try reading the comments before googling around.
  Pull requests welcome!
- The "Secret Manager Secret Accessor" role will need to be added to the service account for each secret if using Google Secret Manager
- Google Secret Manager uses a GLOBAL NAMESPACE for its secrets. If you have multiple projects using the secret manager, make sure to prefix your secrets with your project name so as not to collide with others' secrets.
