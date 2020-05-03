import pandas as pd
from pymongo import MongoClient
import psycopg2
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv
import sys
from datetime import date
import logging
from sparkpost import SparkPost

def env_load():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)


def load_logger():
    logging.basicConfig(filename="logs/{}.log".format(date.today().strftime("%d-%m-%Y")),
                        format='%(asctime)s %(message)s',
                        filemode='a')

    # Creating an object
    logger = logging.getLogger()

    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)


def connect_mongo(host, port, username, password):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s' % (username, password, host, port)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn


def connect_redshift():
    con = psycopg2.connect(dbname=os.getenv("REDSHIFT_DB"), host=os.getenv("REDSHIFT_HOST"),
                           port=os.getenv("REDSHIFT_PORT"),
                           user=os.getenv("REDSHIFT_USER"), password=os.getenv("REDSHIFT_PASSWORD"))

    return con


def read_cursor(database, query):
    cursor = db[database].get_collection(os.getenv("MONGODB_COLLECTION")).find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    return df


def send_email(message, database):
    sp = SparkPost(os.getenv('SPARKPOST_API_KEY'))

    response = sp.transmissions.send(
        use_sandbox=False,
        recipients=['xyz@test.com'],
        html="""
            <p>{}</p><br>
            <p>{}</p>
            """.format(message, database),
        from_email='no-reply@test.com',
        subject='Python Script Error'
    )


if __name__ == "__main__":

    env_load()
    load_logger()

    db = connect_mongo(os.getenv("MONGODB_HOST"), int(os.getenv("MONGODB_PORT")), os.getenv("MONGODB_USER"),
                       os.getenv("MONGODB_PASSWORD"))

    s3 = boto3.client(
        's3',
        # Hard coded strings as credentials, not recommended.
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    filename = 'data.csv'
    bucket_name = os.getenv("S3_BUCKET_NAME")
    r = []
    ds = pd.DataFrame()

    try:
        ds = read_cursor(os.getenv("MONGODB_DATABASE"), {})

        if not ds.empty:

            sql_statement_redshift_columns = "select {} from pg_table_def where tablename = '{}'".format(
                '"column"', os.getenv("REDSHIFT_TABLE"))

            redshift_columns_df = pd.read_sql(sql_statement_redshift_columns, connect_redshift())
            redshift_columns_list = redshift_columns_df['column'].values.tolist()

            all_columns = ds.columns.tolist()

            columns_to_remove = list(set(all_columns) - set(redshift_columns_list))

            for column in columns_to_remove:
                del ds[column]

            ds.to_csv('data.csv', index=False)

            columns = ds.columns.tolist()
            column_str = ','.join(columns)

            # Uploads the given file using a managed uploader, which will split up large
            # files automatically and upload parts in parallel.
            s3.upload_file(filename, bucket_name, filename)

            sql = """copy {}.{} ({}) from '{}'\
                                                   credentials \
                                                   'aws_access_key_id={};aws_secret_access_key={}' \
                                                   DELIMITER ',' FILLRECORD EMPTYASNULL IGNOREHEADER 1 removequotes escape;commit;""" \
                .format('public', os.getenv("REDSHIFT_TABLE"), column_str, "s3://%s/%s" % (bucket_name, filename),
                        os.getenv("AWS_ACCESS_KEY_ID"),
                        os.getenv("AWS_SECRET_ACCESS_KEY"))

            connn = connect_redshift()
            cur = connn.cursor()
            cur.execute(sql)
            connn.commit()
            connn.close()
            os.remove('data.csv')

            logging.info('---------Database successfully uploaded---------- : ' + data)

    except Exception as e:
        logging.exception("message")
        send_email(e, os.getenv("MONGODB_DATABASE"))
        sys.exit(43)

    sys.exit(0)