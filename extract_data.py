from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from helper.parser_helper import parser
from helper.geoip_helper import geoip

spark = SparkSession.builder.appName("Spark for ETL data 99.co ID").config(
    "spark.some.config.option", "some-value"
).getOrCreate()

# Extract Dataset
df_users = spark.read.csv("test/resources/user.csv", header=True)
df_trackers = spark.read.json("test/resources/tracker.txt")

parser = parser()

# geoip = geoip()


def parse_event(path, event):
    return parser.parse_event(path, event)


def parse_ip_to_locality(ipaddress):
    return geoip.get_locality_from_ip(ipaddress)


def parse_userID(path, event):
    """
    Context string is encoded in base64, but somecase it separated with url encoded characters.
    """
    return parse_event(path, event) if parse_event(
        path, event
    ) is not None else get_userid_from_context_array(path)


def get_userid_from_context_array(path):
    """
    Get userID from context array that encoded with base64
    """
    user_name = parser.get_context_username(path)
    if user_name is not None:
        return get_user_id(user_name)
    return None


def get_user_id(user_name):
    user_id = df_users.filter(df_users.username == 'rayaveronica'
                              ).select('id').collect()
    print(user_id)
    return 'Waduk'

    # if len(userID) >= 1:
    #     return userID[0][0]


# Create User definition function
event_country_from_ip = udf(lambda ip: parse_ip_to_locality(ip), StringType())
event_category = udf(lambda path: parse_event(path, 'se_ca'), StringType())
event_action = udf(lambda path: parse_event(path, 'se_ac'), StringType())
event_label = udf(lambda path: parse_event(path, 'se_la'), StringType())
event_sesionID = udf(lambda path: parse_event(path, 'sid'), StringType())
event_visitorID = udf(lambda path: parse_event(path, 'fp'), StringType())
event_userID = udf(lambda path: parse_userID(path, 'uid'), StringType())
event_platform_type = udf(lambda path: parse_event(path, 'p'), StringType())
event_userID = udf(lambda path: parse_userID(path, 'uid'), StringType())
event_aid = udf(lambda path: parse_event(path, 'aid'), StringType())

df_trackers.select(
    'ip',
    event_category('path').alias('event_category'),
    event_action('path').alias('event_action'),
    event_label('path').alias('event_label'),
    event_sesionID('path').alias('event_sessionID'),
    event_visitorID('path').alias('event_visitorID'),
    event_platform_type('path').alias('event_platform_type'),
    event_aid('path').alias('event_aid')
    # event_userID('path').alias('event_userID'),
).show()
