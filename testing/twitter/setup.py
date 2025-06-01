from queries.query import Query

from queries.tpch.q1 import Q1
from queries.tpch.q2 import Q2
from queries.tpch.q3 import Q3
from queries.tpch.q4 import Q4
from queries.tpch.q5 import Q5
from queries.tpch.q6 import Q6
from queries.tpch.q7 import Q7
from queries.tpch.q8 import Q8

QUERIES: dict[str, Query] = {
    'q1': Q1(),
    'q2': Q2(),
    'q3': Q3(),
    'q4': Q4(),
    'q5': Q5(),
    'q6': Q6(),
    'q7': Q7(),
    'q8': Q8(),
}

COLUMN_MAP = {
    'lang': {
        'access': "TRY_CAST(raw_json->>'lang' AS VARCHAR)",
        "type": 'VARCHAR'
    },
    'source': {
        'access': "TRY_CAST(raw_json->>'source' AS VARCHAR)",
        "type": 'VARCHAR'
    },
    "retweetedStatus_user_screenName": {
        'access': "TRY_CAST(raw_json->'retweeted_status'->'user'->>'screen_name' AS VARCHAR)",
        "type": "VARCHAR"
    },
    "retweetedStatus_retweetCount": {
        'access': "TRY_CAST(raw_json->'retweeted_status'->>'retweet_count' AS INT)",
        "type": "INT"
    },
    "retweetedStatus_idStr": {
        'access': "TRY_CAST(raw_json->'retweeted_status'->>'id_str' AS VARCHAR)",
        "type": "VARCHAR"
    },
    "inReplyToUserIdStr": {
        'access': "TRY_CAST(raw_json->>'in_reply_to_user_id_str' AS VARCHAR)",
        "type": "VARCHAR"
    },
    "idStr": {
        'access': "TRY_CAST(raw_json->>'id_str' AS VARCHAR)",
        "type": "VARCHAR"
    },
    "user_screenName": {
        'access': "TRY_CAST(raw_json->'use'->>'screenName' AS VARCHAR)",
        "type": "VARCHAR"
    },
    "user_followersCount": {
        'access': "TRY_CAST(raw_json->'user'->>'followers_count' AS INT)",
        "type": "INT"
    },
    "text": {
        'access': "TRY_CAST(raw_json->>'text' AS VARCHAR)",
        "type": "VARCHAR"
    }
}
