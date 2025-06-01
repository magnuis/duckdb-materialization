from queries.tpch.q1 import Q1
from queries.tpch.q2 import Q2
from queries.tpch.q3 import Q3
from queries.tpch.q4 import Q4
from queries.tpch.q5 import Q5
from queries.tpch.q6 import Q6
from queries.tpch.q7 import Q7
from queries.tpch.q8 import Q8
from queries.query import Query


QUERIES: dict[str, Query] = {
    'q1': Q1(),
    'q2': Q2(),
    'q3': Q3(),
    'q4': Q4(),
    'q5': Q5(),
    'q6': Q6(),
    'q7': Q7(),
    'q8': Q8(),
    # 'q9': Q9(),
    # 'q10': Q10(),
    # 'q11': Q11(),

}

STANDARD_SETUPS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": None,
    },
}


COLUMN_MAP = {
    'lang': {  # 860217
        'access': "raw_json->>'lang'",
        "type": 'VARCHAR',
        "frequency": 860217
    },
    'source': {  # 860217
        'access': "raw_json->>'source'",
        "type": 'VARCHAR',
        "frequency": 860217

    },
    "retweetedStatus_user_screenName": {  # 466512
        'access': "raw_json->'retweeted_status'->'user'->>'screen_name'",
        "type": "VARCHAR",
        "frequency": 466512
    },
    "retweetedStatus_user_idStr": {  # 466512
        'access': "raw_json->'retweeted_status'->'user'->>'id_str'",
        "type": "VARCHAR",
        "frequency": 466512
    },
    "retweetedStatus_retweetCount": {  # 466512
        'access': "raw_json->'retweeted_status'->>'retweet_count'",
        "type": "INT",
        "frequency": 466512
    },
    "retweetedStatus_idStr": {  # 466512
        'access': "raw_json->'retweeted_status'->>'id_str'",
        "type": "VARCHAR",
        "frequency": 466512
    },
    "inReplyToUserIdStr": {  # 209563
        'access': "raw_json->>'in_reply_to_user_id_str'",
        "type": "VARCHAR",
        "frequency": 209563
    },
    "idStr": {  # 860217
        'access': "raw_json->>'id_str'",
        "type": "VARCHAR",
        "frequency": 860217

    },
    "user_screenName": {  # 860217
        'access': "raw_json->'user'->>'screen_name'",
        "type": "VARCHAR",
        "frequency": 860217

    },
    "user_idStr": {  # 860217
        'access': "raw_json->'user'->>'id_str'",
        "type": "VARCHAR",
        "frequency": 860217

    },
    "user_followersCount": {  # 860217
        'access': "raw_json->'user'->>'followers_count'",
        "type": "INT",
        "frequency": 860217

    },
    "user_isTranslator": {  # 860217
        "access": "raw_json->'user'->>'is_translator'",
        "type": "BOOLEAN",
        "frequency": 860217

    },
    "delete_status_idStr": {  # 252502
        "access": "raw_json->'delete'->'status'->>'id_str'",
        "type": "VARCHAR",
        "frequency": 252502
    },
    "delete_status_userIdStr": {  # 252502
        "access": "raw_json->'delete'->'status'->>'user_id_str'",
        "type": "VARCHAR",
        "frequency": 252502
    },
    "delete_timestampMs": {  # 252502
        "access": "raw_json->>'timestamp_ms'",
        "type": "BIGINT",
        "frequency": 252502
    },
    "timestampMs": {  # 860217
        "access": "raw_json->>'timestamp_ms'",
        "type": "BIGINT",
        "frequency": 860217

    }
}
