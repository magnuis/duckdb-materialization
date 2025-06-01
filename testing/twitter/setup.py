from queries.twitter.q1 import Q1
from queries.twitter.q2 import Q2
from queries.twitter.q3 import Q3
from queries.twitter.q4 import Q4
from queries.twitter.q5 import Q5
from queries.twitter.q6 import Q6
from queries.twitter.q7 import Q7
from queries.twitter.q8 import Q8
from queries.twitter.q9 import Q9
from queries.twitter.q10 import Q10
from queries.twitter.q11 import Q11
from queries.twitter.q12 import Q12
from queries.twitter.q13 import Q13
from queries.twitter.q14 import Q14
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
    'q9': Q9(),
    'q10': Q10(),
    'q11': Q11(),
    'q12': Q12(),
    'q13': Q13(),
    'q14': Q14(),
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
    'text': {  # 860217
        'access': "raw_json->>'text'",
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
    "delete_status_userIdStr": {  # 252502
        "access": "raw_json->'delete'->'status'->>'user_id_str'",
        "type": "VARCHAR",
        "frequency": 252502
    },
    "delete_timestampMs": {  # 252502
        "access": "raw_json->'delete'->>'timestamp_ms'",
        "type": "BIGINT",
        "frequency": 252502
    }
}
