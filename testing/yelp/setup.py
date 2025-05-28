from queries.query import Query
from queries.yelp.q1 import Q1
from queries.yelp.q2 import Q2
from queries.yelp.q3 import Q3

QUERIES: dict[str, Query] = {
    'q1': Q1(),
    'q2': Q2(),
    'q3': Q3(),
}


STANDARD_SETUPS = {
    # "no_materialization": {
    #     "materialization": [],
    # },
    # "only_user_id": {
    #     "materialization": ['user_id'],
    # },
    "full_materialization": {
        "materialization": None,
    },
    # # TODO
    # "schema_based_materialization": {
    #     "materialization": ['review_id', 'user_id', 'business_id', 'stars', 'date', 'useful', 'funny', 'cool', 'name']
    # }
}

COLUMN_MAP = {
    ######################## Business ########################
    "business_id": {
        'access': "raw_json->>'business_id'",
        'type': 'VARCHAR'
    },
    "name": {
        'access': "raw_json->>'name'",
        'type': 'VARCHAR'
    },
    "address": {
        'access': "raw_json->>'address'",
        'type': 'VARCHAR'
    },
    "city": {
        'access': "raw_json->>'city'",
        'type': 'VARCHAR'
    },
    "state": {
        'access': "raw_json->>'state'",
        'type': 'VARCHAR'
    },
    "postal_code": {
        'access': "raw_json->>'postal_code'",
        'type': 'VARCHAR'
    },
    "latitude": {
        'access': "CAST(raw_json->>'latitude' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "longitude": {
        'access': "CAST(raw_json->>'longitude' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "stars": {
        'access': "CAST(raw_json->>'stars' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "review_count": {
        'access': "CAST(raw_json->>'review_count' AS INT)",
        'type': 'INT'
    },
    "is_open": {
        'access': "CAST(raw_json->>'is_open' AS INT)",
        'type': 'INT'
    },
    # TODO attributes (objects of either boolean, or subobjects of bools)
    # TODO categoris (list of string)
    # TODO hours (object with strings)
    ######################## Checkin ########################
    ######################## Review ########################
    # User id defined under user
    # Business id defined under business
    "review_id": {
        'access': "raw_json->>'review_id'",
        'type': 'VARCHAR'
    },
    "date": {
        'access': "CAST(raw_json->>'date' AS DATE)",
        'type': 'DATE'
    },
    "text": {
        'access': "raw_json->>'text'",
        'type': 'VARCHAR'
    },
    "useful": {
        'access': "CAST(raw_json->>'useful' AS INT)",
        'type': 'INT'
    },
    "funny": {
        'access': "CAST(raw_json->>'funny' AS INT)",
        'type': 'INT'
    },
    "cool": {
        'access': "CAST(raw_json->>'cool' AS INT)",
        'type': 'INT'
    },
    ######################## Tip ########################
    ######################## User ########################
    "user_id": {
        'access': "raw_json->>'user_id'",
        'type': 'VARCHAR'
    },
    # Name already defined under business
    # Review count already defined under business
    "yelping_since": {
        'access': "raw_json->>'yelping_since'",
        'type': 'VARCHAR'
    },
    # "yelping_since": {
    #     'access': "CAST(raw_json->>'yelping_since' AS DATE)",
    #     'type': 'DATE'
    # },
    # Useful already defined under review
    # Funny already defined under review
    # Cool already defined under review
    "fans": {
        'access': "CAST(raw_json->>'fans' AS INT)",
        'type': 'INT'
    },
    # TODO elite (list of string)
    "average_stars": {
        'access': "CAST(raw_json->>'average_stars' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    # TODO compliments
}
