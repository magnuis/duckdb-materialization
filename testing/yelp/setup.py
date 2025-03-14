QUERIES = [
    'q1',
    'q2',
    'q3',
    'q4',
    'q5',
    'q6',
    # 'q7',
    'q8',
    'q9',
    'q10',
    'q11'
]


STANDARD_SETUPS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": None,
    },
    # TODO
    "schema_based_materialization": {
        "materialization": ['review_id', 'user_id', 'business_id', 'stars', 'date', 'useful', 'funny', 'cool', 'name']
    }
}

COLUMN_MAP = {
    ######################## Business ########################
    "business_id": {
        'query': "raw_json->>'business_id'",
        'type': 'VARCHAR'
    },
    "name": {
        'query': "raw_json->>'name'",
        'type': 'VARCHAR'
    },
    "address": {
        'query': "raw_json->>'address'",
        'type': 'VARCHAR'
    },
    "city": {
        'query': "raw_json->>'city'",
        'type': 'VARCHAR'
    },
    "state": {
        'query': "raw_json->>'state'",
        'type': 'VARCHAR'
    },
    "postal_code": {
        'query': "raw_json->>'postal_code'",
        'type': 'VARCHAR'
    },
    "latitude": {
        'query': "CAST(raw_json->>'latitude' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "longitude": {
        'query': "CAST(raw_json->>'longitude' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "stars": {
        'query': "CAST(raw_json->>'stars' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "review_count": {
        'query': "CAST(raw_json->>'review_count' AS INT)",
        'type': 'INT'
    },
    "is_open": {
        'query': "CAST(raw_json->>'is_open' AS INT)",
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
        'query': "raw_json->>'review_id'",
        'type': 'VARCHAR'
    },
    "date": {
        'query': "CAST(raw_json->>'date' AS DATE)",
        'type': 'DATE'
    },
    "text": {
        'query': "raw_json->>'text'",
        'type': 'VARCHAR'
    },
    "useful": {
        'query': "CAST(raw_json->>'useful' AS INT)",
        'type': 'INT'
    },
    "funny": {
        'query': "CAST(raw_json->>'funny' AS INT)",
        'type': 'INT'
    },
    "cool": {
        'query': "CAST(raw_json->>'cool' AS INT)",
        'type': 'INT'
    },
    ######################## Tip ########################
    ######################## User ########################
    "user_id": {
        'query': "raw_json->>'user_id'",
        'type': 'VARCHAR'
    },
    # Name already defined under business
    # Review count already defined under business
    "yelping_since": {
        'query': "raw_json->>'yelping_since'",
        'type': 'VARCHAR'
    },
    # "yelping_since": {
    #     'query': "CAST(raw_json->>'yelping_since' AS DATE)",
    #     'type': 'DATE'
    # },
    # Useful already defined under review
    # Funny already defined under review
    # Cool already defined under review
    "fans": {
        'query': "CAST(raw_json->>'fans' AS INT)",
        'type': 'INT'
    },
    # TODO elite (list of string)
    "average_stars": {
        'query': "CAST(raw_json->>'average_stars' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    # TODO compliments
}
