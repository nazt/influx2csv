import datetime


def get_measurements(client, db):
    client.switch_database(db)
    res = client.query('SHOW MEASUREMENTS')
    measurments = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return [item['name'] for item in measurments]


def get_field_keys(client, db, measurement):
    client.switch_database(db)
    res = client.query(f'show field keys from "{measurement}" ')
    field_keys = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return [item['fieldKey'] for item in field_keys]


def get_tag_keys(client, db, measurement):
    client.switch_database(db)
    res = client.query(f'show tag keys from "{measurement}"')
    tag_keys = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return [item['tagKey'] for item in tag_keys]


def get_tag_values(client, db, measurement, tag_key):
    client.switch_database(db)
    res = client.query(
        f'show tag values on "{db}" from "{measurement}" WITH KEY="{tag_key}"')
    tag_values = list(res.get_points())
    return [item['value'] for item in tag_values]

    # SHOW TAG VALUES FROM "dustboy2db" WITH KEY = "host"


def get_databases(client):
    res = client.query('show databases')
    databases = list(res.get_points())
    return [item["name"] for item in databases[1:]]


def tomorrow(war_start):
    today = datetime.datetime.strptime(war_start, '%Y-%m-%d')
    x = datetime.timedelta(days=1)
    ret = today + x
    ret = ret.strftime("%Y-%m-%d")
    return ret


def exclude(a, b):
    return [x for x in a if x not in b]
