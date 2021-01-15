import datetime


def get_measurment(client, db):
    client.switch_database(db)
    res = client.query('SHOW MEASUREMENTS')
    measurments = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return measurments


def get_field_keys(client, db):
    client.switch_database(db)
    res = client.query('show field keys')
    field_keys = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return ([item['fieldKey'] for item in field_keys], field_keys)


def get_tag_keys(client, db):
    client.switch_database(db)
    res = client.query('show tag keys')
    tag_keys = list(res.get_points())
    # df = pd.DataFrame(measurments)
    # df['db'] = db
    return ([item['tagKey'] for item in tag_keys], tag_keys)


def get_databases(client):
    res = client.query('show databases')

    databases = list(res.get_points())
    return databases


def tomorrow(war_start):
    today = datetime.datetime.strptime(war_start, '%Y-%m-%d')
    x = datetime.timedelta(days=1)
    ret = today + x
    ret = ret.strftime("%Y-%m-%d")
    return ret


def exclude(a, b):
    return [x for x in a if x not in b]
