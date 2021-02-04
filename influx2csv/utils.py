import datetime
import os


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
    print(">>>>>>>", databases)

    databases = [item["name"] for item in databases[1:]]
    if "_internal" in databases:
        databases.remove("_internal")
    return databases


def tomorrow(war_start):
    today = datetime.datetime.strptime(war_start, '%Y-%m-%d')
    x = datetime.timedelta(days=1)
    ret = today + x
    ret = ret.strftime("%Y-%m-%d")
    return ret


def yesterday(war_start):
    today = datetime.datetime.strptime(war_start, '%Y-%m-%d')
    x = datetime.timedelta(days=1)
    ret = today - x
    ret = ret.strftime("%Y-%m-%d")
    return ret


def exclude(a, b):
    return [x for x in a if x not in b]


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def generate_output_path(out_dir, database_name, ss_start_date, measurement, tag_key, nickname):
    output_gen_path = f'{database_name}/{ss_start_date}/{measurement}/{tag_key}'
    output_file = f'{nickname}.csv'
    target_file = os.path.join(
        out_dir, output_gen_path, output_file)

    os.makedirs(os.path.join(
        out_dir, output_gen_path), exist_ok=True)
    return target_file


def _show_measurements(client, database_name):
    measurement_names = get_measurements(client, database_name)
    # measurement_names = ["dustboy.netpie.2019"]
    for measurement in measurement_names:
        print('  ', measurement)
    if len(measurement_names) == 0:
        print('   ', f'No measurement')


def _show_measurements_with_detail(client, database_name):
    for measurement in get_measurements(client, database_name):
        show_tag_keys(client, database_name, measurement, with_value=True)
        show_field_keys(client, database_name, measurement)


def show_tag_keys(client, database_name, measurement, with_value=False):
    print("------------")
    print(f"tag keys: from {measurement}")
    print("------------")

    tag_keys = get_tag_keys(client, database_name, measurement)

    # print("------------")
    # print(influx_tag_keys)
    # print("------------")
    for idx, tag_key in enumerate(tag_keys, start=1):
        print(idx, ">", tag_key)
    print("------------")

    if with_value:
        for idx, tag_key in enumerate(tag_keys, start=1):
            tag_values = get_tag_values(
                client, database_name, measurement, tag_key)
            print("============")
            print("", ">>", tag_key, "")
            print("============")
            for idx, tag_value in enumerate(tag_values, start=1):
                print(idx, f"-> {tag_value}")
            print("------------")
    return tag_keys


def show_field_keys(client, database_name, measurement):
    print("------------")
    print(f"field_keys: from {measurement}")
    print("------------")

    field_keys = get_field_keys(client, database_name, measurement)

    # print("------------")
    # print(influx_field_keys)
    # print("------------")

    for idx, field_key in enumerate(field_keys, start=1):
        print(idx, '>', field_key)
    print("------------")
