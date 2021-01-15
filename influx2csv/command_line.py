import click
import glob
import os
import json
import pandas as pd
from lupa import LuaRuntime
import toml


from influxdb import InfluxDBClient
from datetime import date

from . import utils

import sys

assert sys.version[:1] == "3"


INFLUX_HOST = ''
INFLUX_USER = ''
INFLUX_PASSWORD = ''
INFLUX_PORT = 8086
client = ''

debug = False
lua = LuaRuntime(unpack_returned_tuples=True)
cfg = None


@click.group()
@click.option('--shout/--no-shout', ' /-S', default=False)
@click.option('--config', type=click.Path(), help='load config files')
def cli(shout, config):
    """influx2csv"""

    global INFLUX_HOST, INFLUX_USER, INFLUX_PORT, INFLUX_PASSWORD
    global client, debug, cfg
    debug = shout

    if not config:
        if debug:
            print("not config")
        pass
    else:
        config = toml.load(config)
        cfg = config
        influx_conf = config['influx']

        # with open(config, 'r') as f:
        #     influx_conf = json.load(f)
        #     if debug:
        #         print(influx_conf)

        INFLUX_USER = influx_conf['username']
        INFLUX_PASSWORD = influx_conf['password']
        INFLUX_PORT = influx_conf['port']
        INFLUX_HOST = influx_conf['host']

        # print()
        lua.execute('''
                    function split(str, sep)
                        local result = {}
                        local regex = ("([^%s]+)"):format(sep)
                        for each in str:gmatch(regex) do
                            table.insert(result, each)
                        end
                        return result
                    end 
                ''')

        client = InfluxDBClient(
            host=INFLUX_HOST, username=INFLUX_USER, password=INFLUX_PASSWORD, port=INFLUX_PORT)

    # p = "{}/config.json".format(out_dir)
    # if not os.path.exists(p):
    # 	ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
    # 	with open(p, 'w') as f:
    # 		json.dump(ret, p)
    #
    #

    if __name__ == '__main__':
        cli()


def filename(path):
    return os.path.basename(path).split('.csv')[0]


# @click.option('--output-dir', required=True, type=str, help='Output directory')
# @click.option('--csv-file', required=True, type=str, help='CSVInput directory')
# @click.version_option()
@ cli.command("show-databases")
def show_databases():
    databases = [db['name'] for db in client.get_list_database()]
    databases.remove("_internal")

    click.echo(databases)


def alldbs():
    # excludes = ['kadyaidb', 'laris1db',
    #             'aqithaidb', 'aqithaicom_db', 'dustboy']
    excludes = []
    databases = [db['name']
                 for db in client.get_list_database() if db['name'] not in excludes]
    databases.remove("_internal")
    results = []
    for db in databases:
        client.switch_database(db)
        rs = client.query('show measurements')
        for measurement in list(rs.get_points()):
            rs = client.query('show tag values with key = "topic"')
            topics = list(rs.get_points(measurement=measurement['name']))
            if len(topics) > 0:
                for topic in topics:
                    # print(json.dumps(topic))
                    # print(measurement['name'], topic_val, utils.getDustBoyId(topic_val))
                    topic_val = utils.getTopicValue(topic)
                    dct = {'db': db, 'measurement': measurement['name'], 'topic': topic_val,
                           'nickname': utils.getDustBoyId(topic_val)}
                    results.append(dct)
            else:
                rs = client.query('show tag values with key = "nickname"')
                nicknames = list(rs.get_points(
                    measurement=measurement['name']))
                if len(nicknames) > 0:
                    for nickname in nicknames:
                        dct = {
                            'db': db, 'measurement': measurement['name'], 'topic': False, 'nickname': nickname['value']}
                        results.append(dct)
                else:
                    pass
                # nicknames = list(rs.get_points(measurement=measurement['name']))
                # # rs = client.query(
                # #     f"show tag keys from \"{measurement['name']}\"")
                # # rsl = list(rs)
                # rsl += [[]]
                # tag_keys = [i['tagKey'] for i in rsl[0]]
                # if 'nickname' in tag_keys:
                #     dct = {'db': db, 'measurement': measurement['name'], 'topic': False,
                #            'nickname': measurement['name']}
                # else:
                #     print("no nickname")
                # print(tag_keys)
                # topics = list(rs.get_points(measurement=measurement['name']))
                # pass
    # print(measurement['name'])  # non-mqtt

    # mapping[measurement['name']] = {}
    # mapping[measurement['name'] + utils.getDustBoyId(topic_val)] = topic_val
    return results


def mm():
    databases = [db['name'] for db in client.get_list_database()]
    databases.remove("_internal")
    mapping = {}
    for db in databases:
        client.switch_database(db)
        rs = client.query('show measurements')
        for measurement in list(rs.get_points()):
            rs = client.query('show tag values with key = "topic"')
            topics = list(rs.get_points(measurement=measurement['name']))
            if len(topics) > 0:
                for topic in topics:
                    # print(json.dumps(topic))
                    # print(measurement['name'], topic_val, utils.getDustBoyId(topic_val))
                    topic_val = utils.getTopicValue(topic)
                    mapping[measurement['name'] +
                            utils.getDustBoyId(topic_val)] = topic_val
    print(json.dumps(mapping))
    return mapping


# @cli.command()
# @click.option('--out-dir', type=str, required=True)
# @click.option('--dry-run', count=True)
# def clear_scripts(out_dir, dry_run):
#     scripts = sorted(glob.glob('{0}/scripts/*.sh'.format(out_dir)))
#     delete = 0
#     for script in scripts:
#         ret = utils.getDictInfo(script)
#         filename = "{}_{}".format(ret['nickname'], ret['date'])
#         check_file = "{}/csv/{}/{}/{}/{}/{}.csv".format(out_dir, ret['database'], ret['measurement'], ret['nickname'],
#                                                         ret['datedir'], filename)
#         if os.path.exists(check_file):
#             delete = delete + 1
#             if dry_run:
#                 print("DRY RUN")
#             else:
#                 os.remove(script)
#     print("{} has been DELETED".format(delete))


# print('a')


@ cli.command()
def show_measurements():
    mapping = mm()


@ cli.command()
@ click.option('--date-start', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
@ click.option('--date-end', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
@ click.option('--out-dir', type=str, required=True)
def dumpall(date_start, date_end, out_dir):
    print(date_start.date(), utils.tomorrow(str(date_end.date())))
    mapping = alldbs()
    skipped = 0
    for d in mapping:
        skipped = skipped + \
            start_dump(date_start, date_end,
                       d['db'], d['measurement'], d['nickname'], d['topic'], out_dir)

    print("SKIPPED={}".format(skipped))


def start_dump(date_start, date_end, database_name, measurement_name, nickname, topic, out_dir):
    tomorrow = utils.tomorrow(str(date_end.date()))
    x = pd.date_range(start=date_start, end=tomorrow, freq='D')
    cmd = 'time influx -host {} -port {} -precision \'u\' -username {} -password {} -database {}'.format(
        INFLUX_HOST, INFLUX_PORT, INFLUX_USER, INFLUX_PASSWORD, database_name)
    skipped = 0
    for i in x:
        # timetuple = pd.to_datetime(i).timetuple()
        # print(x)
        today = str(i.date())
        tomorrow = utils.tomorrow(today)
        outfile = "{}_{}.csv".format(nickname, today)
        dbpath = database_name + "/" + measurement_name + "/" + nickname
        dirpath = "{}/csv/{}/{}".format(out_dir, dbpath, i.strftime("%Y/%m"))
        scriptpath = "{}/scripts".format(out_dir)
        shfile = "{}/{}_-_{}_-_{}_-_{}.sh".format(scriptpath, database_name, measurement_name, nickname,
                                                  i.strftime("%Y-%m-%d"))
        if topic:
            c = cmd + ' -execute "SELECT * FROM \\"{}\\" WHERE time >= \'{}\' AND time < \'{}\' AND ("topic"=\'{}\') tz(\'Asia/Bangkok\')" -format csv > {}/{}'.format(
                measurement_name, today, tomorrow, topic, dirpath, outfile)
        else:
            c = cmd + ' -execute "SELECT * FROM \\"{}\\" WHERE time >= \'{}\' AND time < \'{}\' AND ("nickname"=\'{}\') tz(\'Asia/Bangkok\')" -format csv > {}/{}'.format(
                measurement_name, today, tomorrow, nickname, dirpath, outfile)
        try:
            os.makedirs(dirpath)
        except:
            pass

        try:
            os.makedirs(scriptpath)
        except:
            pass

        cond = "{}/{}".format(dirpath, outfile)
        if os.path.exists(cond):
            skipped = skipped + 1
            print("SKIPPED!", outfile)
        else:
            with open(shfile, 'w') as out:
                out.write(c)

    return skipped


# print("{}/{} has been SKIPPED.".format(skipped, total))
# print(c)

@cli.command()
@click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
@click.option('--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
# @click.option('--measurement-name', type=str, required=True)
# @click.option('--nickname', type=str, required=True)
@click.option('--database-name', type=str, required=True, )
def dump(start_date, end_date, database_name):
    start_date = start_date.date()
    end_date = end_date.date()
    if debug:
        print(f'datebaase: {database_name}')
        print(f'start_date: {start_date}')
        print(f'end_date: {end_date}')

        start_time = f'{start_date} 00:00:00'
        end_time = f'{end_date} 23:59:59'

        print("------------")
        print("tag keys: ")
        print("------------")

        tag_keys, influx_tag_keys = utils.get_tag_keys(client, database_name)
        print(influx_tag_keys)
        for tag_key in tag_keys:
            print('>', tag_key)

        print("------------")
        print("field_keys: ")
        print("------------")

        field_keys, influx_field_keys, = utils.get_field_keys(
            client, database_name)
        print(influx_field_keys)
        for field_key in field_keys:
            print('>', field_key)

        print("------------")
        print("measurements: ")
        print("------------")

        measurements = utils.get_measurment(client, database_name)
        for measurement in utils.get_measurment(client, database_name):
            print('>', measurement)

        measurement_names = [measurement['name']
                             for measurement in measurements]

        print(measurement_names)

        global cfg
        for func_name, func_body in cfg['query']['funcs'].items():
            print(func_name, func_body)
            fn = lua.eval(func_body)
            print(fn('/Dustboy2/gearname/DUSTBOY-001/status'))

        print(cfg['query']['mapping'])
        # for measurement in measurements:
        #     print(measurement['name'])
        #     measurement = measurement['name']
        #     query = f'''SELECT * FROM "{measurement}" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("topic" = 'DUSTBOY/Model-N/WiFi/N-001/status') tz('Asia/Bangkok')'''
        #     print(query)

    # utils.get_measurment(cli)

    # mapping = mm()
    # print(mapping)
    # print(start_date, utils.tomorrow(str(start_date)))
    # x = pd.date_range(start=start_date, end=end_date, freq='D')
    # measurement = "v1"

# AND time >= '2021-01-14 18:00:00' tz('Asia/Bangkok')
    return

    # print(date_start.timetuple().tm_yday)
    # cmd = 'time influx -host {} -port {} -precision \'u\' -username {} -password {} -database {}'.format(
    #     INFLUX_HOST, INFLUX_PORT, INFLUX_USER, INFLUX_PASSWORD, database_name)
    # # print("alias infx='{}'".format(cmd))
    # for i in x:
    #     # timetuple = pd.to_datetime(i).timetuple()
    #     today = str(i.date())
    #     tomorrow = utils.tomorrow(today)
    #     outfile = "{}_{}.csv".format(nickname, today)
    #     c = cmd + ' -execute "SELECT * FROM \\"{}\\" WHERE time >= \'{}\' AND time < \'{}\' AND ("topic"=\'{}\') tz(\'Asia/Bangkok\')" -format csv > {}'.format(
    #         measurement_name, today, tomorrow, mapping[measurement_name + nickname], outfile)
    #     print(c)


@cli.command()
def config():
    ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
    str = json.dumps(ret)
    print(str)


def main():
    cli()
