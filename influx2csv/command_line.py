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


def clear(): return os.system('clear')


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

    # clear()
    print("-----------------------------------")
    print("""
     _        __ _            ____
(_)_ __  / _| |_   ___  _|___ \ ___ _____   __
| | '_ \| |_| | | | \ \/ / __) / __/ __\ \ / /
| | | | |  _| | |_| |>  < / __/ (__\__ \\ V /
|_|_| |_|_| |_|\__,_/_/\_\_____\___|___/ \_/
    """)

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

        # print(cfg)
        # sys.exit()
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
                    function string:split(delimiter)
                        local result = { }
                        local from  = 1
                        local delim_from, delim_to = string.find( self, delimiter, from  )
                        while delim_from do
                            table.insert( result, string.sub( self, from , delim_from-1 ) )
                            from  = delim_to + 1
                            delim_from, delim_to = string.find( self, delimiter, from  )
                        end
                        table.insert( result, string.sub( self, from  ) )
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

def _show_measurements(client, database_name):
    print("------------")
    print("measurements: ")
    print("------------")

    measurement_names = utils.get_measurements(
        client, database_name)

    for measurement in measurement_names:
        print('>', measurement)
    else:
        if len(measurement_names) is 0:
            print(f'No measurement found on "{database_name}"')


def _show_measurements_with_detail(client, database_name):

    for measurement in utils.get_measurements(client, database_name):
        show_tag_keys(client, database_name, measurement, with_value=True)
        show_field_keys(client, database_name, measurement)


def show_tag_keys(client, database_name, measurement, with_value=False):
    print("------------")
    print(f"tag keys: from {measurement}")
    print("------------")

    tag_keys = utils.get_tag_keys(client, database_name, measurement)

    # print("------------")
    # print(influx_tag_keys)
    # print("------------")
    for idx, tag_key in enumerate(tag_keys, start=1):
        print(idx, ">", tag_key)
    print("------------")

    if with_value:
        for idx, tag_key in enumerate(tag_keys, start=1):
            tag_values = utils.get_tag_values(
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

    field_keys = utils.get_field_keys(client, database_name, measurement)

    # print("------------")
    # print(influx_field_keys)
    # print("------------")

    for idx, field_key in enumerate(field_keys, start=1):
        print(idx, '>', field_key)
    print("------------")


def show_tag_values_by_tag_key(client, database_name, tag_key, func_string):
    for tag_value in utils.get_tag_values(client, database_name, tag_key):
        fn = lua.eval(func_string)
        print(tag_value, fn(tag_value))

    return utils.get_tag_values(client, database_name, tag_key)


@ cli.command()
@ click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
@ click.option('--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), required=False)
@ click.option('--out-dir', type=str, required=True)
# @click.option('--measurement-name', type=str, required=True)
# @click.option('--nickname', type=str, required=True)
# @click.option('--database-name', type=str, required=True, )
def dump(start_date, end_date, out_dir):
    start_date = start_date.date()
    if end_date is None:
        end_date = start_date
    else:
        end_date = end_date.date()

    print(cfg, out_dir)
    database_name = cfg['influx']['database_name']
    print("------------")
    print(f'> datebaase: {database_name}')
    print(f'> start_date: {start_date}')
    print(f'> end_date: {end_date}')
    print("------------")

    start_time = f'{start_date} 00:00:00'
    end_time = f'{end_date} 23:59:59'

    # tag_key = cfg['query']['tag_key']
    # print('input tag_key = ', tag_key)

    _show_measurements(client, database_name)

    print(cfg['query']['config'])
    query_config = cfg['query']['config']
    if query_config['tag_key'] is "*":
        _show_measurements_with_detail(client, database_name)
    else:
        for measurement in utils.get_measurements(client, database_name):
            tag_keys = show_tag_keys(client, database_name, measurement)
            for tag_key in tag_keys:
                # print(tag_key)
                # print(query_config[tag_key])
                if tag_key in query_config['logic']:
                    print(f'yay found {tag_key}!')
                    funcs = query_config['logic'][tag_key]['funcs']

                    filter_func = lua.eval(funcs['filter'])
                    transform_func = lua.eval(funcs['transform'])

                    for tag_value in utils.get_tag_values(client, database_name, measurement, tag_key):
                        if filter_func(tag_value):
                            # print(tag_value)
                            nickname = transform_func(tag_value)
                            query = f'''SELECT * FROM "{measurement}" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("{tag_key}" = '{tag_value}') tz('Asia/Bangkok')'''
                            output_gen_path = f'{database_name}/{start_date}/{measurement}/{tag_key}'
                            output_file = f'{nickname}.csv'
                            target_file = os.path.join(
                                out_dir, output_gen_path, output_file)

                            os.makedirs(os.path.join(
                                out_dir, output_gen_path), exist_ok=True)
                            cmd = f'''influx -host {INFLUX_HOST} -port {INFLUX_PORT} -precision \'u\' -username {INFLUX_USER} -password {INFLUX_PASSWORD} -database {database_name} -execute "{query}" > {target_file} '''

                            print(nickname)
                            if os.path.exists(target_file) and os.stat(target_file).st_size > 0:
                                print(target_file, 'exists!')
                            else:
                                os.system(cmd)
                            # print(query, nickname)

                    # print(funcs['filter_func'])
                    # print(enumerate(funcs))
                    # for name, j in funcs.items():
                    #     print(i, j)
            # for tag_key in utils.get_tag_keys
            # show_field_keys(client, database_name, measurement)

    sys.exit()

    # tag_values = show_tag_values_by_tag_key(
    #     client, database_name, cfg['query']['tag_key'])
    # for tag_value in tag_values:
    #     # print(tag_value)
    #     query = f'''SELECT * FROM "{measurement}" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("{tag_key['name']}" = '{tag_value}') tz('Asia/Bangkok')'''

    # print(measurement)

    # for func_name, func_body in cfg['query']['funcs'].items():
    #     print(func_name, func_body)
    #     fn = lua.eval(func_body)
    #     print(fn('/Dustboy2/gearname/DUSTBOY-001/status'))

    # print(cfg['query']['mapping'])

    # for measurement in measurements:
    #     print(measurement['name'])
    #     measurement = measurement['name']
    #     query = f'''SELECT * FROM "{measurement}" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("topic" = 'DUSTBOY/Model-N/WiFi/N-001/status') tz('Asia/Bangkok')'''
    #     print(query)

    # utils.get_measurements(cli)

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


@ cli.command()
def config():
    ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
    str = json.dumps(ret)
    print(str)


def main():
    cli()
