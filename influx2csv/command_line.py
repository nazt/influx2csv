from time import sleep
from tqdm import tqdm
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
        print(cfg)
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
    # databases = [db['name'] for db in client.get_list_database()]
    # databases.remove("_internal")
    databases = utils.get_databases(client)

    click.echo(databases)


@ cli.command()
def show_measurements():
    mapping = mm()


def _show_measurements(client, database_name):
    measurement_names = utils.get_measurements(client, database_name)
    # measurement_names = ["dustboy.netpie.2019"]
    for measurement in measurement_names:
        print('  ', measurement)
    if len(measurement_names) is 0:
        print('   ', f'No measurement')


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


def syscall():
    pass


# @cli.command()
# @click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
# @click.option('--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), required=False)
# @click.option('--out-dir', type=str, required=True)
# @click.option('--dry-run', type=bool, required=False, default=False)
# @click.option('--database', type=str, required=True)
# def dump(start_date, end_date, out_dir, database, dry_run=False):
#     start_date = start_date.date()
#     if end_date is None:
#         end_date = start_date
#     else:
#         end_date = end_date.date()
#     # database_name = cfg['influx']['database_name']
# #     x = pd.date_range(start=date_start, end=tomorrow, freq='D')
#     database_name = cfg['influx']['database_name']

#     start_time = f'{start_date} 00:00:00'
#     end_time = f'{end_date} 23:59:59'

#     # tag_key = cfg['query']['tag_key']
#     # print('input tag_key = ', tag_key)

#     _show_measurements(client, database_name)
#     # print(cfg['query']['config'])
#     query_config = cfg['query']['config']

#     if query_config['tag_key'] is "*":
#         _show_measurements_with_detail(client, database_name)
#     else:
#         for measurement in utils.get_measurements(client, database_name):
#             tag_keys = show_tag_keys(client, database_name, measurement)
#             for tag_key in tag_keys:
#                 if tag_key in query_config['logic']:
#                     print(f'yay found {tag_key}!')
#                     funcs = query_config['logic'][tag_key]['funcs']
#                     filter_func = lua.eval(funcs['filter'])
#                     transform_func = lua.eval(funcs['transform'])
#                     for tag_value in utils.get_tag_values(client, database_name, measurement, tag_key):
#                         if filter_func(tag_value):
#                             # print(tag_value)
#                             nickname = transform_func(tag_value)
#                             query = f'''SELECT * FROM \\\"{measurement}\\\" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("{tag_key}" = '{tag_value}') tz('Asia/Bangkok')'''
#                             output_gen_path = f'{database_name}/{start_date}/{measurement}/{tag_key}'
#                             output_file = f'{nickname}.csv'
#                             target_file = os.path.join(
#                                 out_dir, output_gen_path, output_file)

#                             os.makedirs(os.path.join(
#                                 out_dir, output_gen_path), exist_ok=True)
#                             cmd = f'''influx -host {INFLUX_HOST} -port {INFLUX_PORT} -precision \'u\' -username {INFLUX_USER} -password {INFLUX_PASSWORD} -database {database_name} -execute "{query}" > {target_file} '''

#                             #! TODO://
#                             # if os.path.exists(target_file) and os.stat(target_file).st_size > 0:
#                             #     print(os.path.basename(target_file), 'exists!')
#                             # else:
#                             #     if not dry_run:
#                             #         os.system(cmd)
#                             #     else:
#                             #         print(nickname, "dry run!")


@cli.command()
@click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
@click.option('--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), required=False)
@click.option('--out-dir', type=str, required=True)
@click.option('--dry-run', type=bool, required=False, default=False)
@click.pass_context
def dump_range(ctx, start_date, end_date, out_dir, dry_run):
    database_name = cfg['influx']['database_name']
    #! TODO: Remove this
    # database_name = "*"
    databases = []
    if database_name == "*":
        databases = utils.get_databases(client)
    else:
        databases = [database_name]

    print("------------")
    print("Dry run:", dry_run)
    print(f'> datebase: {database_name}')
    print(f'> start_date: {start_date}')
    print(f'> end_date: {end_date}')
    print("------------")

    print("=== DATABASES ===")
    for idx, database_name in enumerate(databases, start=1):
        print("📍", f'{idx}/{len(databases)}', database_name)

        if database_name == "test1":
            continue
        if database_name == "data-mart":
            continue

        for measurement in utils.get_measurements(client, database_name):
            print("->", measurement)
            tag_keys = utils.get_tag_keys(
                client, database_name, measurement)
            for tag_key in tag_keys:
                print(">", tag_key)
                tag_values = utils.get_tag_values(
                    client, database_name, measurement, tag_key)

                query_config = cfg['query']['config']
                if tag_key in query_config['logic']:
                    print(f'yay found {tag_key}!')
                    funcs = query_config['logic'][tag_key]['funcs']
                    filter_func = lua.eval(funcs['filter'])
                    # transform_func = lua.eval(funcs['transform'])
                    for idx, tag_value in enumerate(utils.get_tag_values(client, database_name, measurement, tag_key), start=1):
                        if filter_func(tag_value):
                            # nickname = transform_func(tag_value)
                            nickname = tag_value.split("/")[-2:-1][0]
                            # if "Model-PRO" in tag_value:
                            #     nickname = tag_value.split("/")[-2:-1]
                            # print("============")
                            # print("", ">>", tag_key, "")
                            # print("============")
                            # for idx, tag_value in enumerate(tag_values, start=1):
                            print(idx, f"-> {tag_value}", nickname)
                            # print("------------")
                            # print
                            date_range = pd.date_range(
                                start=start_date, end=end_date, freq='D')

                            # TODO chunks
                            chunk_size_in_days = 1
                            chunks = list(utils.chunks(
                                list(date_range), chunk_size_in_days))
                            # print('len', len(chunks))
                            # print(chunks[0][0], chunks[0][-1])
                            # print("---------")
                            # print(chunks[1][0], chunks[1][-1])
                            for chunk in chunks:
                                print("---------")
                                ss_start_date = chunk[0].strftime("%Y-%m-%d")
                                ss_end_date = chunk[-1].strftime("%Y-%m-%d")
                                # print(">", start_date, end_date)

                                start_time = f'{ss_start_date} 00:00:00'
                                end_time = f'{ss_end_date} 23:59:59'

                                print(">", ss_start_date, ss_end_date)

                                query = f'''SELECT * FROM \\\"{measurement}\\\" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("{tag_key}" = '{tag_value}') tz('Asia/Bangkok')'''

                                output_gen_path = f'{database_name}/{ss_start_date}/{measurement}/{tag_key}'
                                output_file = f'{nickname}.csv'
                                target_file = os.path.join(
                                    out_dir, output_gen_path, output_file)

                                os.makedirs(os.path.join(
                                    out_dir, output_gen_path), exist_ok=True)
                                cmd = f'''influx -host {INFLUX_HOST} -port {INFLUX_PORT} -precision \'u\' -format csv -username {INFLUX_USER} -password {INFLUX_PASSWORD} -database {database_name} -execute "{query}" > {target_file} '''

                                if not dry_run:
                                    os.system(cmd)
                                # else:
                                #     print(nickname, "dry run!")
                        print("-------")

                    # for date in date_range:
                    #     print(date.strftime("%Y-%m-%d"))
                    # print(">", tag_key)

                    # pbar = tqdm(x, unit='day')
                    # for warp_date in pbar:
                    #     pbar.set_description("Processing %s" % warp_date.strftime("%Y-%m-%d"))
                    #     result = ctx.invoke(dump, start_date=warp_date, end_date=warp_date,
                    #                         out_dir=out_dir, dry_run=dry_run, database=database_name)


@ cli.command()
def config():
    ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
    str = json.dumps(ret)
    print(str)


def main():
    cli()
