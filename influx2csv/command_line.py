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

    if __name__ == '__main__':
        cli()


def show_tag_values_by_tag_key(client, database_name, tag_key, func_string):
    for tag_value in utils.get_tag_values(client, database_name, tag_key):
        fn = lua.eval(func_string)
        print(tag_value, fn(tag_value))

    return utils.get_tag_values(client, database_name, tag_key)


def filename(path):
    return os.path.basename(path).split('.csv')[0]


@cli.command("show-databases")
def show_databases():
    # databases = [db['name'] for db in client.get_list_database()]
    # databases.remove("_internal")
    databases = utils.get_databases(client)

    click.echo(databases)


def syscall():
    pass


@cli.command()
@click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), required=True)
@click.option('--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), required=False)
@click.option('--out-dir', type=str, required=True)
@click.option('--chunk-size', type=int, required=True)
@click.option('--dry-run', type=bool, required=False, default=False)
@click.pass_context
def dump_range(ctx, start_date, end_date, out_dir, chunk_size, dry_run):
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

    skip_date = True
    print("=== DATABASES ===")
    for idx, database_name in enumerate(databases, start=1):
        print("--------------")
        print("📍", f'{idx}/{len(databases)}', database_name)

        if database_name in ["test1", "data-mart"]:
            continue

        # Iterate over measurement
        for measurement in utils.get_measurements(client, database_name):
            print("->", measurement)
            tag_keys = utils.get_tag_keys(
                client, database_name, measurement)
            # Iterate over tag_key to list all tag keys and tag values
            for tag_key in tag_keys:
                print(" >", tag_key)
                tag_values = utils.get_tag_values(
                    client, database_name, measurement, tag_key)
                user_config = cfg['query']['config']
                if tag_key in user_config['logic'].keys():
                    print(f'yay found {tag_key}!')
                    print("--------------")
                    funcs = user_config['logic'][tag_key]['funcs']
                    filter_func = lua.eval(funcs['filter'])
                    # transform_func = lua.eval(funcs['transform'])
                    for idx, tag_value in enumerate(tag_values, start=1):
                        if filter_func(tag_value):
                            nickname = tag_value.split("/")[-2:-1][0]
                            print(idx, f"[{tag_key}] -> {tag_value}", nickname)
                            date_range = pd.date_range(
                                start=start_date, end=end_date, freq='D')

                            # TODO chunks
                            chunk_size_in_days = chunk_size
                            chunks = list(utils.chunks(
                                list(date_range), chunk_size_in_days))

                            for chunk in chunks:
                                print("---------")
                                ss_start_date = chunk[0].strftime("%Y-%m-%d")
                                ss_end_date = chunk[-1].strftime("%Y-%m-%d")
                                # print(">", start_date, end_date)
                                start_time = f'{ss_start_date} 00:00:00'
                                end_time = f'{ss_end_date} 23:59:59'

                                print(">", ss_start_date, ss_end_date)

                                query = f'''SELECT * FROM \\\"{measurement}\\\" WHERE (time >= '{start_time}' AND time <= '{end_time}') AND ("{tag_key}" = '{tag_value}') tz('Asia/Bangkok')'''

                                target_file = utils.generate_output_path(out_dir,
                                                                         database_name, ss_start_date, measurement, tag_key, nickname)
                                cmd = f'''influx -host {INFLUX_HOST} -port {INFLUX_PORT} -precision \'u\' -format csv -username {INFLUX_USER} -password {INFLUX_PASSWORD} -database {database_name} -execute "{query}" > {target_file} '''

                                if not dry_run:
                                    os.system(cmd)
                                # else:
                                #     print(nickname, "dry run!")
                        else:
                            print(f'🧹 {tag_value}')


@cli.command()
def config():
    ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
    str = json.dumps(ret)
    print(str)


def main():
    cli()
