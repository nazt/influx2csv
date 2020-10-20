import glob, os, json
import pandas as pd

from influxdb import InfluxDBClient
from datetime import date

from . import utils

import sys

assert sys.version[:1] == "3"

import click


INFLUX_HOST = ''
INFLUX_USER = ''
INFLUX_PASSWORD = ''
INFLUX_PORT = 8086
client = ''


@click.group()
@click.option('--config', type=click.Path(), help='load config files')
def cli(config):
	"""influx2csv"""
	if not config:
		pass
	else:
		with open(config, 'r') as f:
			ret = json.load(f)

		global INFLUX_HOST, INFLUX_USER, INFLUX_PORT, INFLUX_PASSWORD
		global client

		INFLUX_USER = ret['username']
		INFLUX_PASSWORD = ret['password']
		INFLUX_PORT = ret['port']
		INFLUX_HOST = ret['host']

		client = InfluxDBClient(host=INFLUX_HOST, username=INFLUX_USER, password=INFLUX_PASSWORD, port=INFLUX_PORT)

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
@cli.command("show-databases")
def show_databases():
	databases = [db['name'] for db in client.get_list_database()]
	databases.remove("_internal")

	click.echo(databases)


def alldbs():
	excludes = ['kadyaidb', 'laris1db', 'aqithaidb', 'aqithaicom_db', 'dustboy']
	databases = [db['name'] for db in client.get_list_database() if db['name'] not in excludes]
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
				pass
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
					mapping[measurement['name'] + utils.getDustBoyId(topic_val)] = topic_val
	print(json.dumps(mapping))
	return mapping


@cli.command()
@click.option('--out-dir', type=str, required=True)
@click.option('--dry-run', count=True)
def clear_scripts(out_dir, dry_run):
	scripts = sorted(glob.glob('{0}/scripts/*.sh'.format(out_dir)))
	delete = 0
	for script in scripts:
		ret = utils.getDictInfo(script)
		filename = "{}_{}".format(ret['nickname'], ret['date'])
		check_file = "{}/csv/{}/{}/{}/{}/{}.csv".format(out_dir, ret['database'], ret['measurement'], ret['nickname'],
														ret['datedir'], filename)
		if os.path.exists(check_file):
			delete = delete + 1
			if dry_run:
				print("DRY RUN")
			else:
				os.remove(script)

	print("{} has been DELETED".format(delete))


# print('a')


@cli.command()
def show_measurements():
	mapping = mm()


@cli.command()
@click.option('--date-start', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
@click.option('--date-end', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
@click.option('--out-dir', type=str, required=True)
def dumpall(date_start, date_end, out_dir):
	print(date_start.date(), utils.tomorrow(str(date_end.date())))
	mapping = alldbs()
	skipped = 0
	for d in mapping:
		skipped = skipped + dd(date_start, date_end, d['db'], d['measurement'], d['nickname'], d['topic'], out_dir)

	print("SKIPPED={}".format(skipped))



def dd(date_start, date_end, database_name, measurement_name, nickname, topic, out_dir):
	tomorrow = utils.tomorrow(str(date_end.date()))
	x = pd.date_range(start=date_start, end=tomorrow, freq='D')
	cmd = 'time influx -host {} -precision \'u\' -username {} -password {} -database {}'.format(
		INFLUX_HOST, INFLUX_USER, INFLUX_PASSWORD, database_name)
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
		c = cmd + ' -execute "SELECT * FROM \\"{}\\" WHERE time >= \'{}\' AND time < \'{}\' AND ("topic"=\'{}\') tz(\'Asia/Bangkok\')" -format csv > {}/{}'.format(
			measurement_name, today, tomorrow, topic, dirpath, outfile)
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
@click.option('--date-start', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
@click.option('--date-end', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()))
# @click.option('--measurement-name', type=str, required=True)
# @click.option('--nickname', type=str, required=True)
# @click.option('--database-name', type=str, required=True)
def dump(date_start, date_end, measurement_name, database_name, nickname):
	date_start = date_start.date()
	date_end = date_end.date()

	mapping = mm()
	# print(mapping)
	# print(date_start, utils.tomorrow(str(date_start)))
	x = pd.date_range(start=date_start, end=date_end, freq='D')

	# print(date_start.timetuple().tm_yday)
	cmd = 'time influx -host {} -port {} -precision \'u\' -username {} -password {} -database {}'.format(
		INFLUX_HOST, INFLUX_USER, INFLUX_PORT, INFLUX_PASSWORD, database_name)
	# print("alias infx='{}'".format(cmd))
	for i in x:
		# timetuple = pd.to_datetime(i).timetuple()
		today = str(i.date())
		tomorrow = utils.tomorrow(today)
		outfile = "{}_{}.csv".format(nickname, today)
		c = cmd + ' -execute "SELECT * FROM \\"{}\\" WHERE time >= \'{}\' AND time < \'{}\' AND ("topic"=\'{}\') tz(\'Asia/Bangkok\')" -format csv > {}'.format(
			measurement_name, today, tomorrow, mapping[measurement_name + nickname], outfile)
		print(c)


@cli.command()
def config():
	ret = {'username': '', 'password': '', 'host': '', 'port': 8086}
	str = json.dumps(ret)
	print(str)


def main():
	cli()
