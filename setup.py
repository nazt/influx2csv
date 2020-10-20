from setuptools import setup

setup(
	name="influx2csv",
	version="1.6",
	packages=["influx2csv"],
	include_package_data=True,
	author='Nat Weerawan',
	author_email='nat.wrw@gmail.com',
	license='MIT',
	install_requires=["click==7", "tqdm", "pandas", "influxdb"],
	entry_points={
		'console_scripts': ['influx2csv=influx2csv.command_line:main'],
	},	
)
