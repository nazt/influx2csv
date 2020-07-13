from setuptools import setup

setup(
	name="influx2csv",
	version="1.0",
	py_modules=["influx2csv"],
	include_package_data=True,
	install_requires=["click", "tqdm", "pandas", "influxdb"],
	entry_points={
		'console_scripts': ['influx2csv=influx2csv:cli'],
	},	
)
