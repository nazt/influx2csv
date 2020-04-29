# Installation
    pip install --editable .
    pip install git+git://github.com/nazt/influx2csv.git#egg=influx2csv


#
    influx2csv config > $HOME/mytarget/config.json
    
#
    influx2csv --config=/Users/nat/mytarget/config.json show-measurements
    influx2csv --config=/Users/nat/ccdc/config.json dumpall --date-start=2020-04-01 --out-dir=/Users/nat/ccdc


    influx2csv clear-scripts --out-dir=/Users/nat/ccdc
    
    for i in $(find scripts -name \*.sh); do echo "$i"; bash "$i"; done
    find csv2 -size 0 -delete
