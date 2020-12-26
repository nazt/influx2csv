import datetime


def getDustBoyId(str):  # return DustBoyId
    result = str.split("/status")
    result = result[0].split("/")[-1:]
    return result[0]


def getTopicValue(dct):  # return DustBoyId
    return dct['value']


def tomorrow(war_start):
    today = datetime.datetime.strptime(war_start, '%Y-%m-%d')
    x = datetime.timedelta(days=1)
    ret = today + x
    ret = ret.strftime("%Y-%m-%d")
    return ret


def getDictInfo(string):
    a = string.split("scripts/")[1]
    a = a.split(".sh")[0]
    a = a.split("_-_")
    ret = {
        'database': a[0],
        'measurement': a[1],
        'nickname': a[2],
        'datedir': a[3],
        'date': a[3]
    }

    tmp = ret['date'].split("-")
    datepath = "{}/{}".format(tmp[0], tmp[1])

    ret['datedir'] = datepath
    return ret


def exclude(a, b):
    return [x for x in a if x not in b]
