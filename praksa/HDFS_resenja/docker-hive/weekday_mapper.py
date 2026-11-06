import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    userid, movieid, rating, unixtime = line.split("\t")
    weekday = datetime.datetime.fromtimestamp(float(unixtime)).isoweekday()
    print("\t".join([userid, movieid, rating, str(weekday)]))
