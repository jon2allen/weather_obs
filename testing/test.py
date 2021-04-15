from freezegun import freeze_time
import datetime
import unittest
import time

# Freeze time for a pytest style test:

freezer = freeze_time("2021-02-28 23:59:30", tick=True)
freezer.start()


t_begin = datetime.datetime.now()
print("starting time:",t_begin.strftime("%A, %d. %B %Y %I:%M%p"))

time.sleep(60)


t_after = datetime.datetime.now()
print("starting time:",t_after.strftime("%A, %d. %B %Y %I:%M%p"))


freezer.stop()