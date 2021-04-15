#schedule test
import time
import datetime
import schedule

def test_function():
  print("test function")

job1 = schedule.every(2).minutes.do(test_function)

while True:
    # run_pending
    schedule.run_pending()
    schedule.get_jobs()
    time.sleep(2)
    job1.run()
    print ("schedule: ", schedule.idle_seconds())
    