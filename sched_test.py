import schedule
import logging

logging.basicConfig()
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(level=logging.DEBUG)

def job():
    print("Hello, Logs")

schedule.every().second.do(job)

schedule.run_all()

schedule.clear()