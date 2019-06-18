from crontab import CronTab
from datetime import datetime, timedelta

cron = CronTab(user='username')
job = cron.new(command='/path/to/venv/bin/python /path/to/europarl/retrieve_tweets.py GR_heads >> /path/to/log/retrieve_tweets.log 2>&1',
               comment="Save the GR party heads tweets and the replies")
# Run the task the same day every week 0 0 * * (day of execution)
now = datetime.now()
# job.dow.on(now.weekday())
two_mins_after = now + timedelta(minutes=2)
job.minute.on(two_mins_after.minute)
job.hour.on(now.hour)
# set weekday current weekday + 1 as datetime package counts SUN as 0 and crontab counts as 0 the MON
tomorrow = now + timedelta(days=1)
job.dow.on(tomorrow.weekday())


cron.write()
