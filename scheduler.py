import time
from datetime import datetime

def set_schedule():
  while True:
    date_time_str = input("Set a schedule? (YYYY-MM-DD HH:MM:SS|Blank=Now): ") #'2020-03-02' # 08:15:27.243860'
    if date_time_str == '':
      return None
    elif len(date_time_str.split(' ')) == 2 and len(date_time_str.split('-')) == 3:
      if len(date_time_str.split(':')) == 3:
        # already in correct format
        break
      elif len(date_time_str.split(':')) == 2:
        # strip any right tail space
        date_time_str.rstrip(' ')
        # add seconds
        date_time_str += ':0'
        break
    elif len(date_time_str.split(' ')) < 2:
      if len(date_time_str.split('-')) == 3:
        date_time_str += ' 0:0:0'
        break
      elif len(date_time_str.split(':')) == 3:
        date_time_str = f'{str(datetime.now().date())} {date_time_str}'
        break
      elif len(date_time_str.split(':')) == 2:
        date_time_str = f'{str(datetime.now().date())} {date_time_str}:0'
        break
      else:
        print("Error: wrong datetime format!")
  date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
  print(f"Job scheduled at: {date_time_obj}")
  return date_time_obj

def sleep_until(date_time_obj):
  while datetime.now() < date_time_obj:
    print(f"Counting down {str(date_time_obj - datetime.now()).split('.')[0]}")
    time.sleep(1)
  # print flag
  print("Job started!")