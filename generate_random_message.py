import random
import time
from datetime import datetime

message_list = ['EMERG','CRIT','ALERT', 'ERROR', 'WARN', 'NOTICE', 'INFO', 'DEBUG']
filename = 'message_test.txt'

with open(filename, 'a') as f:
  for _ in range(50):
    f.write(str(datetime.now()) + " " + random.choice(message_list))	
    f.write("\n")
    time.sleep(0.25)
#print(str(datetime.now()) + " " + random.choice(message_list))

    
