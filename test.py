import time
from uuid import uuid4

path = str(uuid4()).replace("-", "") + str(int(time.time() // 1 % 1000000))
print(path)