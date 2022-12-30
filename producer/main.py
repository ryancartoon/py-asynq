import redis
import json
import uuid


lua_script = """
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
"""

all_queues = "asynq:queues"    

class TaskMessage:
    
    def __init__(self):
        self.id = uuid.uuid4()
        self.type = "job:backup"
        self.queue = "default"
        self.payload = json.dumps(dict(id=10000))


def subscript(r: redis.Redis, payload):
    t = TaskMessage()
    qname = "default"
    r.sadd(all_queues, qname)
    keys =[
        "asynq:{%s}:t:%s", qname, t.id,
        "asyncq:{%s}:pending"
    ]
    schedule_cmd = r.register_script(lua_script)
    schedule_cmd(payload)


def main():
    r = redis.Redis(host='localhost', port=6379, db=0)
    payload = json.dumps(dict())
    queue_name = "asynq:queues"
    subscript(r, queue_name, payload)