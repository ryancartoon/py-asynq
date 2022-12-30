import redis
import json
import uuid
import logging
import time

from .proto.asynq_pb2 import TaskMessage


logger = logging.getLogger(__name__)


ENQUEUE_LUA_SCRIPT = """
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
"""

ALL_QUEUES = "asynq:queues"


class AsynqClient:
    def __init__(self, redis: redis.Redis):
        self.redis = redis

    def enqueue(self, msg: TaskMessage) -> None:
        self.redis.sadd(ALL_QUEUES, msg.queue.encode())

        keys =[
            "asynq:{%s}:t:%s" % (msg.queue, msg.id),
            "asynq:{%s}:pending" % (msg.queue)
        ]
        posix_time_nano = time.time_ns()

        arg = [
            msg.SerializeToString(),
            msg.id,
            posix_time_nano,
        ]

        logger.info("keys are %s", keys)
        logger.info("args are %s", arg)

        enqueue_cmd = self.redis.register_script(ENQUEUE_LUA_SCRIPT)
        enqueue_cmd(keys=keys, args=arg)

    @staticmethod
    def gen_msg(queue_name:str, typ:str,  payload:dict, **kwargs) -> TaskMessage:
        t = TaskMessage()
        t.id = str(uuid.uuid4())
        t.retry = kwargs.get("retry", 0)
        t.timeout = kwargs.get("timeout", 0)

        t.type = typ
        t.queue = queue_name
        t.payload = json.dumps(payload).encode()

        return t
    
    def enqueue_task(self, queue_name: str, task_type: str, payload: dict, **kwargs) -> None:
        msg = self.gen_msg(queue_name, task_type, payload, **kwargs)
        self.enqueue(msg)
