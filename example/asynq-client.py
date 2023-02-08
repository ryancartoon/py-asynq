import logging

import redis

from asynq_py import AsynqClient


logger = logging.getLogger(__name__)


def main():
    r = redis.Redis(host='localhost', port=6379, db=0)
    task_type = "email:deliver"
    queue_name = "default"
    payload = {"UserID":43,"TemplateID":"some:template:id"}

    asynq_client = AsynqClient(r)
    msg = asynq_client.gen_msg(queue_name, task_type, payload, timeout=5)
    asynq_client.enqueue(msg)


if __name__ == "__main__":
    main()
