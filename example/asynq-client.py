import logging

import redis

from asynq_py import gen_msg, enqueue


logger = logging.getLogger(__name__)


def main():
    r = redis.Redis(host='localhost', port=6379, db=0)
    task_type = "email:deliver"
    queue_name = "default"
    payload = {"UserID":43,"TemplateID":"some:template:id"}

    msg = gen_msg(task_type, queue_name, payload)
    enqueue(r, msg)


if __name__ == "__main__":
    main()
