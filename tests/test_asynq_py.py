from uuid import uuid4

from asynq_py import AsynqClient


def test_asynq_client_enqueue(redis_conn):
    asynq_client = AsynqClient(redis_conn)
    msg_id = str(uuid4())
    queue_name = "test_queue_name"
    msg_type = "test_type"
    payload = dict(foo="bar")
    msg = asynq_client.gen_msg(queue_name, msg_type, payload)
    msg.id = msg_id

    try: 
        asynq_client.enqueue(msg)

        # asynq:{test_queue_name}:t:e6adba7b-018f-4912-9b3f-ad97fe00f00c  hash
        # asynq:{test_queue_name}:pending list
        task_info_key = "asynq:{%s}:t:%s" % (msg.queue, msg.id)
        pending_task_key = "asynq:{%s}:pending" % (msg.queue)

        task_info = redis_conn.hgetall(task_info_key)
        task_ids = redis_conn.lrange(pending_task_key, 0, -1)

        assert task_info[b'state'] == b'pending'
        assert task_info[b'msg'] == msg.SerializeToString()
        assert task_ids == [msg_id.encode()]

    finally:
        redis_conn.delete(task_info_key, pending_task_key)