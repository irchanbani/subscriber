import json
import logging
import time
from json import JSONDecodeError

import os

from loggers import setup_logging
from subscriber import Subscriber


logger = logging.getLogger(__name__)

project_id = ""
subscription_name = "dev.subs"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""


def callback(message):
    try:
        job_data = json.loads(message.data.decode("utf-8"))
        logger.debug("Got job: {}".format(job_data))
        # print('cek: ', job_data)

        with open("filename", "a") as f:
            f.write(message.data.decode("utf-8") + os.linesep)

        # if message.publish_time:
        #     print("publish time: ", message.publish_time,
        #           " timestamp: ", message.publish_time.timestamp(),
        #           " time: ", time.time())
        # else:
        #     print("nothing")

        if message.attributes:
            print("Attributes: {}", message.attributes)
            temp = []
            for key in message.attributes:
                value = message.attributes.get(key).decode("utf-8")
                res = (key, value)
                print('{}: {}'.format(key, value))
                temp.append(res)
            with open("file_attributes", "a") as f:
                f.write(str(temp) + os.linesep)

    except (JSONDecodeError, TypeError, AttributeError):
        logger.exception("invalid message: {}".format(message.data))
        message.ack()
        return
    message.ack()
    return


def main():
    subscriber = Subscriber(project_id, subscription_name)
    subscription = subscriber.subscribe()

    while True:
        time.sleep(2)

        subscription.open(callback)
        logger.info("Open Subscriber")

        time.sleep(10)

        subscription.close()
        logger.info("Close Subscriber")


if __name__ == "__main__":
    logging.getLogger("googleapiclient.discovery_cache").setLevel(
        logging.ERROR)
    logging.getLogger("urllib3.connectionpool").setLevel("INFO")
    logging.getLogger("urllib3.util.retry").setLevel("INFO")
    logging.getLogger(
        "google.cloud.pubsub_v1.subscriber.policy.thread").setLevel("INFO")
    logging.getLogger(
        "google.cloud.pubsub_v1.subscriber.policy.base").setLevel("INFO")
    logging.getLogger(
        "google.cloud.pubsub_v1.subscriber._helper_threads").setLevel("INFO")
    logging.getLogger(
        "google.cloud.pubsub_v1.subscriber._consumer").setLevel("INFO")

    setup_logging(
        "worker_v2",
        "basic",
        "DEBUG",
        log_dir="/logs"
    )
    main()
