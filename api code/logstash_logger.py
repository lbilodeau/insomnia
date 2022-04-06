#!/usr/bin/env python3
# vim: set fileencoding=UTF-8 :
# Copyright © 2021 BlueVolt, LLC
# All Rights Reserved
import datetime
import json
import time
import socket
import os

LOGSTASH_CONNECTION_RETRY_LIMIT = 10


def send_log_message(status, payload, data, service_component):
    message = {}
    try:
        if "log_level" in data:
            log_level = data["log_level"]
            del(data["log_level"])
        else:
            log_level = "INFO" if status == "success" else "ERROR"
        if "status" in data:
            execution_status = data["status"]
        else:
            execution_status = status
        if "executionTime" in data:
            execution_time = data["executionTime"]
            del(data["executionTime"])
        else:
            execution_time = None
        if "startTime" in data:
            start_time = data["startTime"]
            del(data["startTime"])
        else:
            start_time = None
        message = {
            'transactionId': payload['bvMeta']['transactionId'],
            'service': 'bv_workflow_engine',
            'serviceComponent': service_component,
            'status': execution_status,
            'universityId': payload['bvMeta']['universityId'],
            'userId': payload['bvMeta']['userId'],
            'level': log_level,
            'executionTime': execution_time,
            'startTime': start_time,
            'data': data
        }
        error_message, connected = send_logstash_message(message)
        if error_message is not None and not connected:
            message["data"]["logstashError"] = error_message
        print("[send_log_message]: Message: " + json.dumps(message))
        return connected, message
    except Exception as e:
        message["data"]["logstashError"] = str(e)
        print("[send_log_message]: Error: " + str(e) + " Message: " + json.dumps(message))
        return False, message


def send_logstash_message(message):
    try:
        retry_count = 0
        connected = False
        error_message = None
        logstash_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while not connected and retry_count < LOGSTASH_CONNECTION_RETRY_LIMIT:
            try:
                logstash_client.connect(("logger.bluevolt.local", 5959))
                connected = True
                error_message = None
            except Exception as e:
                error_message = str(e)
                time.sleep(0.05)
            retry_count += 1
        if connected:
            logstash_client.sendall(json.dumps(message).encode())
            logstash_client.shutdown(socket.SHUT_RDWR)
            logstash_client.close()
        return error_message, connected
    except Exception as e:
        error_message = str(e)
        return error_message, False
