#!/usr/bin/env python3
# Copyright © 2022 BlueVolt, LLC
# All Rights Reserved
# 0.0.1 lfb - Initial implementation
###############################################################################################################
# University Incentives microservices - UI endpoints                                                          #
###############################################################################################################


# These are the endpoints to support the admin configuration of 
# 3rd party incentive programs. Endpoints are based on devops task 14090
# https://dev.azure.com/bluevolt/BVLMS/_workitems/edit/14090/
# target operting environment K8 container.
#
# when required the uniID and UserID will be extracted from the BVMeta header.

import asyncio
import json
import os
import socket
import time
import fastapi

# local libraries
from dal import UniIncentiveDB
#-----------------------
# Declarations         |
#-----------------------
# CONSTANTS
DbServiceUser = os.getenv('SERVICE_USER')
DbServiceKey = os.getenv('SERVICE_PASSWORD')

REQUIRED_BLUEVOLT_HEADER_KEYS = {'transactionId', 'universityId', 'userId'}
POOL_SIZE = 4
MAX_RETRIES = 10

SERVICE_NAME = 'Uni-Incentives-UI-endpoints'


#-----------------------
# Helper Functions     |
#-----------------------

def get_available_connection():
    # this routine finds the next available free connection in our cache connection pool, reserves it, and
    # passes it back to the requester.  If no connection is available after MAX_RETRIES, None is returned
    global cache_pool
    connection = None
    retries = 0
    while not connection and retries < MAX_RETRIES:
        for item in range(POOL_SIZE):
            if not cache_pool[item]['busy']:
                connection = cache_pool[item]
                cache_pool[item]['busy'] = True
                break
        if not connection:
            time.sleep(0.1)
            retries += 1
    return connection


def free_connection(pool_id):
    # this routine frees a connection up in our cache connection pool
    global cache_pool
    cache_pool[pool_id]['busy'] = False
    return

#
# initilize db connection
#
# create cache connection.  Since our kafka listener is asynchronous, we only need
# a single synchronous cache connection.  Per couchbase doc, it is best to create this globally and allow it to
# persist, for performance reasons.  If no connection could be established, set cache_status accordingly

cache_pool = []
try:
    for i in range(POOL_SIZE):
        cache_pool.append({'id': i, 'cache': CacheDB(DBUSER, DBPASS), 'busy': False})
    cache_status = 'connected'
except:
    cache_status = 'unable to connect'
    
################################
#   endpoint definitions       #
################################    

# Learner-side User Profile


# GET available incentive programs for a given user


# GET currently enrolled incentive programs for a user
#   returns array of {incentiveProgramId, incentiveProgramName, userMembershipNumber}


# UPDATE edit the User's membership number for a given program
#   Parameters: userId, uniId, membershipNumber <string>


#   Learner-side course/training track 

# GET course points
#   returns points for a given course Instance for a given programID


# GET track points
#   returns points for a completing the specified trainingtrack