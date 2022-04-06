# Copyright © 2021 BlueVolt Technology, LLC
# All Rights Reserved
###############################################################################################################
# Class library for server-side database interaction                                                          #
# The role of this code is to separate the database connection logic out of the event processor code, create  #
# a persistent database connection, and handle commits for insert/update/delete transactions automatically.   #
#                                                                                                             #
###############################################################################################################
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
from couchbase_core.cluster import PasswordAuthenticator

class CacheDB:
    def __init__(self, user, passwd, bucket_name='BvUniversity',
                 collection_name="_default", host='couchbase://cache.bluevolt.local'):
        self.cluster = None
        self.bucket = None
        self.scope = None
        self.collection = None
        self.bucket_name = bucket_name
        self.scope_name = '_default'
        self.collection_name = collection_name
        self.user = user
        self.passwd = passwd
        self.host = host
        self.connect()
    #

    def connect(self):
        self.cluster = Cluster(self.host, ClusterOptions(PasswordAuthenticator(self.user, self.passwd)))
        self.bucket = self.cluster.bucket(self.bucket_name)
        self.collection = self.bucket.scope(self.scope_name).collection(self.collection_name)
    #

    def query(self, n1ql, params=[], autocommit=False):
        #if self.conn.closed:  # first off, lets confirm that we are still connected to the database.  Reconnect if not.
        #    self.connect()
        #    self.open_cursor()
        try:
            if params:
                result = self.cluster.query(n1ql, QueryOptions(positional_parameters=params))
            else:
                result = self.cluster.query(n1ql)
        except:
            result = {}
        #if autocommit and result:sql
        #    self.conn.commit()
        return result
    #
    
    def get_available_incentives_for_uni(self, universityId):
        # reterive incentive programs availavble for the specified university
        rows = self.query("SELECT incentiveProgramId, programName FROM incentivePorgrams WHERE memberUniversities.uniId = $1", [universityId])
        incentivePrograms = []
        for row in rows:
            incentivePrograms.append(row)
        return incentivePrograms