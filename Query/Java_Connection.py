'''
Created on Nov 6, 2017
Java based SDK client interface

@author: riteshagarwal

'''
from com.couchbase.client.java import CouchbaseCluster
from com.couchbase.client.core import CouchbaseException
from java.util.logging import Logger, Level, ConsoleHandler, FileHandler
from com.couchbase.client.java.env import DefaultCouchbaseEnvironment
from com.couchbase.client.core.retry import BestEffortRetryStrategy
from com.couchbase.client.core.env import QueryServiceConfig

env = DefaultCouchbaseEnvironment.builder().analyticsServiceConfig(QueryServiceConfig.create(2,100)).queryServiceConfig(QueryServiceConfig.create(2,100)).mutationTokensEnabled(True).computationPoolSize(5).socketConnectTimeout(100000).keepAliveTimeout(100000).keepAliveInterval(100000).connectTimeout(100000).autoreleaseAfter(10000).retryStrategy(BestEffortRetryStrategy.INSTANCE).build();
class SDKClient(object):
    """Java SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, server_ip, username, password):
        self.ip = server_ip
        self.username = username
        self.password = password
        self.cluster = None
        self.clusterManager = None
        
    def connectCluster(self, username=None, password=None):
        if username:
            self.username = username
        if password:
            self.password = password
        try:
            logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.WARNING);
            fh = FileHandler("java_client.log");
            logger.addHandler(fh);
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler) :
                    h.setLevel(Level.WARNING);
            self.cluster = CouchbaseCluster.create(env, self.ip)
            self.cluster.authenticate(self.username, self.password)
            self.clusterManager = self.cluster.clusterManager()
        except CouchbaseException:
            print "cannot login from user: %s/%s"%(self.username, self.password)
            raise

    def reconnectCluster(self):
        self.disconnectCluster()
        self.connectCluster()

    def disconnectCluster(self):
        self.cluster.disconnect()
