'''
Created on Nov 6, 2017
Java based SDK client interface

@author: riteshagarwal

'''
import sys

sys.path.append(
    "/Users/praneeth.bokka/repositories/AnalyticsQueryApp/Couchbase-Java-Client-3.3.0/java-client-3.3.0.jar")
sys.path.append("/Users/praneeth.bokka/repositories/AnalyticsQueryApp/Couchbase-Java-Client-3.3.0/core-io-2.3.0.jar")
sys.path.append(
    "/Users/praneeth.bokka/repositories/AnalyticsQueryApp/Couchbase-Java-Client-3.3.0/reactor-core-3.4.17.jar")
sys.path.append(
    "/Users/praneeth.bokka/repositories/AnalyticsQueryApp/Couchbase-Java-Client-3.3.0/reactive-streams-1.0.3.jar")

from com.couchbase.client.java import Cluster, ClusterOptions
from com.couchbase.client.core.error import CouchbaseException
from java.util.logging import Logger, Level, ConsoleHandler, FileHandler
from java.time import Duration

from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.core.retry import BestEffortRetryStrategy
from com.couchbase.client.core.env import CompressionConfig, TimeoutConfig, SecurityConfig, \
    IoConfig
from com.couchbase.client.core.deps.io.netty.handler.ssl.util import InsecureTrustManagerFactory

env = ClusterEnvironment.builder() \
    .compressionConfig(CompressionConfig.create().enable(True)) \
    .timeoutConfig(TimeoutConfig
                   .kvTimeout(Duration.ofSeconds(60))
                   .queryTimeout(Duration.ofSeconds(100))
                   .searchTimeout(Duration.ofSeconds(100))
                   .analyticsTimeout(Duration.ofSeconds(100))) \
    .securityConfig(SecurityConfig.enableTls(True)
                    .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)) \
    .ioConfig(IoConfig.numKvConnections(2)
              .enableDnsSrv(True)) \
    .build();


class SDKClient(object):
    """Java SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, server_ip, username, password):
        self.ip = server_ip
        self.username = username
        self.password = password
        self.cluster = None

    def connectCluster(self):
        try:
            logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.SEVERE);
            #             fh = FileHandler("java_client.log");
            #             logger.addHandler(fh);
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler):
                    h.setLevel(Level.SEVERE);
            print("Username used for login:{0}".format(self.username))
            print("Password used for login:{0}".format(self.password))

            self.cluster = Cluster.connect(self.ip,
                                           ClusterOptions.clusterOptions(self.username, self.password).environment(
                                               env));
        except CouchbaseException:
            print "cannot login from user: %s/%s" % (self.username, self.password)
            raise

    def reconnectCluster(self):
        self.disconnectCluster()
        self.connectCluster()

    def disconnectCluster(self):
        self.cluster.disconnect()
