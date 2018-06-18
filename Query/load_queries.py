'''
Created on Apr 26, 2018

@author: riteshagarwal
'''
import random, os
import json
import logging.config
from threading import Thread
import threading
import time
from optparse import OptionParser
from Java_Connection import SDKClient
from com.couchbase.client.java.analytics import AnalyticsQuery, AnalyticsParams
from java.lang import System, RuntimeException
from java.util.concurrent import TimeoutException, RejectedExecutionException,\
    TimeUnit
from com.couchbase.client.core import RequestCancelledException, CouchbaseException
import traceback, sys

def parse_options():
    parser = OptionParser()
    parser.add_option("-q", "--queries", dest="queries",
                      help="queries to be executed")
    
    parser.add_option("-s", "--server_ip", dest="server_ip",
                      help="server IP")
    
    parser.add_option("-p", "--port", dest="port",
                      help="query port")
    
    parser.add_option("-b", "--bucket", dest="bucket",
                      help="bucket name")
    
    parser.add_option("-l", "--log-level",
                      dest="loglevel", default="INFO", help="e.g -l debug,info,warning")

    parser.add_option("-n", "--querycount", dest="querycount",
                      help="Number queries to be run to be always running ton the cluster.")    

    parser.add_option("-d", "--duration", dest="duration",
                      help="Duration for which queries to be run.")
    (options, args) = parser.parse_args()
    
    return options

class query_load(SDKClient):
    
    def __init__(self, server_ip, server_port, queries, bucket, querycount):
        self.ip = server_ip
        self.port = server_port
        self.queries = queries
        self.bucket = bucket
        
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.handles = []
        self.concurrent_batch_size = 50
        self.total_count = querycount
        
        SDKClient(self.ip, "Administrator", "password")
        self.connectionLive = False
        
        self.createConn(self.bucket, "Administrator", "password")
        
    def createConn(self, bucket, username=None, password=None):
        if username:
            self.username = username
        if password:
            self.password = password

        self.connectCluster(username, password)
        System.setProperty("com.couchbase.analyticsEnabled", "true");
        System.setProperty("com.couchbase.sentRequestQueueLimit", '1000');
        self.bucket = self.cluster.openBucket(bucket);
        self.connectionLive = True

    def closeConn(self):
        if self.connectionLive:
            try:
                self.bucket.close()
                self.disconnectCluster()
                self.connectionLive = False
            except CouchbaseException as e:
                time.sleep(10)
                try:
                    self.bucket.close()
                    time.sleep(5)
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                log.error("%s"%e)
                traceback.print_exception(*sys.exc_info())
            except TimeoutException as e:
                time.sleep(10)
                try:
                    self.bucket.close()
                    time.sleep(5)
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                log.error("%s"%e)
                traceback.print_exception(*sys.exc_info())
            except RuntimeException as e:
                log.info("RuntimeException from Java SDK. %s"%str(e))
                time.sleep(10)
                try:
                    self.bucket.close()
                    time.sleep(5)
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                log.error("%s"%e)
                traceback.print_exception(*sys.exc_info())
                
    def _run_concurrent_queries(self, query, num_queries, duration = 120):
        # Run queries concurrently
        log.info("Running queries concurrently now...")
        threads = []
        total_query_count = 0
        for i in range(0, num_queries):
            total_query_count += 1
            threads.append(Thread(target=self._run_query,
                                  name="query_thread_{0}".format(total_query_count), args=(random.choice(query),False)))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                log.info("submitted {0} queries".format(i))
                time.sleep(5)
            thread.start()
            
        st_time = time.time()
        while st_time+duration > time.time():
            threads = []
            log.info("#"*50)
            log.info ("Total queries running on the cluster: %s"%self.total_count)
            new_queries_to_run = num_queries-self.total_count
            for i in range(0, new_queries_to_run):
                total_query_count += 1
                threads.append(Thread(target=self._run_query,
                                      name="query_thread_{0}".format(total_query_count), args=(random.choice(query),False)))
            i = 0
            self.total_count += new_queries_to_run
            for thread in threads:
                # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
                i += 1
                if i % self.concurrent_batch_size == 0:
                    log.info("submitted {0} queries".format(i))
#                     time.sleep(5)
                thread.start()
            
            time.sleep(2)
        log.info(
            "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                num_queries, self.failed_count, self.success_count, self.rejected_count, self.cancel_count, self.timeout_count))
        if self.failed_count+self.error_count != 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s"%(self.failed_count,self.error_count))
    
    def _run_query(self, query,validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName();
        client_context_id = name
        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_util(
                query, timeout=300,
                client_context_id=client_context_id, thread_name=name)
            # Validate if the status of the request is success, and if the count matches num_items
            if status == "success":
                if validate_item_count:
                    if results[0]['$1'] != expected_count:
                        log.info("Query result : %s", results[0]['$1'])
                        log.info(
                            "********Thread %s : failure**********",
                            name)
                        self.failed_count += 1
                        self.total_count -= 1
                    else:
                        log.info(
                            "--------Thread %s : success----------",
                            name)
                        self.success_count += 1
                        self.total_count -= 1
                else:
                    log.info("--------Thread %s : success----------",
                                  name)
                    self.success_count += 1
                    self.total_count -= 1
            else:
                log.info("Status = %s", status)
                log.info("********Thread %s : failure**********", name)
                self.failed_count += 1
                self.total_count -= 1
    
        except Exception, e:
            log.info("********EXCEPTION: Thread %s **********", name)
            if str(e) == "Request Rejected":
                log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
                self.total_count -= 1
            elif str(e) == "Request TimeoutException":
                log.info("Request TimeoutException")
                self.timeout_count += 1
                self.total_count -= 1
            elif str(e) == "Request RuntimeException":
                log.info("Request RuntimeException")
                self.timeout_count += 1
                self.total_count -= 1
            elif str(e) == "Request RequestCancelledException":
                log.info("Request RequestCancelledException")
                self.cancel_count += 1
                self.total_count -= 1
            elif str(e) == "CouchbaseException":
                log.info("General CouchbaseException")
                self.rejected_count += 1
                self.total_count -= 1
            elif str(e) == "Capacity cannot meet job requirement":
                log.info(
                    "Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
                self.total_count -= 1
            else:
                self.error_count +=1
                self.total_count -= 1
                log.info(str(e))
                
    def execute_statement_on_cbas_util(self, statement, timeout=300, client_context_id=None, username=None, password=None, analytics_timeout=300, thread_name=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        try:
            log.info("Running query on cbas via %s: %s"%(thread_name,statement))
            response = self.execute_statement_on_cbas(statement, pretty, client_context_id, username, password,timeout=timeout, analytics_timeout=analytics_timeout)
            
            if type(response) == str: 
                response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
                if type(errors) == str:
                    errors = json.loads(errors)
            else:
                errors = None
    
            if "results" in response:
                results = response["results"]
            else:
                results = None
    
            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None
            
            if "metrics" in response:
                metrics = response["metrics"]
                if type(metrics) == str:
                    metrics = json.loads(metrics)
            else:
                metrics = None
                
            return response["status"], metrics, errors, results, handle
    
        except Exception,e:
            raise Exception(str(e))
        
    def execute_statement_on_cbas(self, statement, pretty=True, 
        client_context_id=None, 
        username=None, password=None, timeout = 300, analytics_timeout=300):

        params = AnalyticsParams.build()
        params = params.rawParam("pretty", pretty)
        params = params.rawParam("timeout", str(analytics_timeout)+"s")
        params = params.rawParam("username", username)
        params = params.rawParam("password", password)
        params = params.rawParam("clientContextID", client_context_id)
        if client_context_id:
            params = params.withContextId(client_context_id)
        
        output = {}
        q = AnalyticsQuery.simple(statement, params)
        try:
            result = self.bucket.query(q, 3600, TimeUnit.SECONDS)
            
            output["status"] = result.status()
            output["metrics"] = str(result.info().asJsonObject())
            
            try:
                output["results"] = str(result.allRows())
            except:
                output["results"] = None
                
            output["errors"] = json.loads(str(result.errors()))
            
            if str(output['status']) == "fatal":
                msg = output['errors'][0]['msg']
                if "Job requirement" in  msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif str(output['status']) == "success":
                output["errors"] = None
                pass
            else:
                log.info("analytics query %s failed status:{0},content:{1}".format(
                    output["status"], result))
                raise Exception("Analytics Service API failed")
            
        except TimeoutException as e:
            log.info("Request TimeoutException from Java SDK. %s"%str(e))
            traceback.print_exception(*sys.exc_info())
            raise Exception("Request TimeoutException")
        except RequestCancelledException as e:
            log.info("RequestCancelledException from Java SDK. %s"%str(e))
            traceback.print_exception(*sys.exc_info())
            raise Exception("Request RequestCancelledException")
        except RejectedExecutionException as e:
            log.info("Request RejectedExecutionException from Java SDK. %s"%str(e))
            traceback.print_exception(*sys.exc_info())
            raise Exception("Request Rejected")
        except CouchbaseException as e:
            log.info("CouchbaseException from Java SDK. %s"%str(e))
            traceback.print_exception(*sys.exc_info())
            raise Exception("CouchbaseException")
        except RuntimeException as e:
            log.info("RuntimeException from Java SDK. %s"%str(e))
            traceback.print_exception(*sys.exc_info())
            raise Exception("Request RuntimeException")
        return output
    
def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("jython.logging.conf")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        newline = line.replace("@@LEVEL@@", level)
        newline = newline.replace("@@FILENAME@@", log_file_name.replace('\\', '/'))
        log_file.write(newline)
    log_file.close()
    tmpl_log_file.close()

def setup_log(options):
    abs_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
    root_log_dir = os.path.join(abs_path, "logs{0}queryrunner-{1}".format(os.sep, str_time))
    if not os.path.exists(root_log_dir):
        os.makedirs(root_log_dir)
    logs_folder = os.path.join(root_log_dir, "querylogs_%s" % time.time())
    os.mkdir(logs_folder)
    test_log_file = os.path.join(logs_folder, "test.log")
    log_config_filename = r'{0}'.format(os.path.join(logs_folder, "test.logging.conf"))
    create_log_file(log_config_filename, test_log_file, options.loglevel)
    logging.config.fileConfig(log_config_filename)
    print "Logs will be stored at {0}".format(logs_folder)

log = logging.getLogger()
options = None
def main():
    options = parse_options()
    setup_log(options)
    load = query_load(options.server_ip, options.port, [], options.bucket,int(options.querycount))
    query = ['SELECT name as id, result as bucketName, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds1 where duration between 3009 and 3010 and profile is not missing and array_length(profile.friends) > 5 limit 100',
             'SELECT name as id, result as bucketName, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds2 where duration between 3009 and 3010 and profile is not missing',
             'select sum(friends.num_friends) from (select array_length(profile.friends) as num_friends from ds3) as friends',
             'SELECT name as id, result as Result, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds4 where result = "SUCCESS" and profile is not missing and array_length(profile.friends) = 5 and duration between 3009 and 3010 UNION ALL SELECT name as id, result as Result, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds4 where result != "SUCCESS" and profile is not missing and array_length(profile.friends) = 5 and duration between 3010 and 3012']
    
    load._run_concurrent_queries(query, int(options.querycount), duration=int(options.duration))
    print load.total_count
    print "Done!!"

'''
    /opt/jython/bin/jython -J-cp '../Couchbase-Java-Client-2.5.6/*' load_queries.py --server_ip 172.23.108.231 --port 8095 --duration 600 --bucket default --querycount 10
'''
if __name__ == "__main__":
    main()
