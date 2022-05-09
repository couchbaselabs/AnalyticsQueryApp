'''
Created on Apr 26, 2018

@author: riteshagarwal
'''
print ("Custom load_queries image loaded")
import random
import json
import logging.config
import uuid
from threading import Thread
import threading
import time
import requests
from optparse import OptionParser
from Java_Connection import SDKClient
from com.couchbase.client.java.analytics import AnalyticsOptions
from com.couchbase.client.java.query import QueryOptions, QueryScanConsistency
from java.time import Duration

from java.lang import System, RuntimeException
from java.util.concurrent import TimeoutException, RejectedExecutionException, \
    TimeUnit
from com.couchbase.client.core.error import RequestCanceledException, CouchbaseException
import traceback, sys
from datetime import datetime
import importlib

HOTEL_DS_IDX_QUERY_TEMPLATES = [
    {"idx1": "select meta().id from keyspacenameplaceholder where country is not null and `type` is not null "
             "and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100 ",
     "idx2": "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from keyspacenameplaceholder "
             "where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 "
             "and `type`='Hotel' group by country limit 100",
     "idx3": "select city,country,count(*) from keyspacenameplaceholder where free_breakfast=True and free_parking=True "
             "group by country,city order by country,city limit 100 offset 100",
     "idx4": "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM keyspacenameplaceholder WHERE price IS NOT NULL GROUP BY city) "
             "SELECT h.name, h.price FROM keyspacenameplaceholder h JOIN city_avg ON h.city = city_avg.city WHERE "
             "h.price < city_avg.avgprice AND h.price IS NOT NULL LIMIT 10;",
     "idx5": "SELECT h.name, h.city, r.author FROM keyspacenameplaceholder h UNNEST reviews AS r WHERE r.ratings.Rooms < 2 "
             "AND h.avg_rating >= 3 ORDER BY r.author DESC LIMIT 100;",
     "idx6": "SELECT COUNT(*) FILTER (WHERE free_breakfast = TRUE) AS count_free_breakfast, "
             "COUNT(*) FILTER (WHERE free_parking = TRUE) AS count_free_parking, "
             "COUNT(*) FILTER (WHERE free_breakfast = TRUE AND free_parking = TRUE) AS count_free_parking_and_breakfast "
             "FROM keyspacenameplaceholder WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC LIMIT 10",
     "idx7": "SELECT h.name,h.country,h.city,h.price,DENSE_RANK() OVER (window1) AS `rank` FROM keyspacenameplaceholder "
             "AS h WHERE h.price IS NOT NULL WINDOW window1 AS ( PARTITION BY h.country ORDER BY h.price NULLS LAST) LIMIT 10;",
     "idx8": "SELECT * FROM keyspacenameplaceholder AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND "
             "r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL",
     "idx9": "SELECT * FROM keyspacenameplaceholder AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and "
             "r.ratings.Rooms > 3 END AND free_parking = True",
     "idx10": "SELECT * FROM keyspacenameplaceholder AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings "
              "SATISFIES n = 'Overall' AND v = 2 END END",
     "idx11": "SELECT * FROM keyspacenameplaceholder AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and "
              "r.ratings.Cleanliness > 1 END AND free_parking = True"

     }
]

HOTEL_DS_IDX_QUERY_INSERT_TEMPLATES = [{
                                           "idx1": "INSERT INTO keyspacenameplaceholder (KEY, VALUE) VALUES (UUID(), { 'type': 'hotel', 'name' : 'new hotel' })",
                                           "idx2": "INSERT INTO keyspacenameplaceholder (KEY UUID(), VALUE price) SELECT price FROM keyspacenameplaceholder WHERE type='Hotel' AND free_breakfast=True AND free_parking=True LIMIT 100",
                                           "idx3": "INSERT INTO keyspacenameplaceholder (KEY, VALUE) VALUES (UUID(), { 'int': 123})",
                                           "idx4": "INSERT INTO keyspacenameplaceholder (KEY UUID(), VALUE _v) SELECT _v FROM keyspacenameplaceholder _v where price is not missing order by meta().id desc limit 1",
                                           "idx5": "INSERT INTO keyspacenameplaceholder (KEY, VALUE) VALUES (UUID(), { 'int': 123})",
										   "idx6": "INSERT INTO keyspacenameplaceholder (KEY UUID(), VALUE _v) SELECT _v FROM keyspacenameplaceholder _v where city is not missing order by meta().id desc limit 10",
										   "idx7": "INSERT INTO keyspacenameplaceholder (KEY UUID(), VALUE _v) SELECT _v FROM keyspacenameplaceholder _v where price is not missing and lower(country) like 'united%' order by meta().id desc limit 1"}]

HOTEL_DS_IDX_QUERY_UPDATE_TEMPLATES = [{"idx1": "UPDATE keyspacenameplaceholder SET foo = 5 where country is not missing limit 100",
                                        "idx2": "UPDATE keyspacenameplaceholder SET free_breakfast = False where free_breakfast=True limit 100",
                                        "idx3": "UPDATE keyspacenameplaceholder SET city = 'San Francisco' where free_breakfast=True and free_parking=True limit 100",
                                        "idx4": "UPDATE keyspacenameplaceholder SET price = price+100 where price is not missing limit 100",
                                        "idx5": "UPDATE keyspacenameplaceholder SET updated = price where (any r in reviews satisfies r.ratings.`Rooms` > 3 end) limit 100",
                                        "idx6": "UPDATE keyspacenameplaceholder SET price = price+100 where city is not missing limit 100",
                                        "idx7": "UPDATE keyspacenameplaceholder SET price = price+100 where price is not missing and lower(country) like 'united%' limit 100",
                                        }]

HOTEL_DS_IDX_QUERY_DELETE_TEMPLATES = [{"idx1": "delete from keyspacenameplaceholder where country is not missing limit 25 ",
                                        "idx2": "delete from keyspacenameplaceholder where free_breakfast=True limit 200",
                                        "idx3": "delete from keyspacenameplaceholder where free_breakfast=True and free_parking=True limit 10",
                                        "idx4": "delete from keyspacenameplaceholder where price is not missing limit 100",
                                        "idx5": "delete from keyspacenameplaceholder where (any r in reviews satisfies r.ratings.`Rooms` > 3 end) limit 50",
                                        "idx6": "delete from keyspacenameplaceholder where city is not missing limit 5",
                                        "idx7": "delete from keyspacenameplaceholder where price is not missing and lower(country) like 'united%' limit 89"
                                        }]

HOTEL_DS_IDX_QUERY_MERGE_TEMPLATES = [{
                                          "idx1": "MERGE INTO keyspacenameplaceholder p USING [{'country':'Gibraltar', 'email': 'Lebsack.Freddie@hotels.com'},{'country':'Macedonia', 'email': 'username.lastname@hotels.com'},{'country':'Finland', 'email': 'Bruen.Lacy@hotels.com'},{'country':'Paraguay', 'email': 'fake.name@hotels.com'},{'country':'Cambodia', 'email': 'user.name@hotels.com'}] o ON  o.country == p.country WHEN MATCHED THEN UPDATE SET p.email = o.email limit 100",
                                          "idx2": "MERGE INTO keyspacenameplaceholder p USING [{'country':'Gibraltar', 'price': 1146},{'country':'Macedonia', 'price': 1150},{'country':'Finland', 'price': 1147},{'country':'Paraguay', 'price': 1148},{'country':'Cambodia', 'price': 1149}] o ON  o.free_breakfast == p.free_breakfast WHEN MATCHED THEN UPDATE SET p.country = o.country limit 100",
                                          "idx3": "MERGE INTO keyspacenameplaceholder p USING [{'city': 'Hattieview','free_breakfast': true},{'city': 'Blakefort','free_breakfast': true},{'city': 'Chuview','free_breakfast': false},{'city': 'Vonfort','free_breakfast': true},{'city': 'Quitzonview','free_breakfast': false}] o ON  o.free_breakfast == p.free_breakfast WHEN MATCHED THEN UPDATE SET p.price = o.price limit 100",
					  "idx4": "MERGE INTO keyspacenameplaceholder p USING [{'city': 'Hattieview','free_breakfast': true},{'city': 'Blakefort','free_breakfast': true},{'city': 'Chuview','free_breakfast': false},{'city': 'Vonfort','free_breakfast': true},{'city': 'Quitzonview','free_breakfast': false}] o ON  o.price == p.price WHEN MATCHED THEN UPDATE SET p.country = o.country limit 100",
					  "idx6": "MERGE INTO keyspacenameplaceholder p USING [{'city': 'Hattieview','free_breakfast': true},{'city': 'Blakefort','free_breakfast': true},{'city': 'Chuview','free_breakfast': false},{'city': 'Vonfort','free_breakfast': true},{'city': 'Quitzonview','free_breakfast': false}] o ON  o.city == p.city WHEN MATCHED THEN UPDATE SET p.country = o.country limit 100",
					  "idx7": "MERGE INTO keyspacenameplaceholder p USING [{'city': 'Hattieview','free_breakfast': true},{'city': 'Blakefort','free_breakfast': true},{'city': 'Chuview','free_breakfast': false},{'city': 'Vonfort','free_breakfast': true},{'city': 'Quitzonview','free_breakfast': false}] o ON  o.price == p.price WHEN MATCHED THEN UPDATE SET p.country = o.country limit 100"}]


def parse_options():
    parser = OptionParser()
    parser.add_option("-q", "--queries", dest="queries",
                      help="queries to be executed")

    parser.add_option("-s", "--server_ip", dest="server_ip",
                      help="server IP")

    parser.add_option("-u", "--username", dest="username",
                      help="Username for the database")

    parser.add_option("-z", "--password", dest="password",
                      help="Password for the database")

    parser.add_option("-p", "--port", dest="port",
                      help="query port")

    parser.add_option("-b", "--bucket", dest="bucket",
                      help="bucket name")

    parser.add_option("-l", "--log-level",
                      dest="loglevel", default="INFO", help="e.g -l debug,info,warning")

    parser.add_option("-n", "--querycount", dest="querycount", default="10",
                      help="Number queries to be run to be always running on the cluster. For n1ql this param should = number of threads.")

    parser.add_option("-d", "--duration", dest="duration",
                      help="Duration for which queries to be run. WARNING: A duration of 0 will set queries to run infinitely (if n1ql flag is set)")

    parser.add_option("-t", "--threads", dest="threads", default="10",
                      help="Number of queries that will be run to completion")

    parser.add_option("-f", "--query_file", dest="query_file", default=None,
                      help="A file containing the list of queries you wish to be run")

    parser.add_option("-v", "--validate", dest="validate", default=False,
                      help="Set if you want n1ql queries to be validated or not")

    parser.add_option("-Q", "--n1ql", dest="n1ql", default=False,
                      help="Set if App is for query use")

    parser.add_option("-T", "--query_timeout", dest="query_timeout", default=300,
                      help="How long each query should run for")

    parser.add_option("-S", "--scan_consistency", dest="scan_consistency", default="NOT_BOUNDED",
                      help="The Scan_consistency of each query")

    parser.add_option("-c", "--collections_mode", dest="collections_mode", default=False,
                      help="Run the script for collections and auto-discover queries to be run")

    parser.add_option("-j", "--run_udf_queries", dest="run_udf_queries", default=False,
                      help="Run the N1QL JS UDF queries")

    parser.add_option("-D", "--dataset", dest="dataset", default="hotel",
                      help="Dataset used in the test. Default = hotel")

    parser.add_option("-B", "--bucket_names", dest="bucket_names", default="[]",
                      help="The list of bucket_names in the test running")

    parser.add_option("-P", "--print_duration", dest="print_duration", default=3600,
                      help="The time interval you would like to wait before printing how many queries have been executed")

    parser.add_option("-X", "--txns", dest="txns", default=False,
                      help="The time interval you would like to wait before printing how many queries have been executed")

    parser.add_option("-a", "--analytics_mode", dest="analytics_mode", default=False,
                      help="Run the script for datasets and auto-discover queries to be run")

    parser.add_option("--analytics_queries", dest="analytics_queries", default="catapult_queries",
                      help="queries to be run on analytics")



    (options, args) = parser.parse_args()

    return options


class query_load(SDKClient):

    def __init__(self, server_ip, server_port, queries, bucket, querycount, username, password,batch_size=50):
        self.ip = server_ip
        self.port = server_port
        self.queries = queries
        self.bucket = bucket
        self.username = username
        self.password=password

        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.total_query_count = 0
        self.handles = []
        self.concurrent_batch_size = batch_size
        self.total_count = querycount
        self.transactions = 0
        self.transactions_committed = 0
        self.transactions_timedout = 0

        SDKClient(self.ip, "Administrator", "Password!")
        self.connectionLive = False

        self.createConn(self.bucket)

    def createConn(self, bucket):
        self.connectCluster()
        System.setProperty("com.couchbase.analyticsEnabled", "true");
        System.setProperty("com.couchbase.sentRequestQueueLimit", '1000');
        self.bucket = self.cluster.bucket(bucket);
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
                #log.error("%s" % e)
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
                #log.error("%s" % e)
                traceback.print_exception(*sys.exc_info())
            except RuntimeException as e:
                #log.info("RuntimeException from Java SDK. %s" % str(e))
                time.sleep(10)
                try:
                    self.bucket.close()
                    time.sleep(5)
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                #log.error("%s" % e)
                traceback.print_exception(*sys.exc_info())

    def _run_concurrent_queries(self, query, num_queries, duration=120, n1ql_system_test=False,
                                timeout=300, scan_consistency="NOT_BOUNDED", validate=False, analytics_timeout=300):
        # Run queries concurrently
        #log.info("Running queries concurrently now... Inside _run_concurrent_queries method")
        threads = []
        total_query_count = 0
        query_count = 0
        if n1ql_system_test:
            name = threading.currentThread().getName()
            thread_name = "query_for_{0}".format(name)
            print("query:{0}".format(query))
            threads.append(Thread(target=self._run_query,
                                  name=thread_name,
                                  args=(random.choice(query), False, 0, True, timeout, scan_consistency, validate,
                                        analytics_timeout)))
        else:
            for i in range(0, num_queries):
                total_query_count += 1
                threads.append(Thread(target=self._run_query,
                                      name="query_thread_{0}".format(total_query_count),
                                      args=(random.choice(query), False, 0, False, 300, "NOT_BOUNDED", False,
                                            analytics_timeout)))

        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                #log.info("submitted {0} queries".format(i))
                time.sleep(5)
            thread.start()
            self.total_query_count += 1
            query_count += 1

        # For n1ql apps we want all queries to finish executing before starting up new queries
        if n1ql_system_test:
            for thread in threads:
                thread.join()

        st_time = time.time()
        i = 0
        if duration == 0:
            while True:
                if n1ql_system_test:
                    #log.info("#" * 50)
                    self.total_count += 1
                    self._run_query(random.choice(query), False, 0, True, timeout, scan_consistency, validate,
                                    analytics_timeout)
                    i += 1
                    query_count += 1
                    self.total_query_count += 1
                else:
                    threads = []
                    #log.info("#" * 50)
                    #log.info("Total queries running on the cluster: %s" % self.total_count)
                    new_queries_to_run = num_queries - self.total_count
                    for i in range(0, new_queries_to_run):
                        total_query_count += 1
                        threads.append(Thread(target=self._run_query,
                                              name="query_thread_{0}".format(total_query_count),
                                              args=(random.choice(query), False, 0, False, 300, "NOT_BOUNDED", False,
                                                    analytics_timeout)))
                        #if total_query_count % 1000 == 0:
                            #log.warning(
                             #   "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                              #      total_query_count, self.failed_count, self.success_count, self.rejected_count,
                              #      self.cancel_count, self.timeout_count))
                    i = 0
                    self.total_count += new_queries_to_run
                    for thread in threads:
                        # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
                        i += 1
                        #if i % self.concurrent_batch_size == 0:
                        #    log.info("submitted {0} queries".format(i))
                        #                    time.sleep(5)
                        thread.start()

                    time.sleep(2)
        else:
            while st_time + duration > time.time():
                if n1ql_system_test:
                    #log.info("#" * 50)
                    self.total_count += 1
                    self._run_query(random.choice(query), False, 0, True, timeout, scan_consistency, validate,
                                    analytics_timeout)
                    i += 1
                    query_count += 1
                    self.total_query_count += 1
                else:
                    threads = []
                    #log.info("#" * 50)
                    #log.info("Total queries running on the cluster: %s" % self.total_count)
                    new_queries_to_run = num_queries - self.total_count
                    for i in range(0, new_queries_to_run):
                        total_query_count += 1
                        threads.append(Thread(target=self._run_query,
                                              name="query_thread_{0}".format(total_query_count),
                                              args=(random.choice(query), False, 0, False, 300, "NOT_BOUNDED", False,
                                                    analytics_timeout)))
                        #if total_query_count % 1000 == 0:
                            #log.warning(
                             #   "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                             #       total_query_count, self.failed_count, self.success_count, self.rejected_count,
                             #       self.cancel_count, self.timeout_count))
                    i = 0
                    self.total_count += new_queries_to_run
                    for thread in threads:
                        # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
                        i += 1
                        #if i % self.concurrent_batch_size == 0:
                           # log.info("submitted {0} queries".format(i))
                        thread.start()
                    time.sleep(2)
        #if n1ql_system_test:
            #log.info("%s queries submitted" % query_count)
        #else:
            #log.info(
            #    "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
            #        num_queries, self.failed_count, self.success_count, self.rejected_count, self.cancel_count,
            #        self.timeout_count))
        if self.failed_count + self.error_count != 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" % (self.failed_count, self.error_count))

    def _run_query(self, query, validate_item_count=False, expected_count=0, n1ql_execution=False,
                   timeout=300, scan_consistency="NOT_BOUNDED", validate=True, analytics_timeout=300):
        name = threading.currentThread().getName();
        client_context_id = name

        try:
            if n1ql_execution:
                if validate:
                    status, metrics, errors, results, handle = self.execute_statement_on_util(
                        query, timeout=timeout, client_context_id=client_context_id, thread_name=name, utility="n1ql",
                        scan_consistency=scan_consistency, analytics_timeout=analytics_timeout)
                    if status == "success":
                        split_query = query.split("WHERE")
                        primary_query = split_query[0] + "USE INDEX (`#primary`) WHERE" + split_query[1]
                        primary_status, primary_metrics, primary_errors, primary_results, primary_handle = self.execute_statement_on_util(
                            primary_query, timeout=timeout, client_context_id=client_context_id, thread_name=name,
                            utility="n1ql",
                            scan_consistency=scan_consistency, analytics_timeout=analytics_timeout)

                else:
                    print("Inside else of  validate of n1ql_execution ")
                    status, metrics, errors, results, handle = self.execute_statement_on_util(
                        query, timeout=timeout, client_context_id=client_context_id, thread_name=name,
                        utility="n1ql", scan_consistency=scan_consistency, analytics_timeout=analytics_timeout)
            else:
                # print("Inside else of  n1ql_execution ")

                status, metrics, errors, results, handle = self.execute_statement_on_util(
                    query, timeout=300, client_context_id=client_context_id, thread_name=name,
                    analytics_timeout=analytics_timeout)
            #log.info("query : {0}".format(query))
            # print ("Results are status: {} metrics: {} errors: {} results: {} handle: {}".format(status, metrics, errors, results, handle))
            # Validate if the status of the request is success, and if the count matches num_items
            #log.info("status:{0}".format(status))
            if status == "SUCCESS":
                if validate_item_count:
                    if results[0]['$1'] != expected_count:
                        #log.info("Query result : %s", results[0]['$1'])
                        #log.info(
                         #   "********Thread %s : failure**********",
                         #   name)
                        self.failed_count += 1
                        self.total_count -= 1
                    else:
                        #log.info(
                        #    "--------Thread %s : success----------",
                         #   name)
                        self.success_count += 1
                        self.total_count -= 1
                elif validate:
                    #log.info("primary_status:{0}".format(primary_status))
                    #log.info("metrics['resultCount']:{0}".format(metrics['resultCount']))
                    #log.info("results:{0}".format(results))
                    #log.info("primary_results:{0}".format(primary_results))

                    if primary_status == "success":
                        if metrics['resultCount'] != 0:
                            if results != primary_results:
                                #log.info("Query result : %s", results[0]['$1'])
                                #log.info(
                                #    "********Thread %s : failure**********",
                                #    name)
                                #print
                                #"Mismatch of results!"
                                #print("=" * 100)
                                #print
                                #"Query: %s" % query
                                #print
                                #"Primary Index Query: %s" % primary_query
                                #print("=" * 100)
                                log.info("failed count increament at  primary_status and metrics result count and results != primary_results")
                                self.failed_count += 1
                                self.total_count -= 1
                            else:
                                #log.info(
                                 #   "--------Thread %s : success----------",
                                 #   name)
                                self.success_count += 1
                                self.total_count -= 1
                        else:
                            #print("Results are zero! Please change query to have results!")
                            #print query
                            log.info("failed count increament at  primary_status and metrics result count==0 ")

                            self.failed_count += 1
                            self.total_count -= 1
                    else:
                        #print
                        #"Primary Index did not run properly, cannot vaildate results"
                        log.info("failed count increament at  primary_status !=success ")

                        self.failed_count += 1
                        self.total_count -= 1

                else:
                    #log.info("--------Thread %s : success----------",name)
                    self.success_count += 1
                    self.total_count -= 1
            else:
                #log.info("Status = %s", status)
                #log.warning("query : {0}".format(query))
                #log.warning("errors : {0}".format((str(errors))))
                #log.warning("********Thread %s : failure**********", name)
                log.info("failed count increament at status!=sucess")

                self.failed_count += 1
                self.total_count -= 1
        except Exception as e:
            #log.info("********EXCEPTION: Thread %s **********", name)
            if str(e) == "Request Rejected":
                #log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
                self.total_count -= 1
            elif str(e) == "Request TimeoutException":
                #log.info("Request TimeoutException")
                self.timeout_count += 1
                self.total_count -= 1
            elif str(e) == "Request RuntimeException":
                #log.info("Request RuntimeException")
                self.timeout_count += 1
                self.total_count -= 1
            elif str(e) == "Request RequestCancelledException":
                #log.info("Request RequestCancelledException")
                self.cancel_count += 1
                self.total_count -= 1
            elif str(e) == "CouchbaseException":
                #log.info("General CouchbaseException")
                self.rejected_count += 1
                self.total_count -= 1
            elif str(e) == "Capacity cannot meet job requirement":
                #log.info("Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
                self.total_count -= 1
            else:
                print("Exception we got:{0}".format(str(e)))
                self.error_count += 1
                self.total_count -= 1
                #log.info(str(e))

    def execute_statement_on_util(
            self, statement, timeout=300, client_context_id=None, username=None,
            password=None, analytics_timeout=300, thread_name=None, utility="cbas",
            scan_consistency="NOT_BOUNDED"):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        try:
            if utility == "n1ql":
                #log.info("Running query on n1ql via %s: %s" % (thread_name, statement))
                response = self.execute_statement_on_n1ql(
                    statement, pretty=True, client_context_id=client_context_id, username=username, password=password,
                    timeout=timeout, scan_consistency=scan_consistency)
            else:
                #log.info("Running query on cbas via %s: %s" % (thread_name, statement))
                response = self.execute_statement_on_cbas(statement, pretty, client_context_id,
                                                          username, password, timeout, analytics_timeout)

            # print("response:{0}".format(response))
            if type(response) == str:
                response = json.loads(response)
                # print("response:{0}".format(response))
            # print("checking for errors")
            if "errors" in response:
                print("Got errors in response ")
                errors = response["errors"]
                print("errors:{0}".format(errors))
                print("errors type:{0}".format(type(errors)))

                if type(errors) == str:
                    errors = json.loads(errors)
            else:
                errors = None

            if "results" in response:
                results = response["results"]
                print("results:{0}".format(results))
            else:
                results = None

            if "handle" in response:
                print("Trying to get handle")
                handle = response["handle"]
                print("handle:{0}".format(handle))

            else:
                handle = None

            if "metrics" in response:
                print("Trying to get handle")
                metrics = response["metrics"]
                print("metrics:{0}".format(metrics))
                print("metrics Type:{0}".format(type(metrics)))

                if type(metrics) == str:
                    metrics = json.loads(metrics)
            else:
                metrics = None
            return response["status"], metrics, errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def execute_statement_on_cbas(
            self, statement, pretty=True, client_context_id=None,
            username=None, password=None, timeout=300, analytics_timeout=300):

        analytics_options = AnalyticsOptions.analyticsOptions()
        analytics_options = analytics_options.raw("pretty", pretty)
        analytics_options = analytics_options.raw("timeout", str(analytics_timeout) + "s")
        analytics_options = analytics_options.raw("username", username)
        analytics_options = analytics_options.raw("password", password)
        analytics_options = analytics_options.raw("clientContextID", client_context_id)
        if client_context_id:
            analytics_options = analytics_options.clientContextId(client_context_id)

        output = {}
        try:
            result = self.cluster.analyticsQuery(statement, analytics_options)
            raise Exception("Need to implement Validations for analyticsQuery ")

            #output["status"] = result.status()
            #output["metrics"] = str(result.info().asJsonObject())

            #try:
                #output["results"] = str(result.allRows())
            #except:
                #output["results"] = None

            #output["errors"] = json.loads(str(result.errors()))

            #if str(output['status']) == "fatal":
                #msg = output['errors'][0]['msg']
                #if "Job requirement" in msg and "exceeds capacity" in msg:
                    #raise Exception("Capacity cannot meet job requirement")
            #elif str(output['status']) == "success":
                #output["errors"] = None
                #pass
            #else:
                #log.info("analytics query %s failed status:{0},content:{1}".format(output["status"], result))
                #raise Exception("Analytics Service API failed")

        except TimeoutException as e:
            #log.info("Request TimeoutException from Java SDK. %s" % str(e))
            #print("Request TimeoutException from Java SDK. %s" % str(e))
            #             traceback.print_exception(*sys.exc_info())
            raise Exception("Request TimeoutException")
        except RequestCancelledException as e:
            #log.info("RequestCancelledException from Java SDK. %s" % str(e))
            #print("RequestCancelledException from Java SDK. %s" % str(e))
            #             traceback.print_exception(*sys.exc_info())
            raise Exception("Request RequestCancelledException")
        except RejectedExecutionException as e:
            #log.info("Request RejectedExecutionException from Java SDK. %s" % str(e))
            #print("Request RejectedExecutionException from Java SDK. %s" % str(e))
            #             traceback.print_exception(*sys.exc_info())
            raise Exception("Request Rejected")
        except CouchbaseException as e:
            log.info("CouchbaseException from Java SDK. %s" % str(e))
            print("CouchbaseException from Java SDK. %s" % str(e))
            #             traceback.print_exception(*sys.exc_info())
            raise Exception("CouchbaseException")
        except RuntimeException as e:
            #log.info("RuntimeException from Java SDK. %s" % str(e))
            #print("RuntimeException from Java SDK. %s" % str(e))
            #             traceback.print_exception(*sys.exc_info())
            raise Exception("Request RuntimeException")
        return output

    def execute_statement_on_n1ql(self, statement, pretty=True, client_context_id=None,
                                  username=None, password=None, timeout=300, scan_consistency="NOT_BOUNDED"):
        n1ql_options = QueryOptions.queryOptions()
        n1ql_options = n1ql_options.raw("timeout", str(300) + "s")
        if scan_consistency == "REQUEST_PLUS":
            n1ql_options = n1ql_options.scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        elif scan_consistency == "STATEMENT_PLUS":
            n1ql_options = n1ql_options.scanConsistency(QueryScanConsistency.STATEMENT_PLUS);
        else:
            n1ql_options = n1ql_options.scanConsistency(QueryScanConsistency.NOT_BOUNDED);

        if client_context_id:
            n1ql_options = n1ql_options.clientContextId(client_context_id)

        n1ql_options = n1ql_options.timeout(Duration.ofSeconds(3600))

        output = {}
        try:
            print("Acquired all n1ql_options for execute_statement_on_n1ql. Executing query")
            print("Query Stmt:{0}".format(statement))

            result = self.cluster.query(statement,n1ql_options)
            print("Query execution completed. Printing out results and executing validations")
            output["status"] = result.metaData().status().name()
            print("status:{0}".format(output["status"]))

            try:
               output["metrics"] = str(result.metaData().metrics().get())
               print("metrics:{0}".format(output["metrics"]))
            except:
               output["metrics"] = None

            try:
                output["results"] = json.loads(str(result.rowsAsObject()))
                print("results:{0}".format(output["results"]))
            except:
                output["results"] = None


            if str(output['status']) == "FATAL":
               # TODO: Commenting below exception but need to figure out relevant exception in 3.3.*
                #msg = output['errors'][0]['msg']
                #if "Job requirement" in msg and "exceeds capacity" in msg:
                    #raise Exception("Capacity cannot meet job requirement")
                raise Exception("N1ql Service API failed")
            elif str(output['status']) == "SUCCESS":
                # print("success:{0}".format(output['status']))
                # #output["errors"] = None
                pass
            elif str(output['status']) == 'TIMEOUT':
                print("timeout:{0}".format(output['status']))
                raise Exception("Request TimeoutException")
            else:
                #log.info("n1ql query %s failed status:{0},content:{1}".format(
                #    output["status"], result))
                print("default:{0}".format(output['status']))
                raise Exception("N1ql Service API failed")

        except TimeoutException as e:
            print("Request TimeoutException from Java SDK. {0}")
            raise Exception("Request TimeoutException")
        except RequestCanceledException as e:
            print("RequestCancelledException from Java SDK. %s" % str(e))
            raise Exception("Request RequestCancelledException")
        except RejectedExecutionException as e:
            print("Request RejectedExecutionException from Java SDK. %s" % str(e))
            raise Exception("Request Rejected")
        except CouchbaseException as e:
            print("CouchbaseException from Java SDK. %s" % str(e))
            raise Exception("CouchbaseException")
        except RuntimeException as e:
            print("RuntimeException from Java SDK. %s" % str(e))
            raise Exception("Request RuntimeException")
        # print("Output from n1ql execution:{0}".format(output))
        return output

    def monitor_query_status(self, duration, print_duration=3600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while True:
                if st_time + print_duration < time.time():
                    print
                    "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                        self.total_query_count, self.failed_count, self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count)
                    st_time = time.time()
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    print
                    "%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
                        self.total_query_count, self.failed_count, self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count)
                    update_time = time.time()

    def generate_queries_for_collections(self, dataset, bucketname, txns=False, run_udf_queries=False):
        idx_query_templates = HOTEL_DS_IDX_QUERY_TEMPLATES
        if txns:
            idx_insert_templates = HOTEL_DS_IDX_QUERY_INSERT_TEMPLATES
            idx_delete_templates = HOTEL_DS_IDX_QUERY_DELETE_TEMPLATES
            idx_update_templates = HOTEL_DS_IDX_QUERY_UPDATE_TEMPLATES
            idx_merge_templates = HOTEL_DS_IDX_QUERY_MERGE_TEMPLATES
            keyspace_idx_map = {}
            txn_queries = {}
            tmp_merge = {}
            txn_queries['select'] = []
            txn_queries['insert'] = []
            txn_queries['delete'] = []
            txn_queries['merge'] = []
            txn_queries['update'] = []
            tmp_merge['idx1'] = []
            tmp_merge['idx2'] = []
            tmp_merge['idx3'] = []

        # This needs to be expanded when there are more datasets
        if dataset == "hotel":
            idx_query_templates = HOTEL_DS_IDX_QUERY_TEMPLATES

        for attempt in range(5):
            try:
                # Determine all scopes and collections for all buckets
                keyspaceListQuery = "select '`' || `namespace` || '`:`' || `bucket` || '`.`' || `scope` || '`.`' || `name` || '`' as `path` from system:all_keyspaces where `bucket` = '{0}';".format(
                    bucketname)
                queryResults = self.execute_statement_on_n1ql(keyspaceListQuery, True)
            except Exception as e:
                log.info("Query - {0} - failed. Exception : {1}, retrying.. queryResults={2}".format(keyspaceListQuery, str(e), str(queryResults)))
                time.sleep(120)
            else:
                break

        keyspaceList = []
        if 'results' in queryResults:
            for row in queryResults['results']:
                print("Each row:{0}".format(row))
                print("Each Path:{0}".format(row['path']))

                keyspaceList.append(row['path'])
        else:
            log.info("No query results for query : {0} : {1}".format(keyspaceListQuery, str(queryResults)))

        # For each collection, determine the indexes created
        queryList = []
        keyspace_idx_map = {}
        for keyspace in keyspaceList:
            keyspace_idx_map[keyspace] = []
            for attempt in range(5):
                try:
                    idxListQuery = "select `name` from system:all_indexes where `using`='gsi' and " \
                                   "'`' || `namespace_id` || '`:`' || `bucket_id` || '`.`' || `scope_id` || '`.`' || `keyspace_id` || '`' = '{0}' " \
                                   "order by `bucket_id`, `scope_id`, `keyspace_id`, name".format(keyspace)

                    queryResults = self.execute_statement_on_n1ql(idxListQuery, True)
                except Exception as e:
                    #log.info("Query - {0} - failed. Exception : {1}, retrying..".format(idxListQuery, str(e)))
                    time.sleep(120)
                else:
                    break

            # For each index, select the corresponding query from the index-query mapping template for the dataset.
            # Add the query to the query_list after replacing the keyspace name
            if len(queryResults['results']) > 0:
                print("queryResults['results'] for IDX:{0}".format(queryResults['results']))
                for row in queryResults['results']:
                    print("row for IDX:{0}".format(row))

                    # Idx name has a suffix that is a random string separated by an underscore.
                    # We need to extract the string before the _ to use it as a key for the idx-query map
                    idx_template_name = row["name"].split("_")[0]
                    print("idx_template_name:{0}".format(idx_template_name))
                    print("Generated queries before if txns {}".format(queryList))
                    if txns:
                        keyspace_idx_map[keyspace].append(idx_template_name)
                        try:
                            txn_queries['insert'].append(
                                idx_insert_templates[0][idx_template_name].replace("keyspacenameplaceholder", keyspace))
                        except Exception as e:
                            log.info("Issue with keyspace {0}".format(keyspace))
                            pass
                        try:
                            txn_queries['delete'].append(
                                idx_delete_templates[0][idx_template_name].replace("keyspacenameplaceholder", keyspace))
                        except Exception as e:
                            #log.info("Issue with keyspace {0}".format(keyspace))
                            pass
                        try:
                            txn_queries['update'].append(
                                idx_update_templates[0][idx_template_name].replace("keyspacenameplaceholder", keyspace))
                        except Exception as e:
                            #log.info("Issue with keyspace {0}".format(keyspace))
                            pass
                        try:
                            tmp_merge[idx_template_name].append(
                                idx_merge_templates[0][idx_template_name].replace("keyspacenameplaceholder", keyspace))
                        except Exception as e:
                            #log.info("Issue with keyspace {0}".format(keyspace))
                            pass
                    try:
                        print("Generated queries before if txns block are {}".format(queryList))
                        if txns:
                            txn_queries['select'].append(
                                idx_query_templates[0][idx_template_name].replace("keyspacenameplaceholder", keyspace))
                            if run_udf_queries:
                                print("Adding udf in if block")
                                txn_queries['select'].append("EXECUTE FUNCTION run_n1ql_query('{0}')".format(bucketname))

                        else:
                            queryList.append(
                                idx_query_templates[0][idx_template_name].replace("keyspacenameplaceholder",
                                                                                  keyspace))
                            if run_udf_queries:
                                print("Adding udf in else block")
                                queryList.append("EXECUTE FUNCTION run_n1ql_query('{0}')".format(bucketname))

                    except Exception as e:
                        #log.info("Issue with keyspace {0}".format(keyspace))
                        pass
            if txns:
                # Find the indexes in the merge query
                for idx in tmp_merge:
                    # Find the query itself in the templates
                    for merge_query in tmp_merge[idx]:
                        # Find the list of available keyspaces
                        for keyspace in keyspace_idx_map:
                            # make sure keyspace is not already in the query
                            if keyspace not in merge_query:
                                # Make sure the keyspace thats not in the query has the same index as the first keyspace
                                for index in keyspace_idx_map[keyspace]:
                                    # ensure no duplicate queries
                                    if (index == idx) and (
                                            merge_query.replace("secondholder", keyspace) not in txn_queries['merge']):
                                        txn_queries['merge'].append(merge_query.replace("secondholder", keyspace))
                queryList = txn_queries
        #log.info("=====  Query List (total {0} queries )  ===== ".format(len(queryList)))
        #if txns:
            #for querystmt in queryList:
                #log.info(queryList[querystmt])
        #else:
            #for querystmt in queryList:
                #log.info(querystmt)

        # Return query_list
        # print("Generated queries are {}".format(queryList))
        return queryList

    def generate_txns(self, txns):
        transactions = []
        for z in range(0, 10):
            i = 0
            rollback_exists = False
            savepoints = []
            txn_queries = []
            savepoint = 0
            test_batch = []
            rollback_point = 0
            transaction = []
            commit_found = False
            transaction.append("START TRANSACTION")
            random.seed(uuid.uuid4())
            # we want each txn to be between 10-20 queries long
            txn_size = random.randint(10, 20)
            for x in range(0, txn_size):
                random.seed(uuid.uuid4())
                # Select a random query type to append to the txn
                query_chance = random.randint(1, 100)
                if query_chance <= 50:
                    query_type = 'select'
                elif 50 < query_chance <= 70:
                    query_type = 'update'
                elif 70 < query_chance <= 80:
                    query_type = 'insert'
                elif 80 < query_chance <= 95:
                    query_type = 'delete'
                elif 95 < query_chance <= 100:
                    query_type = 'merge'
                query = random.choice(txns[query_type])
                transaction.append(query)
                # Determine if we want to insert a save point, ~ 5% of the time
                percentage2 = random.randint(1, 100)
                if percentage2 <= 5:
                    savepoint = i
                    transaction.append("SAVEPOINT s{0}".format(savepoint))
                    savepoints.append("s{0}".format(savepoint))
                    i = i + 1
                # We can only rollback to a savepoint if a savepoint exists
                if savepoints:
                    percentage3 = random.randint(1, 100)
                    if percentage3 <= 10:
                        # Pick a random savepoint that was created
                        rollback_point = random.randint(0, savepoint)
                        # If we want to rollback, then decide to rollback to a savepoint or generic rollback
                        percentage4 = random.randint(1, 100)
                        if percentage4 <= 70 and not rollback_exists:
                            transaction.append("ROLLBACK TRANSACTION TO SAVEPOINT s{0}".format(rollback_point))
                            rollback_exists = True
                        elif not rollback_exists:
                            transaction.append("ROLLBACK TRANSACTION")
                            rollback_exists = True

                percentage5 = random.randint(1, 100)
                # Commit is end of the transaction building, insert randomly  and then exit the txn
                if percentage5 <= 10:
                    transaction.append("COMMIT TRANSACTION")
                    break
            # We want transactions to be commited a large majority of the time, very rarely we want a txn to timeout instead of being committed
            for txn in transaction:
                if "COMMIT" in txn:
                    commit_found = True
            if not commit_found:
                percentage6 = random.randint(1, 100)
                if percentage6 <= 99:
                    transaction.append("COMMIT TRANSACTION")
            transactions.append(transaction)
        return transactions

    def run_txns(self, txns, ip, port, query_timeout):
        txid = ''
        headers = {'Content-Type': 'application/json'}
        auth = ('Administrator', 'password')
        query_endpoint = 'http://{0}:{1}/query/service'.format(ip, port)
        for txn in txns:
            self.transactions = self.transactions + 1
            random.seed(uuid.uuid4())
            # Randomize transaction timeout per transaction
            txtimeout = str(random.randint(900, 1200)) + "s"
            kvtimeout = str(random.randint(10, 20)) + "s"
            # Randomize scan_consistency per transaction
            scan_consistency = random.choice(['request_plus', 'not_bounded'])
            start_time = time.time()
            for query in txn:
                # Start transaction is special because it sets the settings of txn, plus we need to capture txn id to pass with subsequent queries
                if query == "START TRANSACTION":
                    data = '{{"statement":"{0}", "txtimeout":"{1}","scan_consistency":"{2}","Kvtimeout":"{3}"}}'.format(query,txtimeout,scan_consistency,kvtimeout)
                    try:
                        response = requests.post(query_endpoint, headers=headers, data=data, auth=auth)
                        results = response.json()
                        txid = results['results'][0]['txid']
                    except Exception as e:
                        print(
                            "txid does not exist, something went wrong with the transaction! query:{0}, response:{1}".format(
                                data, response.json()))

                # Every other query besides start, needs the txid passed to it and the query_timeout passed to it
                else:
                    if query == "COMMIT TRANSACTION":
                        end_time = time.time()
                        txduration = end_time - start_time
                        print("txns took {0}s to run".format(txduration))
                        self.transactions_committed = self.transactions_committed + 1
                    data = '{{"statement":"{0}", "txid":"{1}","timeout":"{2}s"}}'.format(query, txid, query_timeout)
                    try:
                        response = requests.post(query_endpoint, headers=headers, data=data, auth=auth)
                    except Exception as e:
                        print("Something went wrong query:{0}, response:{1}, error:{2}".format(data, response.json(),
                                                                                               str(e)))
        return

    def transaction_worker(self, queries, ip, port, query_timeout):
        txns = self.generate_txns(queries)
        self.run_txns(txns, ip, port, query_timeout)
        return

    def generate_queries_for_analytics(self, analytics_queries, bucket_list, timeout=300, analytics_timeout=300):

        def retry_execute_statement_on_cbas(ddl):
            retry = 0
            while retry < 20:
                try:
                    output = self.execute_statement_on_cbas(
                        statement, pretty=True, client_context_id=None,
                        username=None, password=None, timeout=timeout,
                        analytics_timeout=analytics_timeout)
                    return output
                except Exception as err:
                    print "Following error occured - {0}".format(str(err))
                    print "Sleeping 30 seconds before retrying"
                    retry += 1
                    time.sleep(30)
                    print "Retry attempt - {0}".format(retry)

        print "Generating queries for CBAS"
        query_templates = (importlib.import_module('cbas_queries').cbas_queries)[analytics_queries]

        statement = "select dv.DataverseName from Metadata.`Dataverse` as dv where dv.DataverseName != \"Metadata\";"
        output = retry_execute_statement_on_cbas(statement)
        if output["results"]:
            dataverses = json.loads(output["results"])
            dataverses = [dv["DataverseName"] for dv in dataverses]
            dataverses = json.dumps(dataverses, encoding="utf-8").replace("\'", "\"")

        bucket_list = json.dumps(bucket_list, encoding="utf-8").replace("\'", "\"")
        datasets = list()
        statement = "select ds.DataverseName, ds.DatasetName from Metadata.`Dataset` as ds " \
                    "where ds.DataverseName in {0} and ds.BucketName in {1};".format(dataverses, bucket_list)
        output = retry_execute_statement_on_cbas(statement)
        for dataset in json.loads(output["results"]):
            datasets.append([dataset["DataverseName"], dataset["DatasetName"]])

        # Find all synonyms created on synonyms
        statement = "select syn.ObjectName from Metadata.`Synonym` as syn where syn.ObjectName like \"synonym_%\""
        output = retry_execute_statement_on_cbas(statement)
        syn_on_syn = list()
        for synonym in json.loads(output["results"]):
            syn_on_syn.append(synonym["ObjectName"])
        syn_on_syn = json.dumps(syn_on_syn, encoding="utf-8").replace("\'", "\"")

        statement = "select syn.DataverseName, syn.SynonymName from Metadata.`Synonym` as syn " \
                    "where syn.ObjectDataverseName in {0} and syn.ObjectName in (select value ds.DatasetName from " \
                    "Metadata.`Dataset` as ds where ds.DataverseName in {0} and ds.BucketName in {1}) or syn.ObjectName " \
                    "in (select value syn1.SynonymName from Metadata.`Synonym` as syn1 where syn1.SynonymName in {2} and " \
                    "syn1.ObjectName in (select value ds1.DatasetName from Metadata.`Dataset` as ds1 where ds1.DataverseName " \
                    "in {0} and ds1.BucketName in {1}));".format(dataverses, bucket_list, syn_on_syn)
        output = retry_execute_statement_on_cbas(statement)
        for synonym in json.loads(output["results"]):
            datasets.append([synonym["DataverseName"], synonym["SynonymName"]])

        queries = list()
        while datasets:
            for query_template in query_templates:
                try:
                    dataset = datasets.pop()
                    queries.append(query_template.format(dataset[0], dataset[1]))
                except:
                    continue
        print "Finished generating CBAS queries"
        return queries


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
    log.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    log.addHandler(ch)
    timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))
    fh = logging.FileHandler("./querylogs-{0}.log".format(timestamp))
    fh.setFormatter(formatter)
    log.addHandler(fh)

log = logging.getLogger()
logging.getLogger("requests").setLevel(logging.WARNING)
options = None


def main():
    options = parse_options()
    setup_log(options)
    print("Options are {}".format(options))
    if options.n1ql:
        # print("numThreads:{0}".format(options.threads))
        load = query_load(options.server_ip, options.port, [], options.bucket, int(options.threads),
                           options.username, options.password,int(options.threads))
    else:
        # print("In else block options.n1ql")
        load = query_load(options.server_ip, options.port, [], options.bucket, options.username, options.password, int(options.querycount))

    bucket_list = options.bucket_names.strip('[]').split(',')

    if options.collections_mode:
        # print("In if block collections_mode")
        queries = load.generate_queries_for_collections(options.dataset, options.bucket, run_udf_queries=options.run_udf_queries)
        # print("From collections_mode:{0}".format(queries))
    # If we get txns we want to spawn the number of threads specified, each thread runs 10 txns
    elif options.txns:
        # print("In if block use txns")
        #print("use txns")
        queries = load.generate_queries_for_collections(options.dataset, options.bucket, txns=options.txns, run_udf_queries=options.run_udf_queries)
        # If duration is 0 run forever, else run for set amount of time
        if int(options.duration) == 0:
            while True:
                threads = []
                for i in range(0, load.concurrent_batch_size):
                    threads.append(Thread(target=load.transaction_worker,
                                          name="transaction_thread_{0}".format(i),
                                          args=(queries, options.server_ip, options.port, options.query_timeout)))
                    #print('creating transaction worker {0}'.format(str(i)))

                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()
                # Updates every thread count x 10 transactions ( for example if threads = 10, update every 100 txns)
                #print("{0} num_txns, {1} num_txns_committed".format(load.transactions, load.transactions_committed))

        else:
            # print("In else block")
            st_time = time.time()
            while st_time + int(options.duration) > time.time():
                threads = []
                for i in range(0, load.concurrent_batch_size):
                    threads.append(Thread(target=load.transaction_worker,
                                          name="query_thread_{0}".format(i),
                                          args=(queries, options.server_ip, options.port, options.query_timeout)))
                    #print('creating transaction worker {0}'.format(str(i)))

                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()
                # Updates every thread count x 10 transactions ( for example if threads = 10, update every 100 txns)
                #print("{0} num_txns, {1} num_txns_committed".format(load.transactions, load.transactions_committed))
    elif options.analytics_mode:
        # print("In analytics_mode if block")
        queries = load.generate_queries_for_analytics(options.analytics_queries, bucket_list,
                                                      timeout=options.query_timeout,
                                                      analytics_timeout=options.query_timeout)
    else:
        # print("In analytics_mode else block")
        if options.query_file:
            # print("In query_file block")
            f = open(options.query_file, 'r')
            queries = f.readlines()
            i = 0
            for query in queries:
                queries[i] = query.strip()
                for x in range(0, len(bucket_list)):
                    bucket_name = "bucket" + str(x)
                    if bucket_name in query:
                        queries[i] = query.replace(bucket_name, bucket_list[x])
                i += 1
            f.close()
        else:
            # print("In query_file else block")
            queries = [
                'SELECT name as id, result as bucketName, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds1 where duration between 3009 and 3010 and profile is not missing and array_length(profile.friends) > 5 limit 100',
                'SELECT name as id, result as bucketName, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds2 where duration between 3009 and 3010 and profile is not missing',
                'select sum(friends.num_friends) from (select array_length(profile.friends) as num_friends from ds3) as friends',
                'SELECT name as id, result as Result, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds4 where result = "SUCCESS" and profile is not missing and array_length(profile.friends) = 5 and duration between 3009 and 3010 UNION ALL SELECT name as id, result as Result, `type` as `Type`, array_length(profile.friends) as num_friends FROM  ds4 where result != "SUCCESS" and profile is not missing and array_length(profile.friends) = 5 and duration between 3010 and 3012']

    if options.txns:
        print("{0} num_txns, {1} num_txns_committed".format(load.transactions, load.transactions_committed))
        pass
    elif options.n1ql:
        threads = []
        print("In options.n1ql thread block to run concurrent queries")
        print("All the queries are {}".format(queries))
        for i in range(0, load.concurrent_batch_size):
            threads.append(Thread(target=load._run_concurrent_queries,
                                  name="query_thread_{0}".format(i),
                                  args=(queries, int(options.querycount), int(options.duration), options.n1ql,
                                        options.query_timeout, options.scan_consistency, options.validate)))

        threads.append(Thread(target=load.monitor_query_status, name="monitor_thread",
                              args=(int(options.duration), int(options.print_duration))))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
    else:
        print("In options.n1ql else thread block to run concurrent queries")
        load._run_concurrent_queries(queries, int(options.querycount), duration=int(options.duration),
                                     timeout=options.query_timeout, analytics_timeout=options.query_timeout)

    if not options.txns:
        print("%s queries submitted, %s failed, %s passed, %s rejected, %s cancelled, %s timeout" % (
            load.total_query_count, load.failed_count, load.success_count, load.rejected_count, load.cancel_count,
            load.timeout_count))
        print(load.total_count)
    print("Done!!")


'''
    /opt/jython/bin/jython -J-cp '../Couchbase-Java-Client-2.5.6/*' load_queries.py --server_ip 172.23.108.231 --port 8095 --duration 600 --bucket default --querycount 10
'''
if __name__ == "__main__":
    main()
