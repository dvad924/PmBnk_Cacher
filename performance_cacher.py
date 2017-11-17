import psycopg2, time, urllib, urllib2, json, datetime, calendar, threading, sys
import argparse
from timelogger import TimeLogger
from connection_data import aresConnectionData
from auth_login import login
types = ['phed','lottr','tttr']
parser = argparse.ArgumentParser(description='generates bottleneck metric from the data')
parser.add_argument('state', type=str, help='Enter the state that the calculation should be run with')
parser.add_argument('--clear', dest='clear', help='Set flag to clear the database')
parser.add_argument('--dates', type=str, dest='dates', help='''Simple json map
to set year and month limits e.g. {2016:12}
means the year of 2016 with all 12 months
''')
args = parser.parse_args()
print (args)

API_HOST = "https://staging.npmrds.availabs.org/api/"
auth_header = {}
last_refresh = 0
TOKEN_REFRESH_THRESH = 250
def check_auth():
    if 'Authorization' in auth_header and ((time.time() - last_refresh) < TOKEN_REFRESH_THRESH):
        return
    else:
        print(time.time() - last_refresh)
        set_header()
def set_header():
    print ('Aquiring Auth Token')
    params = urllib.urlencode({'email':login['email'], 'password':login['password']})
    request = urllib2.Request('https://aauth.availabs.org/login/auth', params)
    token = ''
    try :
        resp = urllib2.urlopen(request)
        j = json.loads(resp.read())
        print (j)
        token = str(j['token'])
        print (token)
    except urllib2.URLError as e :
        print ('Error Fetching Url', request)
        raise e

    auth_header['Authorization'] = "Bearer "+ token
    global last_refresh
    last_refresh = time.time()

years = [2017, 2016]
year_max = {2017:8, 2016:12}
def parse_dates(dates) :
    if not dates:
        return
    jdata = json.loads(dates)
    global years
    global year_max

    years = map(int,jdata.keys())
    for year in years:
        year_max[year] = jdata[str(year)]


MAX_THREADS = 1
TMC_LIMIT = 100
tmcatts = {}
def queryTmcLengths(connection):
    states = {"ny":'New York'}
    result = None
    with connection.cursor() as cursor:
        sql = """ SELECT tmc, miles FROM tmc_attributes WHERE state='{}'"""
        cursor.execute(sql.format(states[args.state]))
        for row in cursor:
            tmcatts[row[0]] = float(row[1]);
    connection.commit()

def queryTmcs(connection):
    #return ('120P05865', '120P05864', '120P05863', '120P05862', '120P05861')
    result = None
    with connection.cursor() as cursor:
        sql = """
            SELECT DISTINCT tmc
            FROM tmc_attributes
            WHERE tmc NOT IN (SELECT DISTINCT tmc FROM {}.pm_bottlenecks )
            AND state = '{}'
        """
        cursor.execute(sql.format(args.state, args.state))
        #print(cursor.query)
        result = tuple([row[0] for row in cursor])
    connection.commit()
    return result

def getConnection(connectionData):
    connection = None

    try:
        connection = psycopg2.connect(**connectionData)
    except Exception as e:
        print ("Could not establish a connection with {}." \
            .format(connectionData["database"]), e.message)
        raise e

    if (connection):
        print ("Connection establised with {}" \
            .format(connectionData["database"]))

    return connection

class TmcThreader(threading.Thread):

    tmcList = []
    listLock = threading.Lock()
    # measure, state, tmcs, year, month
    BASE_URL = API_HOST + 'hppm/{}/{}/tmcs/{}/{}/{}'

    def __init__(self, num, connection):
        threading.Thread.__init__(self)
        self.id = "thread-{}".format(num)
        self.logger = TimeLogger(self.id)
        self.connection = connection
        self.tmcs = []
        self.lottr = {}
        self.tttr = {}
        self.phed = {}

    @staticmethod
    def initThreader(tmcList):
        TmcThreader.tmcList = tmcList

    def __del__(self):
        self.logger.log("Exiting thread")

    def run(self):
        self.logger.log("Starting thread")

        while True:
            self.lottr = {}
            self.tttr = {}
            self.phed = {}

            with TmcThreader.listLock:
                if len(TmcThreader.tmcList):
                    length = len(TmcThreader.tmcList)
                    self.tmcs = TmcThreader.tmcList[max(0, length - TMC_LIMIT) :]
                    TmcThreader.tmcList = TmcThreader.tmcList[0 : max(0, length - TMC_LIMIT)]
                else:
                    break
            self.logger.log("Processing {} TMCs".format(len(self.tmcs)))
            self.processTmcs()

    def processTmcs(self):
        months = range(0,13)
        for year in years:
            for month in months:
                if month > year_max[year]:
                    continue

                self.logger.start("LOTTR")
                self.buildLOTTR(year,month)
                self.logger.end("LOTTR")

                self.logger.start("TTTR")
                self.buildTTTR(year, month)
                self.logger.end("TTTR")

                self.logger.start("PHED")
                self.buildPHED(year, month)
                self.logger.end("PHED")

                self.logger.start("INSERTS")
                self.insertConstructs(year, month)
                self.logger.end("INSERTS")

    def buildLOTTR(self, year, month):
        url = TmcThreader.BASE_URL.format("lottr", args.state, ''.join(self.tmcs), year, month)
        data = self.requestData(url)
        for tmc in self.tmcs:
            score = 0
            if data[args.state]!=None and tmc in data[args.state]['lottr_data_by_tmc']:
                d = data[args.state]['lottr_data_by_tmc'][tmc]['by_lottr_time_period']
                keys = ['AM_PEAK','MIDDAY','PM_PEAK', 'WEEKEND']
                try:
                    scores = map(lambda x: d[x]['lottr'] if 'lottr' in d[x] else 0, keys)
                    score = max(scores) or 0
                except:
                    self.logger.log(d);
            else:
                self.logger.log('No lottr Data Found for : {} in {} {}'.format(tmc, year, month))

            self.lottr[tmc] = score

    def buildTTTR(self, year, month):
        url = TmcThreader.BASE_URL.format("tttr", args.state, ''.join(self.tmcs), year, month)
        data =self.requestData(url)
        for tmc in self.tmcs:
            score = 0
            if data[args.state]!=None and tmc in data[args.state]['tttr_data_by_tmc']:
                d = data[args.state]['tttr_data_by_tmc'][tmc]['by_tttr_time_period']
                keys = ['AM_PEAK','MIDDAY','PM_PEAK', 'WEEKEND', 'OVERNIGHT']
                try:
                    scores = map(lambda x: d[x]['tttr'] if 'tttr' in d[x] else 0, keys)
                    score = max(scores) or 0
                except:
                    self.logger.log(d)
            else:
                self.logger.log('No tttr data found for : {} in {} {}'.format(tmc, year, month))
            self.tttr[tmc] = score

    def buildPHED(self, year, month):
        url = TmcThreader.BASE_URL.format("phed", args.state, ''.join(self.tmcs), year, month)
        data = self.requestData(url)
        for tmc in self.tmcs:
            score = 0
            if data[args.state]!=None and tmc in data[args.state]['phed_data_by_tmc']:
                d = data[args.state]['phed_data_by_tmc'][tmc]
                if 'PHED_AM_PEAK' in d['by_phed_time_period']:
                    score += float(d['by_phed_time_period']['PHED_AM_PEAK']['phed'])
                if 'PHED_PM_PEAK_1' in d['by_phed_time_period'] and 'PHED_PM_PEAK_2' in d['by_phed_time_period']:
                    score += float(max(d['by_phed_time_period']['PHED_PM_PEAK_1']['phed'], d['by_phed_time_period']['PHED_PM_PEAK_2']['phed']))
                elif 'PHED_PM_PEAK_1' in d['by_phed_time_period']:
                    score += float(d['by_phed_time_period']['PHED_PM_PEAK_1']['phed'])
                elif 'PHED_PM_PEAK_2' in d['by_phed_time_period']:
                    score += float(d['by_phed_time_period']['PHED_PM_PEAK_2']['phed'])
            else:
                self.logger.log('No PHed data found for {} in {} {}'.format(tmc, year, month))

            self.phed[tmc] = (score or 0) / tmcatts[tmc]

    def insertConstructs(self, year, month):
        self.logger.log("PHoney inserts")
        data = [(tmc, year, month, self.phed[tmc], self.tttr[tmc], self.lottr[tmc]) for tmc in self.tmcs]
        with self.connection.cursor() as cursor:
            d = "(%s, %s, %s, %s, %s, %s)"
            sql = """
            INSERT INTO {}.pm_bottlenecks
            VALUES {}
            """.format(args.state, ",".join(map(lambda x: cursor.mogrify(d, x), data)))
            cursor.execute(sql)
            self.connection.commit()


    def requestData(self, url):
        check_auth()
        request = urllib2.Request(url, headers=auth_header)
        data = None
        try:
            response = urllib2.urlopen(request)
            data = json.loads(response.read())
        except:
            self.logger.log("ERROR - requesting data from {}".format(url))

        return data



def del_table(connection):
    with connection.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS {}.pm_bottlenecks;
        """
        cursor.execute(sql.format(args.state))
    connection.commit()
    return
def init_table(connection):
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS {}.pm_bottlenecks (
          tmc character varying(9) NOT NULL,
          year smallint NOT NULL,
          month smallint NOT NULL,
          phed real NOT NULL,
          tttr real NOT NULL,
          lottr real NOT NULL
        );
        """
        cursor.execute(sql.format(args.state))
    connection.commit()


def main():
    logger = TimeLogger().start("Running time")

    with getConnection(aresConnectionData) as connection:
        if args.clear and args.state:
            del_table(connection)
            return
        elif args.clear:
            print ("need table space to delete from")
            return

        init_table(connection)

        threaders = [TmcThreader(d, connection) for d in range(MAX_THREADS)]
        queryTmcLengths(connection)
        TmcThreader.initThreader(queryTmcs(connection))

        for threader in threaders:
            threader.start()
        for threader in threaders:
            threader.join()

    logger.log("=========")
    logger.end("Running time")


if __name__ == "__main__":
    main()
    print('FINISHED')
