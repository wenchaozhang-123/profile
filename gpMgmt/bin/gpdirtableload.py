#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# gpdirtableload - load files(s) to directory table
# Copyright Hashdata 2024

import sys
import argparse

if sys.hexversion < 0x2040400:
    sys.stderr.write("gpdirtableload needs python 2.4.4 or higher\n")
    sys.exit(2)

import platform

try:
    import pg
except ImportError:
    try:
        from pygresql import pg
    except Exception as e:
        pass
except Exception as e:
    errorMsg = "gpload was unable to import The PyGreSQL Python module (pg.py) - %s\n" % str(e)
    sys.stderr.write(str(errorMsg))
    errorMsg = "Please check if you have the correct Visual Studio redistributable package installed.\n"
    sys.stderr.write(str(errorMsg))
    sys.exit(2)

import datetime, getpass, os, signal, socket, threading, time, traceback, re
from gppylib.commands.base import WorkerPool, Command, LOCAL

try:
    from gppylib.gpversion import GpVersion
except ImportError:
    sys.stderr.write("gpload can't import gpversion, will run in GPDB5 compatibility mode.\n")
    noGpVersion = True
else:
    noGpVersion = False

thePlatform = platform.system()
if thePlatform in ['Windows', 'Microsoft']:
    windowsPlatform = True
else:
    windowsPlatform = False

if windowsPlatform == False:
    import select

from sys import version_info

if version_info.major == 2:
    import __builtin__

    long = __builtin__.long
else:
    long = int

EXECNAME = 'gpdirtableload'

NUM_WARN_ROWS = 0
received_kill = False


def parseargs():
    parser = argparse.ArgumentParser(description='gpdirtableload --load file to directory table')

    parser.add_argument('--database', '-d', default="gpadmin",
                        help='Database to connect to(default gpadmin)')
    parser.add_argument('--dest-path', help='Path relative to the table root directory')

    parser.add_argument('--force-password-auth',
                        help='Force a password prompt (default false)')

    parser.add_argument('--host', '-h', default="localhost",
                        help='Host to connect to (default localhost)')
    parser.add_argument('--input-file', help='Input files or directory')

    parser.add_argument('--logfile', help='Log output to logfile (default none)')

    parser.add_argument('--port', '-p', type=int, default="5432",
                        help='Port to connect to (default 5432)')
    parser.add_argument('--stop-on-error', default=False,
                        help='Stop loading files when an error occurs (default false)')
    parser.add_argument('--table', '-t', help='Directory table to load to')
    parser.add_argument('--tag', help='Tag name')
    parser.add_argument('--tasks', '-T', type=int, default="1",
                        help='The maximum number of files that concurrently loads (default 1)')
    parser.add_argument('--user', '-U', default="gpadmin",
                        help='User to connect as (default gpadmin)')
    parser.add_argument('--verbose', '-V', action='store_true',
                        help='Indicates that the tool should generate verbose output (default false)')
    parser.add_argument('--version', '-v', action='version', version='gpdirtableload version 1.0.0\n',
                        help='Print version info and exit')

    # Parse the command line arguments
    args = parser.parse_args()
    return args, parser


def handle_kill(signum, frame):
    # already dying?
    global received_kill
    if received_kill:
        return

    received_kill = True

    g.log(g.INFO, "received signal %d" % signum)
    g.exitValue = 2
    sys.exit(2)


def splitPgpassLine(a):
    """
    If the user has specified a .pgpass file, we'll have to parse it. We simply
    split the string into arrays at :. We could just use a native python
    function but we need to escape the ':' character.
    """
    b = []
    escape = False
    d = ''
    for c in a:
        if not escape and c == '\\':
            escape = True
        elif not escape and c == ':':
            b.append(d)
            d = ''
        else:
            d += c
            escape = False
    if escape:
        d += '\\'
    b.append(d)
    return b


class options:
    pass


class gpdirtableload:
    """
    Main class wrapper
    """

    def __init__(self, argv):
        self.threads = []  # remember threads so that we can join() against them
        self.exitValue = 0
        self.dbs = []
        self.options = options()
        self.options.database = 'gpadmin'
        self.options.dest_path = None
        self.options.force_password_auth = False
        self.options.host = None
        self.options.input_file = None
        self.options.logfile = None
        self.options.port = 5432
        self.options.password = None
        self.options.max_retries = 3
        self.options.stop_on_error = False
        self.options.table = None
        self.options.tag = None
        self.options.tasks = 1
        self.options.user = None
        self.options.verbose = False
        self.DEBUG = 5
        self.LOG = 4
        self.INFO = 3
        self.WARN = 2
        self.ERROR = 1
        self.options.qv = self.INFO
        self.startTimestamp = time.time()
        self.pool = None

        args, parser = parseargs()

        self.options.database = args.database
        self.options.dest_path = args.dest_path
        self.options.force_password_auth = args.force_password_auth
        self.options.host = args.host
        self.options.input_file = args.input_file
        self.options.logfile = args.logfile
        self.options.port = args.port
        self.options.stop_on_error = args.stop_on_error
        self.options.table = args.table
        self.options.tag = args.tag
        self.options.tasks = args.tasks
        self.options.user = args.user
        self.options.verbose = args.verbose

        # set default log level
        if self.options.verbose is not None:
            self.options.qv = self.DEBUG
        else:
            self.options.qv = self.INFO

        # default to gpAdminLogs for a log file, may be overwritten
        if self.options.logfile is None:
            self.options.logfile = os.path.join(os.environ.get('HOME', '.'), 'gpAdminLogs')
            if not os.path.isdir(self.options.logfile):
                os.mkdir(self.options.logfile)

            self.options.logfile = os.path.join(self.options.logfile, 'gpdirtableload_' + \
                                                datetime.date.today().strftime('%Y%m%d') + '.log')

        try:
            self.logfile = open(self.options.logfile, 'a')
        except Exception as e:
            self.log(self.ERROR, "could not open logfile %s: %s" %
                     (self.options.logfile, e))

        self.log(self.INFO, 'gpdirtableload session started ' + \
                 datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def elevel2str(self, level):
        if level == self.DEBUG:
            return "DEBUG"
        elif level == self.LOG:
            return "LOG"
        elif level == self.INFO:
            return "INFO"
        elif level == self.ERROR:
            return "ERROR"
        elif level == self.WARN:
            return "WARN"
        else:
            self.log(self.ERROR, "unknown log type %i" % level)

    def log(self, level, a):
        """
        Level is either DEBUG, LOG, INFO, ERROR. a is the message
        """
        log = ''
        try:
            log = '|'.join(
                [datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                 self.elevel2str(level), a]) + '\n'

        except Exception as e:
            # log even if contains non-utf8 data and pass this exception
            self.logfile.write("\nWarning: Log() threw an exception: %s \n" % (e))

        if level <= self.options.qv:
            sys.stdout.write(log)

        if level <= self.options.qv or level <= self.INFO:
            try:
                self.logfile.write(log)
                self.logfile.flush()
            except AttributeError as e:
                pass

        if level == self.ERROR:
            self.exitValue = 2;
            sys.exit(self.exitValue)

    def readPgpass(self, pgpassname):
        """
        Get password form .pgpass file
        """
        try:
            f = open(pgpassname, 'r')
        except IOError:
            return
        for row in f:
            try:
                row = row.rstrip("\n")
                line = splitPgpassLine(row)
                if line[0] != '*' and line[0].lower() != self.options.h.lower():
                    continue
                if line[1] != '*' and int(line[1]) != self.options.p:
                    continue
                if line[2] != '*' and line[2] != self.options.d:
                    continue
                if line[3] != '*' and line[3] != self.options.U:
                    continue
                self.options.password = line[4]
                break
            except (ValueError, IndexError):
                pass
        f.close()

    def setup_connection(self, recurse=0):
        """
        Connect to the backend
        """
        if self.db != None:
            self.db.close()
            self.db = None
        if self.options.force_password_auth:
            if self.options.password == None:
                self.options.password = getpass.getpass()
        else:
            if self.options.password == None:
                self.options.password = os.environ.get('PGPASSWORD')
            if self.options.password == None:
                self.readPgpass(os.environ.get('PGPASSFILE',
                                               os.environ.get('HOME', '.') + '/.pgpass'))
        try:
            self.log(self.DEBUG, "connection string:" +
                     " user=" + str(self.options.user) +
                     " host=" + str(self.options.host) +
                     " port=" + str(self.options.port) +
                     " database=" + str(self.options.database))
            self.db = pg.DB(dbname=self.options.database
                            , host=self.options.host
                            , port=self.options.port
                            , user=self.options.user
                            , passwd=self.options.password)
            self.log(self.DEBUG, "Successfully connected to database")

            if noGpVersion == False:
                # Get GPDB version
                curs = self.db.query("SELECT version()")
                self.gpdb_version = GpVersion(curs.getresult()[0][0])
                self.log(self.DEBUG, "GPDB version is: %s" % self.gpdb_version)

        except Exception as e:
            errorMessage = str(e)
            if errorMessage.find("no password supplied") != -1:
                self.options.password = getpass.getpass()
                recurse += 1
                if recurse > 10:
                    self.log(self.ERROR, "too many login attempt failures")
                self.setup_connection(recurse)
            elif errorMessage.find("Connection timed out") != -1 and self.options.max_retries != 0:
                recurse += 1
                if self.options.max_retries > 0:
                    if recurse > self.options.max_retries:  # retry failed
                        self.log(self.ERROR, "could not connect to database after retry %d times, " \
                                             "error message:\n %s" % (recurse - 1, errorMessage))
                    else:
                        self.log(self.INFO, "retry to connect to database, %d of %d times" % (recurse,
                                                                                              self.options.max_retries))
                else:  # max_retries < 0, retry forever
                    self.log(self.INFO, "retry to connect to database.")
                self.setup_connection(recurse)
            else:
                self.log(self.ERROR, "could not connect to database: %s. Is " \
                                     "the Cloudberry Database running on port %i?" % (errorMessage,
                                                                                      self.options.port))

    def isDirectoryMode(self):
        if self.options.input_file != None and not os.path.exists(self.options.input_file):
            self.log(self.ERROR, "File or directory %s does not exist." % self.options.input_file)
        if self.options.input_file != None and os.path.isdir(self.options.input_file):
            return True
        return False

    def collectAllFiles(self):
        self.allFiles = []
        self.numFiles = 0

        if self.isDirectoryMode():
            for root, dirs, files in os.walk(self.options.input_file):
                for file in files:
                    dirpath = os.path.abspath(root)
                    filepath = os.path.join(dirpath, file)
                    self.allFiles.append(filepath)
                    self.numFiles += 1
        else:
            if self.options.input_file is not None and os.path.exists(self.options.input_file):
                filepath = os.path.abspath(self.options.input_file)
                self.allFiles.append(filepath)
                self.numFiles = 1

    def confirmWorkers(self):
        if self.numFiles < self.options.tasks:
            self.numWorkers = self.numFiles
        else:
            self.numWorkers = self.options.tasks

    def startLoadFiles(self):
        """
        startLoadFiles
        """
        self.pool = WorkerPool(numWorkers=self.numWorkers, should_stop=self.options.stop_on_error)

        srcfile = None
        if os.environ.get('GPHOME_LOADERS'):
            srcfile = os.path.join(os.environ.get('GPHOME_LOADERS'),
                                   'greenplum_loaders_path.sh')
        elif os.environ.get('GPHOME'):
            srcfile = os.path.join(os.environ.get('GPHOME'),
                                   'greenplum_path.sh')
        if (not (srcfile and os.path.exists(srcfile))):
            self.log(self.ERROR, 'cannot find greenplum environment ' +
                     'file: environment misconfigured')

        cmdstrbase = "source %s ; psql "

        if self.options.database != None:
            cmdstrbase += "-d %s " % self.options.database
        if self.options.host != None:
            cmdstrbase += "-h %s " % self.options.host
        if self.options.port != 0:
            cmdstrbase += "-p %d " % self.options.port
        if self.options.user != None:
            cmdstrbase += "-U %s " % self.options.user
        if self.options.password != None:
            cmdstrbase += "-P %s " % self.options.password

        try:
            for file in self.allFiles:
                cmdstr = cmdstrbase
                cmdstr += '-c \"copy binary %s from \'%s\' ' % (self.options.table, file)
                if self.isDirectoryMode():
                    cmdstr += '\'%s/%s\' ' % (self.options.dest_path, os.path.relpath(file))
                else:
                    cmdstr += '\'%s\' ' % self.options.dest_path

                if self.options.tag is not None:
                    cmdstr += 'with tag %s \"' % self.options.tag
                else:
                    cmdstr += '\"'

                cmd = Command(name='load directory table', ctxt=LOCAL, cmdStr=cmdstr)
                self.pool.addCommand(cmd)
            self.pool.join()
            items = self.pool.getCompletedItems()
            for i in items:
                if not i.was_successful():
                    self.log(self.ERROR, 'failed load file to directory table %s, msg:%s' %
                             (self.options.table, i.get_results().stderr))
            self.pool.check_results()
        except Exception as err:
            self.log(self.ERROR, 'errors in job:')
            self.log(self.ERROR, err.__str__())
            self.log(self.ERROR, 'exiting early')
        finally:
            self.pool.haltWork()
            self.pool.joinWorkers()

    def run2(self):
        try:
            start = time.time()
            self.collectAllFiles()
            self.confirmWorkers()
            self.setup_connection()
            self.startLoadFiles()
            self.log(self.INFO, 'running time: %.2f seconds' % (time.time() - start))
        except Exception as e:
            raise

    def run(self):
        self.db = None
        signal.signal(signal.SIGINT, handle_kill)
        signal.signal(signal.SIGTERM, handle_kill)
        # win32 doesn't do SIGQUIT
        if not platform.system() in ['Windows', 'Microsoft']:
            signal.signal(signal.SIGQUIT, handle_kill)
            signal.signal(signal.SIGHUP, signal.SIG_IGN)

        try:
            try:
                self.run2()
            except Exception:
                traceback.print_exc(file=self.logfile)
                self.logfile.flush()
                self.exitValue = 2
                if (self.options.qv > self.INFO):
                    traceback.print_exc()
                else:
                    self.log(self.ERROR, "unexpected error -- backtrace " +
                             "written to log file")
        finally:
            if self.exitValue == 0:
                self.log(self.INFO, 'gpdirtableload succeeded')
            elif self.exitValue == 1:
                self.log(self.INFO, 'gpdirtableload succeeded with warnings')
            else:
                self.log(self.INFO, 'gpdirtableload failed')


if __name__ == '__main__':
    g = gpdirtableload(sys.argv[1:])
    g.run()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(g.exitValue)
