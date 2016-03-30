import sqlite3 as lite
import os, threading, queue, time, shutil, re, codecs, configparser, sys


#################################################################
# Global variable declarations
#################################################################

accepted_filetypes = ["mkv", "mp4", "avi"]
readyFlag = 0
init = False
parser = configparser.ConfigParser()

# Directories
destinations = {}

# Global variables used for multithreading
currentFileSize = 0
lastIt = time.time()
allowprint = True
exitFlag = 0
queueLock = threading.Lock()
writeLock = threading.Lock()

dbLock = threading.Lock()

workQueue = queue.Queue()
interval = 1800
active = False

# Database and logg variables
exists = os.path.exists("files.db")
con = lite.connect("files.db")


#################################################################
# Setup functions / general functions
#################################################################

# Class for thread that moves files
class MoveHandler(threading.Thread):
    def __init__(self, thID, name, q):
        threading.Thread.__init__(self)
        self.thID = thID
        self.name = name
        self.q = q

    def run(self):
        copyFiles(self.name, self.q)


# Create database file and table
def setup_db():

    with con:
        cur = con.cursor()

        cur.execute(
            "CREATE TABLE File(" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "filename TEXT, " +
                "path TEXT, " +
                "lastSize INTEGER, " +
                "done INTEGER DEFAULT 0, " +
                "copied INTEGER DEFAULT 0)"
        )


# Read config, creates new config if it doesn't exist
def readConfig():
    global destinations, interval

    requiredSections = ["Paths", "Misc"]
    requiredFields = ["tv", "movie", "manual", "base"]

    if not os.path.exists("config.ini"):
        with open("config.ini", "w") as conf:
            conf.write("[Paths]\nTV = ./TV\nMovie = ./Movies\nManual = ./Manual\nbase = ./\n\n"
                       + "[Misc]\nUnit = minutes\nInterval = 30")
        print("No config was found, so a new one was created. Please edit and restart")
        exit(1)
    else:
        parser.read("config.ini")

        for section in requiredSections:
            if section not in parser.keys():
                print("Error: Missing section %s in config" % section)
                exit(1)

        for key in requiredFields:
            if key not in parser['Paths']:
                print("Error: missing parameter %s in config" % key)
                exit(1)

        # for val in parser['Paths']:
        destinations = parser['Paths']

        if "interval" in parser['Misc']:
            factor = 60
            if "unit" in parser['Misc']:
                unit = parser['Misc']['Unit']
                if unit == "seconds":
                    factor = 1
                elif unit == "minutes":
                    factor = 60
                elif unit == "hours":
                    factor = 3600
                else:
                    print("Unknown unit %s. Using standard: minutes" % unit)
            else:
                print("Unit not set. Using standard: minutes")

            interval = int(parser['Misc']['Interval']) * factor
            print("Interval = %i" % interval)
        else:
            print("Interval not set. Using standard: 30min")


# Write an entry to the log, aquire writeLock first to be safe.
def write_logg(entry, entryType):

    writeLock.acquire()

    logg = codecs.open("logg.txt", "a", "utf-8")

    time_of_entry = time.asctime(time.localtime(time.time()))
    entry = "%s: [%s] %s\n" % (time_of_entry, entryType, entry)

    with logg:
        logg.write(entry)

    writeLock.release()


#################################################################
# Functions for tracking files
#################################################################

# Check if the file is a desired media file by size and filetype
def ismediafile(filepath):
    filename = filepath.split(os.path.sep)[-1]
    substrings = filename.split('.')
    ftype = substrings[len(substrings) - 1]
    size = os.path.getsize(filepath)

    size /= 1000000
    if ftype in accepted_filetypes and size > 150:
        return True
    else:
        return False


# Retrieve all files in the database
def getLoggedFiles():
    files = {}
    with con:
        cur = con.cursor()

        cur.execute("SELECT filename, lastSize, done FROM File")

        result = cur.fetchall()

        for filename, lastSize, done in result:
            files[filename] = [lastSize, done]

    return files


# Read directory for all files
def readFolder(baseDir):

    fileList = []

    for dirpath, dirnames, files in os.walk(baseDir):
        for f in files:
            fp = os.path.join(dirpath, f)

            if(ismediafile(fp)):
                fileList.append(fp)

    return fileList


# Add files that dos not already exist in the db
def checkFiles():
    global readyFlag, init, con
    con = lite.connect("files.db")
    fileList = readFolder(destinations['base'])
    loggedFiles = getLoggedFiles()

    terout("\n%s: Starting new run" % time.asctime(time.localtime(time.time())))

    insert = False
    update = False

    with con:
        cur = con.cursor()

        if init:
            insQuery = "INSERT INTO File (filename, path, lastSize, done, copied) VALUES "
        else:
            insQuery = "INSERT INTO File (filename, path, lastSize) VALUES "

        updateQueries = []
        insQueries = []
        it = 1

        for currentFile in fileList:
            fileSize = os.path.getsize(currentFile)
            fileName = currentFile.split(os.path.sep)[-1]

            insName = fileName.replace("\'", "\'\'")
            insPath = currentFile.replace("\'", "\'\'")

            if fileName not in loggedFiles.keys():

                if init:
                    insQuery += "('%s', '%s', %i, 1, 1), " % (insName, insPath, fileSize)
                else:
                    insQuery += "('%s', '%s', %i), " % (insName, insPath, fileSize)

                if it > 100:
                    insQuery = insQuery.rstrip(", ")
                    insQueries.append(insQuery)

                    if init:
                        insQuery = "INSERT INTO File (filename, path, lastSize, done, copied) VALUES "
                    else:
                        insQuery = "INSERT INTO File (filename, path, lastSize) VALUES "

                insert = True
            else:
                lastSize = loggedFiles[fileName][0]
                done = loggedFiles[fileName][1]

                if fileSize == lastSize and not done:
                    updateQueries.append("UPDATE File SET done=1 WHERE filename='%s'; " % insName)
                    readyFlag = 1
                    update = True
                elif fileSize != lastSize and not done:
                    updateQueries.append("UPDATE File SET lastSize=%i WHERE filename='%s'" % (fileSize, insName))
                    update = True

            it += 1

        if insert:
            if not insQuery.endswith("VALUES "):
                insQuery = insQuery.rstrip(", ")
                insQueries.append(insQuery)

            for query in insQueries:
                cur.execute(query)

            terout("Found new files")
        else:
            terout("No new files this time...")

        if update:
            for updateQuery in updateQueries:
                cur.execute(updateQuery)
            terout("Updating some data on the files")

    if readyFlag:
        terout("Ready flag up - initiating file transfers")
        handleItems()
        readyFlag = 0

    if init:
        init = False
        terout("Initial run: ignoring all files")
        terout("Reading folder: %s" % destinations['base'])


#################################################################
# Functions for copying files
#################################################################


def copyfileobj(fsrc, fdst, length=10*1024*1024):
    copied = 0
    while True:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)
        copied += len(buf)
        displayProgress(copied)


# Returns the target destinations based on regex and size matching
def determineDestination(filedata):
    size = filedata[2]
    fileName = filedata[1]
    size /= 1000000

    matchSeries = re.search(r'(?ix) (s|season) \d{1,2} (e|ep|x|episode) \s* \d{1,2} .*$', fileName)

    if matchSeries:
        return destinations["TV"]
    elif size > 5*1024:
        return destinations["Movie"]
    else:
        return destinations["Manual"]


# Make a list of files that are marked done but not copied
def creteRunList():
    moveList = []
    with con:
        cur = con.cursor()

        query = "SELECT filename, path, lastSize FROM File WHERE done=1 AND copied=0"
        cur.execute(query)

        result = cur.fetchall()

        for filename, filepath, lastSize in result:
            moveList.append([filename, filepath, lastSize])
            udquery = "UPDATE File SET copied=1 WHERE path='%s'" % filepath.replace("\'", "\'\'")
            cur.execute(udquery)

    return moveList


# Move files
def copyFiles(threadName, q):
    global currentFileSize

    while not exitFlag:
        if not workQueue.empty():

            queueLock.acquire()
            fileData = q.get()
            queueLock.release()

            dest = determineDestination(fileData)
            
            if os.path.isdir(dest):
                dest = os.path.join(dest, os.path.basename(fileData[1]))

            filename = fileData[0]
            currentFileSize = fileData[2]

            try:
                print("\n\nCopying file: %s" % filename)
                with open(fileData[1], 'rb') as fsrc:
                    with open(dest, 'wb') as fdst:
                        copyfileobj(fsrc, fdst)
                shutil.copymode(fileData[1], dest)               

                #shutil.copy(fileData[1], dest)
                write_logg("Copied file: %s to %s" % (filename, dest), "Success")

            except IOError as e:
                write_logg("Failed to copy file %s" % filename, "Error")


# Run threads. When all threads are done, restart if new items have appeared in the queue.
def runThread(threads):
    global active, exitFlag

    for thread in threads:
        thread.start()

    while not workQueue.empty():
        pass

    exitFlag = 1

    for thread in threads:
        thread.join()

    if not workQueue.empty():
        runThread(threads)
    else:
        active = False


# Starts up threads, fills the workqueue with the files that are ready to be copied.
def handleItems():
    global exitFlag, active, allowprint

    runList = creteRunList()

    if runList and not active:
        terout("Creating new threads")

        exitFlag = 0
        active = 1

        threadID = 1
        threads = []

        for i in range(1,2):
            thread = MoveHandler(i, "Thread-%i" % i, workQueue)
            threads.append(thread)

        queueLock.acquire()

        for item in runList:

            workQueue.put(item)

        queueLock.release()

        allowprint = False
        runThread(threads)
        allowprint = True

    elif active:
        terout("Threads active, putting work in queue")
        queueLock.acquire()

        for item in runList:

            workQueue.put(item)

        queueLock.release()


#################################################################
# Functions
#################################################################

# Makes a progressbar when copying files
def displayProgress(copied):
    global lastIt

    if time.time() - lastIt >= 1 or copied == currentFileSize:
        lastIt = time.time()
        progress = int((copied/currentFileSize) * 20)
        
        cp = copied/1000000
        tot = currentFileSize/1000000

        spaces = 20 - progress
        strout = "[%s>%s] %iMB / %iMB\r" % ("="*progress, " "*spaces, cp, tot)

        sys.stdout.write(strout)
        sys.stdout.flush()


# Print given message to console if allowed
def terout(msg):
    if allowprint:
        sys.stdout.write("\n" + msg)


# Performs a task at regular intervals.
def task(interval, work_function, it = 0):
    if it != 1:
        threading.Timer(
            interval,
            task, [interval, work_function, 0 if it == 0 else it-1]
            ).start()

    work_function()


def main():
    global init

    if not exists:
        setup_db()
        init = True

    readConfig()

    task(interval, checkFiles)


main()
