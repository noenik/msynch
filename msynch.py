import sqlite3 as lite
import os, threading, queue, time, shutil, re, codecs, configparser


#################################################################
## Global variable declarations
#################################################################

accepted_filetypes = ["mkv", "mp4", "avi"]
readyFlag = 0
init = False
parser = configparser.ConfigParser()

# Directories
destinations = {}

# Global variables used for multithreading
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
logg = codecs.open("logg.txt", "a", "utf-8")


#################################################################
## Setup functions / general functions
#################################################################

## Class for thread that moves files
class MoveHandler(threading.Thread):
    def __init__(self, thID, name, q):
        threading.Thread.__init__(self)
        self.thID = thID
        self.name = name
        self.q = q

    def run(self):
        moveFiles(self.name, self.q)


## Create database file and table
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


## Read config, creates new config if it doesn't exist
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


## Create a log entry with timestamp. 
def createLogEntry(entry, entryType):
    time_of_entry = time.asctime(time.localtime(time.time()))
    entry = "%s: [%s] %s\n" % (time_of_entry, entryType, entry)

    return entry


## Write an entry to the log, aquire writeLock first to be safe.
def write_logg(message):
    writeLock.acquire()
    print("Writing to log")
    logg.write(message)

    writeLock.release()


#################################################################
## Functions for tracking files
#################################################################

## Check if the file is a desired media file by size and filetype
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

## Retrieve all files in the database
def getLoggedFiles():
    files = {}
    with con:
        cur = con.cursor()

        cur.execute("SELECT filename, lastSize, done FROM File")

        result = cur.fetchall()

        for filename, lastSize, done in result:
            files[filename] = [lastSize, done]

    return files

## Read directory for all files
def readFolder(baseDir):

    fileList = []

    for dirpath, dirnames, files in os.walk(baseDir):
        for f in files:
            fp = os.path.join(dirpath, f)

            if(ismediafile(fp)):
                fileList.append(fp)

    return fileList

## Add files that dos not already exist in the db
def checkFiles():
    global readyFlag, init, con
    con = lite.connect("files.db")
    fileList = readFolder(destinations['base'])
    loggedFiles = getLoggedFiles()

    print("\n%s: Starting new run" % time.asctime(time.localtime(time.time())))

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

            print("Found new files")
        else:
            print("No new files this time...")

        if update:
            for updateQuery in updateQueries:
                cur.execute(updateQuery)
            print("Updating some data on the files")

    if readyFlag:
        print("Ready flag up - initiating file transfers")
        handleItems()
        readyFlag = 0

    if init:
        init = False
        print("Initial run: ignoring all files")
        print("Reading folder: %s" % destinations['base'])


#################################################################
## Functions for moving files
#################################################################

def determineDestination(filedata):
    size = filedata[2]
    fileName = filedata[1]
    size /= 1000000

    matchSeries = re.search(r'(?ix) (s|season)? \d{1,2} (e|ep|x|episode) \s* \d{1,2} .*$', fileName)

    if matchSeries:
        return destinations["TV"]
    elif size > 5*1024:
        return destinations["Movie"]
    else:
        return destinations["Manual"]

## Make a list of files that are marked done but not copied
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

## Move files
def moveFiles(threadName, q):

    while not exitFlag:
        if not workQueue.empty():

            queueLock.acquire()
            fileData = q.get()
            queueLock.release()

            dest = determineDestination(fileData)
            filename = fileData[0]
            target = os.path.join(dest, filename)

            try:
                write_logg(createLogEntry("Staring copy of file: %s" % filename, "Info"))

                shutil.copy(fileData[1], dest)

                write_logg(createLogEntry("Copied file: %s to %s" % (filename, dest), "Success"))

            except IOError:
                write_logg(createLogEntry("Failed to copy file %s" % filename, "Error"))

## Run threads. When all threads are done, restart if new items have appeared in the queue.
def runThreads(threads):
    global active, exitFlag

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    exitFlag = 1

    # Wait	or all threads to complete
    for t in threads:
        t.join()

    if not workQueue.empty():
        runThreads(threads)
    else:
        active = False

## Starts up threads, fills the workqueue with the files that are ready to be copied.
def handleItems():
    global exitFlag, logg, active

    threads = []
    runList = creteRunList()

    if runList and not active:
        print("Creating new threads")
        logg = codecs.open("logg.txt", "a", "utf-8")

        exitFlag = 0
        active = 1

        threadList = ["Thread-1", "Thread-2", "Thread-3"]
        threadID = 1

        # Create new threads
        for tName in threadList:
            thread = MoveHandler(threadID, tName, workQueue)
            threads.append(thread)
            threadID += 1

        # Fill the queue
        num = 0
        queueLock.acquire()

        for item in runList:

            num += 1
            workQueue.put(item)

        queueLock.release()

        runThreads(threads)

        logg.close()

        runList = []

    elif active:
        print("Threads active, putting work in queue")
        queueLock.acquire()

        for item in runList:

            workQueue.put(item)

        queueLock.release()


## Performs a task at regular intervals. 
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
