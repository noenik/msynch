import sqlite3 as lite
import os, threading, queue, time, shutil, re, codecs, configparser, sys

#################################################################
# Global variable declarations
#################################################################

accepted_filetypes = ["mkv", "mp4", "avi"]
readyFlag = 0
init = False
parser = configparser.ConfigParser()

animes = []

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

# Database and log variables
exists = os.path.exists("files.db")
con = lite.connect("files.db")


#################################################################
# Setup functions / general functions
#################################################################

# Create database file and table
def setup_db():
    with con:
        cur = con.cursor()

        cur.execute(
            "CREATE TABLE File(" +
            "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "date_copied DATETIME DEFAULT CURRENT_TIMESTAMP, " +
            "filename TEXT, " +
            "old_path TEXT, " +
            "new_path TEXT, " +
            "dest_category TEXT," +
            "copied INTEGER)"
        )


# Read config, creates new config if it doesn't exist
def readConfig():
    global destinations, interval, animes

    requiredSections = ["Paths", "Misc"]
    requiredFields = ["tv", "movie", "manual", "base"]

    if not os.path.exists("config.ini"):
        with open("config.ini", "w") as conf:
            conf.write("[Paths]\nTV = ./TV\nMovie = ./Movies\nManual = ./Manual\nbase = ./\n\n"
                       + "[Misc]\nUnit = minutes\nInterval = 30")
        terout("No config was found, so a new one was created. Please edit and restart", prefix="INFO")
        exit(1)
    else:
        parser.read("config.ini")

        for section in requiredSections:
            if section not in parser.keys():
                terout("Error: Missing section %s in config" % section, prefix="CONFIG ERROR")
                exit(1)

        for key in requiredFields:
            if key not in parser['Paths']:
                terout("Error: missing parameter %s in config" % key, prefix="CONFIG ERROR")
                exit(1)

        # for val in parser['Paths']:
        destinations = parser['Paths']

        if "Anime" in parser.keys() and "Series" in parser["Anime"]:
            animes = parser['Anime']['Series'].split(",")
        else:
            animes = False


# Write an entry to the log, aquire writeLock first to be safe.
def write_logg(entry, entryType):
    logg = codecs.open("logg.txt", "a", "utf-8")

    time_of_entry = time.asctime(time.localtime(time.time()))
    entry = "%s: [%s] %s\n" % (time_of_entry, entryType, entry)

    with logg:
        logg.write(entry)


# Print given message to console if allowed
def terout(msg, prefix=""):
    if prefix:
        prefix = "[%s] " % prefix
    time_of_entry = time.asctime(time.localtime(time.time()))
    sys.stdout.write(time_of_entry + " " + prefix + msg + "\n")


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
    files = []
    with con:
        cur = con.cursor()

        cur.execute("SELECT filename FROM File")

        result = cur.fetchall()

        for filename in result:
            files.append(filename[0])

    return files


# Read directory for all files
def readFolder(baseDir):
    fileList = []

    for dirpath, dirnames, files in os.walk(baseDir):
        for f in files:
            fp = os.path.join(dirpath, f)

            if ismediafile(fp):
                fileList.append(fp)

    return fileList


# Add files that dos not already exist in the db
def check_files():
    global init, con
    con = lite.connect("files.db")
    file_list = readFolder(destinations['base'])
    logged_files = getLoggedFiles()

    with con:
        cur = con.cursor()

        ins_query = "INSERT INTO File (filename, old_path, new_path, dest_category, copied) VALUES "

        ins_queries = []
        ins_vals = []
        it = 1

        copied = 1 if init else 0

        for currentFile in file_list:
            file_name = currentFile.split(os.path.sep)[-1]
            destination, dest_cat = determineDestination(file_name, os.path.getsize(currentFile))
            destination = os.path.join(destination, file_name)

            ins_name = file_name.replace("\'", "\'\'")
            ins_path_source = currentFile.replace("\'", "\'\'")
            ins_path_dest = destination.replace("\'", "\'\'")

            if file_name not in logged_files:
                terout("New file! " + file_name, prefix="INFO")
                ins_vals.append("('%s', '%s', '%s', '%s', %i)" % (ins_name, ins_path_source, ins_path_dest, dest_cat, copied))

                if it > 100:
                    ins_queries.append(ins_query + ", ".join(ins_vals) + ";")
                    ins_vals = []
                    it = 0

                it += 1

        if ins_vals:
            ins_queries.append(ins_query + ", ".join(ins_vals) + ";")

        if ins_queries:
            for query in ins_queries:
                cur.execute(query)
            handle_items()

    if init:
        init = False
        terout("Initial run: ignoring all files", prefix="INFO")
        terout("Reading folder: %s" % destinations['base'], prefix="INFO")


#################################################################
# Functions for copying files
#################################################################


def copyfileobj(fsrc, fdst, length=16 * 1024 * 1024):
    copied = 0
    while True:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)


# Returns the target destinations based on regex and size matching
def determineDestination(file_name, size):
    size /= 1000000

    matchSeries = re.search(r'(?ix) (s|season) \d{1,2} (e|ep|x|episode) \s* \d{1,2} .*$', file_name)
    match_anime = False

    for anime in animes:
        a = anime.lower()
        fn = file_name.lower()
        if a in fn:
            match_anime = True

    if match_anime:
        return destinations["Anime"], "Anime"
    elif matchSeries:
        return destinations["TV"], "TV"
    elif size > 5 * 1024:
        return destinations["Movie"], "Movie"
    else:
        return destinations["Manual"], "Manual"


# Make a list of files that are marked done but not copied
def creteRunList():
    moveList = []
    with con:
        cur = con.cursor()

        query = "SELECT filename, old_path, new_path FROM File WHERE copied=0"
        cur.execute(query)

        result = cur.fetchall()

        for filename, old_path, new_path in result:
            moveList.append([filename, old_path, new_path])
            udquery = "UPDATE File SET copied=1 WHERE old_path='%s'" % old_path.replace("\'", "\'\'")
            cur.execute(udquery)

    return moveList


# Starts up threads, fills the workqueue with the files that are ready to be copied.
def handle_items():
    global exitFlag, active, allowprint

    run_list = creteRunList()

    if run_list:
        for item in run_list:
            filename = item[0]
            source_path = item[1]
            dest_path = item[2]

            try:
                terout("Copying file " + filename, prefix="COPY")
                with open(source_path, 'rb') as fsrc:
                    with open(dest_path, 'wb') as fdst:
                        copyfileobj(fsrc, fdst)
                shutil.copymode(source_path, dest_path)

                terout("File copied", prefix="SUCCESS")
                write_logg("Copied file: %s to %s" % (filename, dest_path), "Success")

            except IOError as e:
                terout("Failed to copy file %s, is the destination right?" % filename, prefix="ERROR")
                write_logg("Failed to copy file %s" % filename, "Error")


#################################################################
# Functions
#################################################################

def main():
    global init

    if not exists:
        setup_db()
        init = True

    readConfig()

    check_files()


if __name__ == '__main__':
    main()
