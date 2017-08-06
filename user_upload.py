""" Author: Jack Stinchcombe
email: stinchjack@gmail.com
Updated: 6 August 2017
github:
"""
import os
import argparse
import MySQLdb
import csv
import re

def  help():
  """
    Help output def
  """

  helpText = """--file [csv file name] - this is the name of the CSV to be parsed
  --create_table - this will cause the MySQL users table to be built (and no further
  action will be taken)
  --dry_run - this will be used with the --file directive in the instance that we want to run the
  script but not insert into the DB. All other defs will be executed, but the database won't
  be altered.
  -u - MySQL username
  -p - MySQL password
  -h - MySQL host
  --dbname - specify a DB name
  --help - output this help """

  print helpText


def connectDB (username, password, host, dbname):
  # Connect to Database
  try:
      link = MySQLdb.connect(passwd=password,db=dbname, host = host, user=username)
      return link
  except Exception as e:
      print "Error: Unable to connect to MySQL." + os.linesep
      print str(e)

      return False

def execSQL (link, sql, displayError = True):
    # Exectutes an SQL statement

    c=link.cursor()
    try:
        c.execute(sql)
    except Exception as e:
        if displayError:
            print "Error: Unable to execute '"+ sql +"'." + os.linesep
            print str(e)

        return False

    return c

def checkTable(link, DBtable):
  # check specified DB table exists

  result = execSQL (link,  "SELECT 1 FROM users LIMIT 1", False)

  if (result is False):
    print os.linesep + "Table " + DBtable + " does not exist" + os.linesep
    return False

  else:
    return True

def createTable(link, tableExists):

    # If it exsits, remove so it can reuilt
    if (tableExists):

        print (os.linesep + "removing existing table 'users' " + os.linesep)

    sql = "drop table users"
    result = execSQL (link,  sql)
    if (result is not False):
      print (os.linesep + "Table users dropped " + os.linesep)

    else:
      # display error output
      print os.linesep + "Could not drop table" + os.linesep
      return False

    # SQL to creates a table 'users' in the database with
    # name, surname, and email fields.\
    sql =  """CREATE TABLE users
      (
         name VARCHAR(40),
         surname VARCHAR(40),
         email VARCHAR(40) UNIQUE
      )"""

    result = execSQL (link,  sql)

    if (result is not False):
        print (os.linesep + "Table users created " + os.linesep)
        return True

    else:
        # display error output on error
        print os.linesep + "Could not create table" + os.linesep
        return False

    return result

def loadCSV (filename):
    # Load data from CSV
    rows = []

    try:
        with open(filename) as csvfile:

            # Read CSV as dictionary, and extract values as array into the rows array.
            # The docitReader removes the header row automatically
            reader = csv.DictReader(csvfile)
            for row in reader:
                rows.append (row.values())

        return rows

    except Exception as e:
      print "Error: Unable to read CSV" + os.linesep
      print str(e)
      return False


def isValidEmail(email):
     # Checks if an email address is valid. Regex from:
     # https://stackoverflow.com/questions/201323/using-a-regular-expression-to-validate-an-email-address
    if len(email) > 7:
     regex = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""

     if re.match(regex, email) != None:
         return True
    return False

def ucfirst (string):
    # converts the first letter of a string to a captial
    return string[1].upper() + string[1:]

def cleanData (rows):
    # Cleans and validates data from CSV - assumes items in each row is first
    # name, surname and email address

    # Assumes no column headers in data
    cleanedRows = []

    for row in rows:

        row[2] = row[2].strip() # trim spaces so filter_var can do its job

        # Check for email address and skip row if not valid
        if (isValidEmail(row[2])):

          # Make sure first name and surname fields have first letter capital
          row[0] =  ucfirst (row[0].strip())
          row[1] =  ucfirst (row[0].strip())

        else:
          print os.linesep + "Email address " + row[2] + " is not valid - this row will not be inserted into table  " + os.linesep

        return cleanedRows

def insertData (link, rows):

    # inserts each row of data into the table
    count = 0
    for row in rows:

        # escape each value to avoid SQL injection problems
        #name = mysqli_real_escape_string(link, row[0])
        #surname = mysqli_escape_string(link, row[1])
        #email = mysqli_escape_string(link, row[2])

        name = row[0]
        surname = row[1]
        email = row[2]

        # create SQL insert statement and execute
        # 'insert ignore' used to ignore insertions which fail due to unique key

        sql = 'insert ignore into users (name, surname, email) values ( "' + name +'", "'+ surname +'", "'+ email +'") '

        result = execSQL  (link,  sql)

        if (result is False) :
          # display error output
          print os.linesep + "Could insert data into table" + os.linesep
          return False
        # Count rows processed for user output
        count = count + 1

    return count

def run():

    # the main function

    #set up argument parser
    parser = argparse.ArgumentParser(description='Process command line flags',
        add_help=False, usage = argparse.SUPPRESS)

    # setup parser expected arguments. Default values setup where apprpriate
    # default help behavior overridden
    parser.add_argument("-u", type=str)
    parser.add_argument("-p", type=str)
    parser.add_argument("-h", type=str, default = 'localhost')
    parser.add_argument("--help", action =  'store_true')
    parser.add_argument("--dbname", type=str, default = 'catalystUsers')
    parser.add_argument("--file", type=str, default = 'users.csv')
    parser.add_argument("--dry_run", action =  'store_true')
    parser.add_argument("--create_table", action =  'store_true')

    #process the arguments and convert to dictionary

    try:
        options = vars(parser.parse_args())
    except:
        help()
        return

    if options['help']:
        help()
        return

    CSVfile = options["file"]
    DBuser = options["u"]
    DBpassword = options["p"]
    DBhost= options["h"]
    DBname= options["dbname"]
    dry_run = options["dry_run"]
    create_table = options["create_table"]

    if (not DBuser or not DBpassword):
        print os.linesep + "MySQL username or password not set " + os.linesep
        help()
        return

    # Connect to MySQL
    DBconn = connectDB (DBuser, DBpassword, DBhost, DBname)

    if (not DBconn):
        print "Could not connect to DB" + os.linesep
        return

    # Check DB table exists
    tableExists = checkTable (DBconn, "users")

    # fail if table does not exist and table if not to be created
    if (not tableExists and not create_table):
        help()
        return

    # print error if users table already exists
    if (tableExists and create_table):
        print os.linesep + "Table 'users' already exists " + os.linesep

    # if --create_table specified, create the table if it doesn't exist
    if (create_table):
        result = createTable(DBconn, tableExists)

        if (result is not False):
            print os.linesep + "create_table flag specified, no data inserted" + os.linesep

        return

    # Load CSV data
    data = loadCSV (CSVfile)

    if (data is False):
        print "Could not load CSV CSVfile " + os.linesep
        return


    # Clean CSV data
    data = cleanData (data)
    # Stop if dry_run flag set.
    if (dry_run):
        print os.linesep + "Dry run - no data inserted into table " + os.linesep
    return

    # Insert data into table
    result = insertData(DBconn, data)

    if (result ):
        print " $result CSV rows processed " + os.linesep


run()
