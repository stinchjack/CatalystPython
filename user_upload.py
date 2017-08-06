""" Author: Jack Stinchcombe
email: stinchjack@gmail.com
Updated: 6 August 2017
github:
"""
import os
import argparse
import MySQLdb
import pdb


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

def execSQL (link, sql):
    c=db.cursor()
    try:
        c.execute(sql, (result))
      except Exception as e:
          print "Error: Unable to execute '"+ sql +"'." + os.linesep
          print str(e)

          return False

def checkTable(link, DBtable):
  # check specified DB table exists
  result = execSQL (link,  "SELECT 1 FROM users LIMIT 1")

  if (result is False):
    print os.linesep + "Table DBtable does not exist" + os.linesep
    return False

  else:
    return True


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
    parser.add_argument("--help", action =  'store_True')
    parser.add_argument("--dbname", type=str, default = 'catalystUsers')
    parser.add_argument("--file", type=str, default = 'users.csv')
    parser.add_argument("--dry_run", action =  'store_True')
    parser.add_argument("--create_table", action =  'store_True')

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
"""

  # print error if users table already exists
  if (tableExists and create_table):
    print os.linesep + "Table 'users' already exists " + os.linesep


  # if --create_table specified, create the table if it doesn't exist
  if (create_table):

    result = createTable(DBconn, tableExists)

    if (result):
      print os.linesep + "create_table flag specified, no data inserted" + os.linesep


    return


  # Load CSV data
  data = loadCSV (CSVfile)

  if (not CSVfile):
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
    print " result rows inserted /updated " + os.linesep

def loadCSV (filename):

  if (not filename):
    return False


  # # Load data from CSV
  file = fopen (filename, "r")
  if (not file):
    return False


  rows = array()

  while ((data = fgetcsv(file, 1000, ",")) not == False):

      array_push (rows, data)



  fclose (file)

  return rows


def cleanData (rows):
  # Cleans and validates data from CSV - assumes items in each row is first
  # name, surname and email address

  # Assumes first row is column headers
  array_shift (rows)

  cleanedRows = array()

  foreach (rows as row):

    row[2] = trim (row[2]) # trim spaces so filter_var can do its job

    # Check for email address and skip row if not valid
    if (filter_var(row[2], FILTER_VALIDATE_EMAIL)):

      # Make sure first name and surname fields have first letter capital
      row[0] =  ucfirst (trim(strtolower(row[0])))
      row[1] =  ucfirst (trim(strtolower(row[1])))

      array_push (cleanedRows, row)


    else:
      print PHP_EOL + "Email address row[2] is not valid - this row will not be inserted into table  " + PHP_EOL



  return cleanedRows









def createTable(link, tableExists):

  # If it exsits, remove so it can reuilt
  if (tableExists):

    print (os.linesep + "removing existing table 'users' " + os.linesep)

    sql = "drop table users"
    result = mysqli_query (link,  sql)
    if (result):
      print (os.linesep + "Table users dropped " + os.linesep)

    else:
      # display error output
      print os.linesep + "Could not drop table" + os.linesep
      print "Debugging errno: " . mysqli_connect_errno() + os.linesep
      print "Debugging error: " . mysqli_connect_error() + os.linesep
      return False




  # SQL to creates a table 'users' in the database with
  # name, surname, and email fields.\
  sql =  "CREATE TABLE users
      (
         name VARCHAR(40),
         surname VARCHAR(40),
         email VARCHAR(40) UNIQUE
      )"

  result = mysqli_query (link,  sql)

  if (result):
    print (os.linesep + "Table users created " + os.linesep)
    return True

  else:
    # display error output
    print os.linesep + "Could not create table" + os.linesep
    print "Debugging errno: " . mysqli_connect_errno() + os.linesep
    print "Debugging error: " . mysqli_connect_error() + os.linesep
    return False


  return result



def insertData (link, rows):
  # inserts each row of data into the table
  count = 0
  foreach (rows as row):

    # escape each value to avoid SQL injection problems
    name = mysqli_real_escape_string(link, row[0])
    surname = mysqli_escape_string(link, row[1])
    email = mysqli_escape_string(link, row[2])


    # create SQL insert statement and execute
    # 'insert ignore' used to ignore insertions which fail due to unique key

    sql = 'insert ignore into users (name, surname, email) values ( "'. name .'", "'. surname .'", "'. email .'") '

    result = mysqli_query (link,  sql)

    if (not result) :
      # display error output

      print os.linesep + 'Could insert data into table + os.linesep
      print "Debugging errno: " . mysqli_connect_errno() + os.linesep
      print "Debugging error: " . mysqli_connect_error() + os.linesep
      return False


    # Count rows inserted for user output
    count++



  return count

"""

run()
