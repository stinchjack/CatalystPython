""" Author: Jack Stinchcombe
email: stinchjack@gmail.com
Updated: 6 August 2017
github:
"""

run()

"""
def  help() :
  /*
    Help output function
  */

  $helpText = PHP_EOL . "--file [csv file name] - this is the name of the CSV to be parsed
  --create_table - this will cause the MySQL users table to be built (and no further
  action will be taken)
  --dry_run - this will be used with the --file directive in the instance that we want to run the
  script but not insert into the DB. All other functions will be executed, but the database won't
  be altered.
  -u - MySQL username
  -p - MySQL password
  -h - MySQL host
  -- dbname - specify a DB name
  --help – output this help " . PHP_EOL

  print helpText


}
""

""" function loadCSV ($filename) :

  if (!$filename) :
    return false
  }

  /*Load data from CSV*/
  $file = fopen ($filename, "r")
  if (!$file) :
    return false
  }

  $rows = array()

  while (($data = fgetcsv($file, 1000, ",")) !== FALSE) :

      array_push ($rows, $data)

  }

  fclose ($file)

  return $rows
}

function cleanData ($rows) :
  // Cleans and validates data from CSV - assumes items in each row is first
  // name, surname and email address

  // Assumes first row is column headers
  array_shift ($rows)

  $cleanedRows = array()

  foreach ($rows as $row) :

    $row[2] = trim ($row[2]) // trim spaces so filter_var can do its job

    // Check for email address and skip row if not valid
    if (filter_var($row[2], FILTER_VALIDATE_EMAIL)) :

      // Make sure first name and surname fields have first letter capital
      $row[0] =  ucfirst (trim(strtolower($row[0])))
      $row[1] =  ucfirst (trim(strtolower($row[1])))

      array_push ($cleanedRows, $row)
    }

    else :
      print PHP_EOL . "Email address $row[2] is not valid - this row will not be inserted into table  " . PHP_EOL
    }
  }

  return $cleanedRows
}

function connectDB ($username, $password, $host, $dbname) :
  /*Connect to Database*/

  $link = mysqli_connect($host, $username, $password, $dbname)

  //Display error info on failure
  if (!$link) :

      echo "Error: Unable to connect to MySQL." . PHP_EOL
      echo "Debugging errno: " . mysqli_connect_errno() . PHP_EOL
      echo "Debugging error: " . mysqli_connect_error() . PHP_EOL

      return false
  }

  return $link

}

function checkTable($link, $DBtable) :
  //check specified DB table exists
  $result = mysqli_query ($link,  "SELECT 1 FROM users LIMIT 1")

  if (!$result) :
    print PHP_EOL . "Table $DBtable does not exist" . PHP_EOL
    return false
  }
  else :
    return true
  }
}

function createTable($link, $tableExists) :

  // If it exsits, remove so it can reuilt
  if ($tableExists) :

    print (PHP_EOL . "removing existing table 'users' " . PHP_EOL)

    $sql = "drop table users"
    $result = mysqli_query ($link,  $sql)
    if ($result) :
      print (PHP_EOL . "Table users dropped " . PHP_EOL)
    }
    else :
      //display error output
      print PHP_EOL . "Could not drop table" . PHP_EOL
      echo "Debugging errno: " . mysqli_connect_errno() . PHP_EOL
      echo "Debugging error: " . mysqli_connect_error() . PHP_EOL
      return false
    }
  }


  // SQL to creates a table 'users' in the database with
  // name, surname, and email fields.\
  $sql =  "CREATE TABLE users
      (
         name VARCHAR(40),
         surname VARCHAR(40),
         email VARCHAR(40) UNIQUE
      )"

  $result = mysqli_query ($link,  $sql)

  if ($result) :
    print (PHP_EOL . "Table users created " . PHP_EOL)
    return true
  }
  else :
    //display error output
    print PHP_EOL . "Could not create table" . PHP_EOL
    echo "Debugging errno: " . mysqli_connect_errno() . PHP_EOL
    echo "Debugging error: " . mysqli_connect_error() . PHP_EOL
    return false
  }

  return $result
}


function insertData ($link, $rows) :
  //inserts each row of data into the table
  $count = 0
  foreach ($rows as $row) :

    //escape each value to avoid SQL injection problems
    $name = mysqli_real_escape_string($link, $row[0])
    $surname = mysqli_escape_string($link, $row[1])
    $email = mysqli_escape_string($link, $row[2])


    /* create SQL insert statement and execute
      'insert ignore' used to ignore insertions which fail due to unique key
    */
    $sql = 'insert ignore into users (name, surname, email) values ( "'. $name .'", "'. $surname .'", "'. $email .'") '

    $result = mysqli_query ($link,  $sql)

    if (!$result)  :
      //display error output

      print PHP_EOL . "Could insert data into table" . PHP_EOL
      echo "Debugging errno: " . mysqli_connect_errno() . PHP_EOL
      echo "Debugging error: " . mysqli_connect_error() . PHP_EOL
      return false
    }

    //Count rows inserted for user output
    $count++

  }

  return $count
}

function run() :

  /*
  The main function of the script
  */

  //get options
  $options = getopt("u:p:h:",  array("dry_run", "file:", "create_table", "help", "dbname:"))

  if (array_key_exists ("help", $options)) :
    // if help in command line options, display help then exit
    help()
    return
  }


  //process file name from command line
  if (array_key_exists  ("file", $options)) :
    $CSVfile = options["file"]
  }
  else :
    $CSVfile = "users.csv" // default file name to use if none specified
  }

  // get MYSQL user name from command line
  if (array_key_exists  ("u", $options)) :
    $DBuser = $options["u"]
  }

  // get MYSQL user name from command line
  if (array_key_exists  ("p", $options)) :
    $DBpassword = $options["p"]
  }

  // get MYSQL hostname from command line
  if (array_key_exists  ("h", $options)) :
    $DBhost= $options["h"]
  }
  else :
    $DBhost= "localhost" // default if no host specified
  }

  // get MYSQL Database from command line
  if (array_key_exists  ("dbname", $options)) :
    $DBname= $options["dbname"]
  }
  else :
    $DBname= "catalystUsers" // default if no DB name specified
  }

  // get dry_run flag from
  $dry_run = array_key_exists  ("dry_run", $options)

  // get create_table flag from
  $create_table = array_key_exists  ("create_table", $options)

  if (!$DBuser || !$DBpassword) :
    print PHP_EOL . "MySQL username or password not set " . PHP_EOL
    help()
    return
  }

  // Connect to MySQL
  $DBconn = connectDB ($DBuser, $DBpassword, $DBhost, $DBname)

  if (!$DBconn) :
    print "Could not connect to DB" . PHP_EOL
    return
  }

  //Check DB table exists
  $tableExists = checkTable ($DBconn, "users")

  // fail if table does not exist and table if not to be created
  if (!$tableExists && !$create_table) :
    help()
    return
  }

  // print error if users table already exists
  if ($tableExists && $create_table) :
    print PHP_EOL . "Table 'users' already exists " . PHP_EOL
  }

  //if --create_table specified, create the table if it doesn't exist
  if ($create_table) :

    $result = createTable($DBconn, $tableExists)

    if ($result) :
      print PHP_EOL . "create_table flag specified, no data inserted" . PHP_EOL
    }

    return
  }

  // Load CSV data
  $data = loadCSV ($CSVfile)

  if (!$CSVfile) :
    print "Could not load CSV $CSVfile " . PHP_EOL
    return
  }

  // Clean CSV data
  $data = cleanData ($data)
  //Stop if dry_run flag set.
  if ($dry_run) :
    print PHP_EOL . "Dry run - no data inserted into table " . PHP_EOL
    return
  }

  // Insert data into table
  $result = insertData($DBconn, $data)

  if ($result) :
    print " $result rows inserted /updated " . PHP_EOL
  }
}
"""
