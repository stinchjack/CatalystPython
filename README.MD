# user_upload.py

## Author: Jack Stinchcombe
### Email: stinchjack@gmail.com
### Updated: 6 August 2017

## Description

Loads a CSV file with three columns (first name, surname, and email) into a database table. Entries with duplicate email addresses or already in the table will be ignored, as the email column has a UNIQUE index.

## Assumptions
- The input needs to be a comma separated value (CSV) file.
- The input CSV needs to have exactly three columns in every row.
- The three columns of the CSV must be in the order first name, surname, and email.
- There is a header row at the first row of the CSV.
- There are no blank rows in the CSV.

## Requirements:
- mysqlclient 1.3.10 - https://github.com/PyMySQL/mysqlclient-python
- Python 2.7
- Mysql 5.4

## Installation:
-
- Create a MySQL database.
- sudo apt-get install python-mysqldb

## Usage
- Create the database table using the --create_table table flag first time the script is run. If this flag is used when the table already exists, the table will be dropped and rebuilt.
- Specify MySQL username and password on the command line (-u and -p flags).
- Optionally, you may specify a hostname with -h flag (defult localhost)
- Specify a database name to use using the --dbname flag (default catalystUsers)
- Specify a input CSV using --file flag (default users.csv)

See Command line Options, below.

## Example usage:
- python user_upload.py-u john -p password

## Command line options:

- --file [csv file name] - this is the name of the CSV to be parsed (default users.csv if not specified)
- --create_table - this will cause the MySQL users table to be built (and no further action will be taken)
- --dry_run - this will be used with the --file directive in the instance that we want to run the script but not insert into the DB. All other functions will be executed, but the database won't be altered.
- -u - MySQL username
- -p - MySQL password
- -h - MySQL host (default localhost if not specified)
- --dbname - specify a DB name (default catalystUsers if not specified)
- --help – output help
