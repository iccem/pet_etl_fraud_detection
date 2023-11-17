import sqlite3

import py_scripts.utility as u
import py_scripts.init_and_load__ddl_dmldb as init
import py_scripts.etl_pipeline as etl


def main():
    connected = False
    try:
        con = sqlite3.connect('db.db')
        connected = True
    except sqlite3.Error as err:
        print(err)
    if connected:
        init.create_db(con)

    print('Please enter a date the report in format dd-mm-yyyy')
    print('='*100)

    date_report = input()
    result_of_validation = u.validate_date(date_report)
    if result_of_validation:
        etl.create_etl_pipeline(con, date_report)

    cursor = con.cursor()
    try:
        print('='*100)
        print('The report of fraudulent transactions in the current month on ' + date_report)
        con_ = con.execute('select * from REP_FRAUD')
        names = list(map(lambda x: x[0], con_.description))
        print(names)
        u.show_table(cursor, 'REP_FRAUD')
    except:
        print('Failed to build report')


if __name__ == '__main__':
    main()
