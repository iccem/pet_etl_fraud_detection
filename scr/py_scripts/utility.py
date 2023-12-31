import datetime
import os


path = 'D:/Icc/PycharmProjects/pet_etl_fraud_detection/pet_etl_fraud_detection/data/'



def validate_date(date_of_report: str) -> bool:
    """
    Validate the the data.

    """
    try:
        datetime.datetime.strptime(date_of_report, '%d-%m-%Y')
        return True
    except ValueError as msg:
        print(msg)
        return False


def get_date(filename: str) -> str:
    """
    Parse the file name and return the date.

    """
    f = filename.split('_')
    f_ = f[-1].split('.')
    date_ = f_[0]
    temp_date = date_[4:] + '-' + date_[2:4] + '-' + date_[:2]
    return temp_date


def show_table(cursor, tbl_name: str) -> None:
    """
    Print all rows from the table.

    """
    cursor.execute('select * from ' + tbl_name)
    for row in cursor.fetchall():
        print(row)


def delete_tbl(cursor, tbl_name) -> None:
    """
    Drop table.

    """
    cursor.execute(f'DROP TABLE if exists {tbl_name}')


def delete_view(cursor, tbl_name):
    """
    Drop view.

    """
    cursor.execute(f'DROP VIEW if exists {tbl_name}')


def get_readable_date(report_dt):
    """
    Change the date according to the template.

    """
    report_dt = report_dt.replace('-', '')
    readable_report_dt = report_dt[4:] + '-' + report_dt[2:4] + '-' + report_dt[:2]
    return readable_report_dt


def upload(file):
    path_archive = os.path.join('archive', file + '.backup')
    os.rename(file, path_archive)
