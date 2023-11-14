import datetime


def validate_date(date_of_report: str) -> bool:
    """
    Validates the validity of the data.

    """
    try:
        datetime.datetime.strptime(date_of_report, '%d-%m-%Y')
        return True
    except ValueError as msg:
        print(msg)
        return False


def show_table(cursor, tbl_name: str) -> None:
    """
    Prints all rows from the table.

    """
    cursor.execute('select * from ' + tbl_name)
    for row in cursor.fetchall():
        print(row)


def delete_tbl(cursor, tbl_name) -> None:
    """
    Drops table.

    """
    cursor.execute(f'DROP TABLE if exists {tbl_name}')


def delete_view(cursor, tbl_name):
    """
    Drops view.

    """
    cursor.execute(f'DROP VIEW if exists {tbl_name}')


def get_readable_date(report_dt):
    """
    Changes the date according to the template.

    """
    report_dt = report_dt.replace('-', '')
    readable_report_dt = report_dt[4:] + '-' + report_dt[2:4] + '-' + report_dt[:2]
    return readable_report_dt
