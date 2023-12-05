
def create_db(cursor) -> None:
    """
    Generate fake data for the project.

    """
    #cursor = con.cursor()
    try:
        with open('scr\sql_scripts\ddl_dml.sql', 'r', encoding='utf-8') as f:
            script = f.read()

        cursor.executescript(script)
    except: 
        print('Database\'s already existed.')
