import pypyodbc
def connect():
    # Specifying the ODBC driver, server name, database, etc. directly
    driver = 'Devart ODBC Driver for PostgreSQL'
    cnxn = pypyodbc.connect('DRIVER={' + driver + '};SERVER=127.0.0.1;PORT=5431;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # cnxn = pypyodbc.connect('DSN=postgres;SERVER=127.0.0.1;PORT=5432;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # Create a cursor from the connection
    return cnxn.cursor()


def read_create_db_sql(file_name,cursor):
    with open(file_name, "r") as file:
        sqlScript = file.read()
        for statement in sqlScript.split(';'):
            #ugly af but it works
            try:
                cursor.execute(statement+';')
                cursor.commit()
                print("SUCCESS: " + statement+';')
            except:
                print("FAILED: " + statement+';')



def insert_site(cursor,domain,robots_content,sitemap_content):
    cursor.execute('insert into "crawldb"."site"("domain","robots_content","sitemap_content") values (?, ?, ?)', [domain,robots_content,sitemap_content])
    cursor.commit()


#def insert_page(cursor,)
