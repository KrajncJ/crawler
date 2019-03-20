import pypyodbc
def connect():
    # Specifying the ODBC driver, server name, database, etc. directly
    driver = 'Devart ODBC Driver for PostgreSQL'
    cnxn = pypyodbc.connect('DRIVER={' + driver + '};SERVER=127.0.0.1;PORT=5431;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # cnxn = pypyodbc.connect('DSN=postgres;SERVER=127.0.0.1;PORT=5432;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # Create a cursor from the connection
    return cnxn.cursor()


def read_create_db_sql(file_name,cursor):
    # dbHelper.read_create_db_sql("crawldb.sql",cursor)
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
    # dbHelper.insert_site(cursor,"www.google.com","robots","sitecontent")
    last_row = cursor.execute('insert into "crawldb"."site"("domain","robots_content","sitemap_content") values (?, ?, ?) RETURNING id', [domain,robots_content,sitemap_content])
    cursor.commit()
    id = None
    for r in last_row:
        id = r[0]
    return id

def insert_page(cursor,site_id,page_type_code,url,html_content,http_status_code,accessed_time):
    # dbHelper.insert_page(cursor,1,"HTML","www.domena.com/index.php","vsebinastrani",200,str(datetime.datetime.now()))
    last_row = cursor.execute('insert into "crawldb"."page"("site_id","page_type_code","url","html_content","http_status_code","accessed_time") values (?, ?, ?, ?, ?, ?) RETURNING id',
                   [str(site_id),page_type_code,url,html_content,str(http_status_code),str(accessed_time)])
    cursor.commit()
    id = None
    for r in last_row:
        id = r[0]
    return id

def insert_image(cursor,page_id,filename,content_type,data,accessed_time):
    # with open("image.jpg", 'rb') as f:
    #     hexdata = f.read()
    # dbHelper.insert_image(cursor,5,"xxx.png","contenttype",hexdata,datetime.datetime.now())
    last_row = cursor.execute('insert into "crawldb"."image"("page_id","filename","content_type","data","accessed_time") values (?, ?, ?, ?, ?) RETURNING id',
                   [str(page_id),filename,content_type,pypyodbc.Binary(data),str(accessed_time)])
    cursor.commit()
    id = None
    for r in last_row:
        id = r[0]
    return id

def insert_page_data(cursor, page_id, data_type_code, data):
    # with open("image.jpg", 'rb') as f:
    #     hexdata = f.read()
    last_row = cursor.execute(
        'insert into "crawldb"."page_data"("page_id","data_type_code","data") values (?, ?, ?) RETURNING id',
        [str(page_id), data_type_code, pypyodbc.Binary(data)])
    cursor.commit()
    id = None
    for r in last_row:
        id = r[0]
    return id

def insert_link(cursor, from_page, to_page):
    cursor.execute(
        'insert into "crawldb"."link"("from_page","to_page") values (?, ?)',
        [str(from_page), str(to_page)])
    cursor.commit()





