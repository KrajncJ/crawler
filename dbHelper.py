import pypyodbc

class New_dbHelper:
    def __init__(self):
        self.cursor = self.connect()



    def connect(self):
        # Specifying the ODBC driver, server name, database, etc. directly
        driver = 'Devart ODBC Driver for PostgreSQL'
        cnxn = pypyodbc.connect('DRIVER={' + driver + '};SERVER=127.0.0.1;PORT=5431;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
        return cnxn.cursor()


    def read_create_db_sql(self,file_name):
        # dbHelper.read_create_db_sql("crawldb.sql",cursor)
        with open(file_name, "r") as file:
            sqlScript = file.read()
            for statement in sqlScript.split(';'):
                #ugly af but it works
                try:
                    self.cursor.execute(statement+';')
                    self.cursor.commit()
                    print("SUCCESS: " + statement+';')
                except:
                    print("FAILED: " + statement+';')



    def insert_site(self,domain,robots_content,sitemap_content):
        # dbHelper.insert_site(cursor,"www.google.com","robots","sitecontent")
        last_row = self.cursor.execute('insert into "crawldb"."site"("domain","robots_content","sitemap_content") values (?, ?, ?) RETURNING id', [domain,robots_content,sitemap_content])
        self.cursor.commit()
        id = None
        for r in last_row:
            id = r[0]
        return id

    def insert_page(self,site_id,page_type_code,url,html_content,http_status_code,accessed_time):
        # dbHelper.insert_page(cursor,1,"HTML","www.domena.com/index.php","vsebinastrani",200,str(datetime.datetime.now()))
        last_row = self.cursor.execute('insert into "crawldb"."page"("site_id","page_type_code","url","html_content","http_status_code","accessed_time") values (?, ?, ?, ?, ?, ?) RETURNING id',
                       [str(site_id),page_type_code,url,html_content,str(http_status_code),str(accessed_time)])
        self.cursor.commit()
        id = None
        for r in last_row:
            id = r[0]
        return id

    def insert_image(self,page_id,filename,content_type,data,accessed_time):
        # with open("image.jpg", 'rb') as f:
        #     hexdata = f.read()
        # dbHelper.insert_image(cursor,5,"xxx.png","contenttype",hexdata,datetime.datetime.now())
        last_row = self.cursor.execute('insert into "crawldb"."image"("page_id","filename","content_type","data","accessed_time") values (?, ?, ?, ?, ?) RETURNING id',
                       [str(page_id),filename,content_type,pypyodbc.Binary(data),str(accessed_time)])
        self.cursor.commit()
        id = None
        for r in last_row:
            id = r[0]
        return id

    def insert_page_data(self, page_id, data_type_code, data):
        # with open("image.jpg", 'rb') as f:
        #     hexdata = f.read()
        last_row = self.cursor.execute(
            'insert into "crawldb"."page_data"("page_id","data_type_code","data") values (?, ?, ?) RETURNING id',
            [str(page_id), data_type_code, pypyodbc.Binary(data)])
        self.cursor.commit()
        id = None
        for r in last_row:
            id = r[0]
        return id

    def insert_link(self, from_page, to_page):
        self.cursor.execute(
            'insert into "crawldb"."link"("from_page","to_page") values (?, ?)',
            [str(from_page), str(to_page)])
        self.cursor.commit()





