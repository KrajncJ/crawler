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
        return_id = None
        # dbHelper.insert_site(cursor,"www.google.com","robots","sitecontent")
        select_query = """
            SELECT id
            FROM "crawldb"."site" 
            WHERE domain like '{}%';""".format(domain)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()

        if len(query_result) <= 0:
            insert_query = 'INSERT INTO "crawldb"."site"("domain","robots_content","sitemap_content") VALUES(?, ?, ? ) RETURNING id;'
            last_row = self.cursor.execute(insert_query, [domain,robots_content,sitemap_content])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        else:
            return_id = query_result[0][0]
        return return_id

    def insert_page(self,site_id,page_type_code,url,html_content,http_status_code,accessed_time):
        return_id = None
        # dbHelper.insert_page(cursor,1,"HTML","www.domena.com/index.php","vsebinastrani",200,str(datetime.datetime.now()))
        select_query = """
                   SELECT id,accessed_time
                   FROM "crawldb"."page" 
                   WHERE url like '{}%';""".format(url)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()

        status_code = None if http_status_code == None else str(http_status_code)
        access_time = None if accessed_time == None else str(accessed_time)

        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."page"("site_id","page_type_code","url","html_content","http_status_code","accessed_time") values (?, ?, ?, ?, ?, ?) RETURNING id'
            # last_row = self.cursor.execute(insert_query, [str(site_id),page_type_code,url,html_content,str(http_status_code),str(accessed_time)])
            last_row = self.cursor.execute(insert_query, [str(site_id),page_type_code,url,html_content,status_code,access_time])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        elif(len(query_result) > 0 and accessed_time != None and query_result[0][1] == None):
            update_query = 'UPDATE "crawldb"."page" SET "site_id" = ?,page_type_code=?,url=?,html_content=?,http_status_code=?,accessed_time=? WHERE id = ?'
            last_row = self.cursor.execute(update_query, [str(site_id), page_type_code, url, html_content, status_code, access_time, str(query_result[0][0])])
            self.cursor.commit()
            return_id = query_result[0][0]
        else:
            return_id = query_result[0][0]
        return return_id

    def insert_image(self,page_id,filename,content_type,data,accessed_time):
        return_id = None
        select_query = """
                    SELECT id
                    FROM "crawldb"."image" 
                    WHERE data = ?;"""
        self.cursor.execute(select_query,[pypyodbc.Binary(data)])
        query_result = self.cursor.fetchall()

        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."image"("page_id","filename","content_type","data","accessed_time") values (?, ?, ?, ?, ?) RETURNING id;'
            last_row = self.cursor.execute(insert_query, [str(page_id),filename,content_type,pypyodbc.Binary(data),str(accessed_time)])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        else:
            return_id = query_result[0][0]
        return return_id

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
        return_from_page = None
        select_query = """
                           SELECT from_page,to_page
                           FROM "crawldb"."link" 
                           WHERE from_page = {} AND to_page = {};""".format(from_page,to_page)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()

        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."link"("from_page","to_page") values (?, ?)'
            self.cursor.execute(insert_query,[str(from_page), str(to_page)])
            self.cursor.commit()
        else:
            return_from_page = [query_result[0][0],query_result[0][1]]
        return return_from_page








