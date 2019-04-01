import pypyodbc
import datetime
class New_dbHelper:
    def __init__(self):
        self.cursor = self.connect()



    def connect(self):
        # Specifying the ODBC driver, server name, database, etc. directly
        driver = 'Devart ODBC Driver for PostgreSQL'
        cnxn = pypyodbc.connect('DRIVER={' + driver + '};SERVER=192.168.99.100;PORT=5432;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
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
            WHERE domain = '{}';""".format(domain)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()

        if len(query_result) <= 0:
            insert_query = 'INSERT INTO "crawldb"."site"("domain","robots_content","sitemap_content", "accessed_time") VALUES(?, ?, ?, ? ) RETURNING id;'
            last_row = self.cursor.execute(insert_query, [domain,robots_content,sitemap_content, str(datetime.datetime.now())])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        else:
            return_id = query_result[0][0]
        return return_id

    def set_duplicate_page(self,page_id):
        update_query = 'UPDATE "crawldb"."page" SET page_type_code=? WHERE id = ?'
        self.cursor.execute(update_query, ["DUPLICATE",str(page_id)])
        self.cursor.commit()

    def insert_page(self,site_id,page_type_code,url,html_content,http_status_code,accessed_time, hex_digest):
        return_id = None
        # dbHelper.insert_page(cursor,1,"HTML","www.domena.com/index.php","vsebinastrani",200,str(datetime.datetime.now()))
        select_query = """
                   SELECT id,accessed_time
                   FROM "crawldb"."page" 
                   WHERE url = '{}';""".format(url)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()

        status_code = None if http_status_code == None else str(http_status_code)
        access_time = None if accessed_time == None else str(accessed_time)

        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."page"("site_id","page_type_code","url","html_content","http_status_code","accessed_time", "accessed", "hex_digest") values (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id'
            # last_row = self.cursor.execute(insert_query, [str(site_id),page_type_code,url,html_content,str(http_status_code),str(accessed_time)])
            last_row = self.cursor.execute(insert_query, [str(site_id),page_type_code,url,html_content,status_code,access_time, '0', None])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        elif(len(query_result) > 0 and access_time != None and query_result[0][1] == None):
            update_query = 'UPDATE "crawldb"."page" SET "site_id" = ?,page_type_code=?,url=?,html_content=?,http_status_code=?,accessed_time=?, accessed = accessed +1, hex_digest=? WHERE id = ?'
            # print([str(site_id), page_type_code, url, html_content, status_code, access_time, str(query_result[0][0])])
            self.cursor.execute(update_query, [str(site_id), page_type_code, url, html_content, status_code, access_time, hex_digest, str(query_result[0][0])])
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

        i_filename = filename
        if len(filename) > 255:
            i_filename = filename[-249:]
        i_content_type = content_type
        if len(content_type) > 50:
            i_content_type = content_type[:49]
            return None



        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."image"("page_id","filename","content_type","data","accessed_time") values (?, ?, ?, ?, ?) RETURNING id;'
            last_row = self.cursor.execute(insert_query, [str(page_id),i_filename,i_content_type,pypyodbc.Binary(data),str(accessed_time)])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        else:
            return_id = query_result[0][0]
        return return_id

    def insert_page_data(self, page_id, data_type_code, data):
        return_id = None
        select_query = """
                           SELECT id
                           FROM "crawldb"."page_data" 
                           WHERE page_id = ?;"""
        self.cursor.execute(select_query, [str(page_id)])
        query_result = self.cursor.fetchall()

        if len(query_result) <= 0:
            insert_query = 'insert into "crawldb"."page_data"("page_id","data_type_code","data") values (?, ?, ?) RETURNING id'
            # print([str(page_id), data_type_code, pypyodbc.Binary(data)])
            last_row = self.cursor.execute(insert_query, [str(page_id), data_type_code.upper(), pypyodbc.Binary(data)])
            self.cursor.commit()
            for r in last_row:
                return_id = r[0]
        else:
            return_id = query_result[0][0]
        return return_id


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


    def get_frontier_size(self):
        select_query = """
                                  SELECT count(id)
                                  FROM "crawldb"."page" 
                                  WHERE page_type_code = 'FRONTIER' 
                                  """
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()
        print('**** DB frontier count: '+ str(query_result[0]))


    def get_site(self, netloc):
        select_query = """
                                         SELECT id,domain,robots_content
                                         FROM "crawldb"."site" 
                                         WHERE domain = '{}'
                                         """.format(netloc)

        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()
        if len(query_result) > 0:
            return query_result[0]
        return None

    def get_pages_to_fetch(self, limit):
        select_query = """
                             SELECT id,site_id,url
                             FROM "crawldb"."page" 
                             WHERE accessed < 5 and page_type_code = 'FRONTIER' 
                             ORDER BY id ASC LIMIT {} 
                             """.format(limit)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()
        return query_result

    def exist_page(self, url):
        select_query = """
                   SELECT id,accessed_time
                   FROM "crawldb"."page" 
                   WHERE url = '{}';""".format(url)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()
        result = len(query_result) > 0
        print('Existing with url: '+ url + ' -> '+ str(len(query_result))+' '+ str(result))
        return result

    def exist_digest(self, digest):
        select_query = """
                   SELECT id,accessed_time
                   FROM "crawldb"."page" 
                   WHERE hex_digest = '{}';""".format(digest)
        self.cursor.execute(select_query)
        query_result = self.cursor.fetchall()
        result = len(query_result) > 0
        return result