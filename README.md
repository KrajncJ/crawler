# crawler
##1. run docker containter
docker run -d --name ieps-database -e POSTGRES_USER=userDB -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgresDB -p 5432:5432 postgres:11.2
##2. install odbc driver for windows
https://www.devart.com/odbc/postgresql/docs/windows.htm
