# crawler
## 1. run docker containter
docker run -d --name ieps-database -e POSTGRES_USER=userDB -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgresDB -p 5431:5432 postgres:11.2
## 2. install odbc driver for windows
https://www.devart.com/odbc/postgresql/docs/windows.htm
## 3. report template
https://docs.google.com/document/d/1-j9zZIcu9B0l54W6XUj4XFufuzZPqVHF5LMJyrMx-Cw/edit?usp=sharing