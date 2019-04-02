# crawler
## 1. run docker containter
```bash
docker run -d --name ieps-database -e POSTGRES_USER=userDB -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgresDB -p 5431:5432 postgres:11.2
```
## 2. install odbc driver for windows
https://www.devart.com/odbc/postgresql/docs/windows.htm
## 3 download chromedriver and add it to PATH
http://chromedriver.chromium.org/
## 4. after crawling
to check docker container size:
```sh 
docker system df -v
```