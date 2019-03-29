# crawler
## 1. run docker containter
docker run -d --name ieps-database -e POSTGRES_USER=userDB -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgresDB -p 5431:5432 postgres:11.2
## 2. install odbc driver for windows
https://www.devart.com/odbc/postgresql/docs/windows.htm
## 3 download chromedriver and add it to PATH
http://chromedriver.chromium.org/
## 4. install  visual cpp build tools 
so that pip install reppy works
https://visualstudio.microsoft.com/visual-cpp-build-tools/
https://stackoverflow.com/questions/29846087/microsoft-visual-c-14-0-is-required-unable-to-find-vcvarsall-bat
https://stackoverflow.com/questions/43858836/python-installing-clarifai-vs14-0-link-exe-failed-with-exit-status-1158
## 5. report template
https://docs.google.com/document/d/1-j9zZIcu9B0l54W6XUj4XFufuzZPqVHF5LMJyrMx-Cw/edit?usp=sharing