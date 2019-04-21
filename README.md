# whitepages_webapi_demo
webservice demo for whitepages

basic developing startegie: bottom up 
- implement the inner works first: multithreaded (asyncronous) requesing the whitepages api, fill the missing data in the css.
- wrapping the implementation into a web api. Powerd by a lightweight python solution
- host it on a server

python interpreter: 2.7

required pip installs:

- fuzzywuzzy
- (optional) python-Levenshtein
- usaddress
- tornado
