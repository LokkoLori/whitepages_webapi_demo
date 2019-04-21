import tornado.ioloop
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient
import csv
from fuzzywuzzy import fuzz
from io import StringIO
import urllib.parse
import urllib.request
import json
import usaddress

# hard coded api keys for first experiments
api_key_reverse_phone = '7f365d38fb47449a985ccc972e7818aa'
api_key_find_person = '44c5b314db5b4dd0a9a7c2454fbbdd02'

revese_phone_url_format = "https://proapi.whitepages.com/3.0/phone.json"
find_person_url_format = "https://proapi.whitepages.com/3.0/person"

def urlparams(url, **kwargs):

    rurl = url
    sc = "?"
    for key, value in kwargs.items():
        if not value:
            continue
        key = key.replace("__", ".")
        rurl += sc + key + "=" + urllib.parse.quote(value)
        sc = "&"

    return rurl


def extractaddress(wp_addr_dict):

    raddrd = [
        wp_addr_dict["street_line_1"],
        wp_addr_dict["street_line_2"],
        wp_addr_dict["city"],
        wp_addr_dict["state_code"] + ' ' + wp_addr_dict["postal_code"]
    ]
    raddrd = [ad for ad in raddrd if ad and ad != "None"]
    return ", ".join(raddrd)

http_client = AsyncHTTPClient(max_clients=20)

'''
represent a contact, it has 3 fileds: name, address, phone
'''
class Contact():


    def __init__(self, form, row, idx):

        self.row = row
        self.idx = idx
        self.form = form

        self.name = row[form.contact_filed_idx[0]]
        self.address = row[form.contact_filed_idx[1]]
        self.phone = row[form.contact_filed_idx[2]]

        self.get_data_from_whitepages()

    def writeback(self):

        self.row[self.form.contact_filed_idx[0]] = self.name
        self.row[self.form.contact_filed_idx[1]] = self.address
        self.row[self.form.contact_filed_idx[2]] = self.phone

        self.form.feedback(self.idx)

    def on_fetch_reverse_phone(self, f):

        rdata = json.loads(f.result().body)

        if rdata.get("belongs_to"):
            # todo: check if it similar to the original data ... but what if it is not?
            self.name = rdata["belongs_to"][0].get("name")
        if rdata.get("current_addresses"):
            self.address = extractaddress(rdata["current_addresses"][0])

        self.writeback()

    def on_fetch_find_person(self, f):

        rdata = json.loads(f.result().body)

        if type(rdata.get("person")) is not list or not rdata["person"]:
            return

        person = rdata["person"][0]
        if person.get("name"):
            self.name = person["name"]
        if person.get("phones") and person["phones"][0].get("phone_number"):
            self.phone = person["phones"][0]["phone_number"]
        if person.get("found_at_address"):
            self.address = extractaddress(person["found_at_address"])

        self.writeback()

    def get_data_from_whitepages(self):
        # this is the main processing method, push it into thread a pool for parallel processing...


        # fill data by phone number
        if self.phone:

            url = urlparams(revese_phone_url_format, api_key=api_key_reverse_phone, phone=self.phone)
            print("request: {}".format(url))

            fetch = http_client.fetch(url)
            fetch.add_done_callback(self.on_fetch_reverse_phone)
            return

        #fill data by name and address, find person api
        city = ""
        state = ""
        street = ""
        postal_code = ""
        if self.address:
            paddr = usaddress.parse(self.address)
            for e in paddr:
                if e[1] == "PlaceName":
                    city += " " + e[0]
                elif e[1] == "StateName":
                    state = e[0]
                elif e[1] == "ZipCode":
                    postal_code = e[0]
                elif e[1] in ["AddressNumber", "StreetName", "StreetNamePostType", "StreetNamePostDirectional", "OccupancyType", "OccupancyIdentifier"]:
                    street += " " + e[0]

        city = city.replace(",", "").strip()
        street = street.replace(",", "").strip()

        url = urlparams(find_person_url_format, api_key=api_key_find_person,
                        name=self.name,
                        address__city=city,
                        address__postal_code=postal_code,
                        address__state_code=state,
                        address__street_line_1=street
                        )

        print("request: {}".format(url))
        fetch = http_client.fetch(url)
        fetch.add_done_callback(self.on_fetch_find_person)


'''
a class represent the csv data
'''
class CSV_form():

    def __init__(self, field_list):
        self.field_list = field_list
        self.contact_filed_idx = []
        expected_fields = ("name", "address", "phone")

        #do the fuzzy matching (experimental)
        for field in expected_fields:
            maxmatch = 0
            maxind = -1
            treshold = 30 #experimental value
            for i in range(len(field_list)):
                if i in self.contact_filed_idx:
                    continue
                matchv = fuzz.ratio(field, field_list[i])
                if maxmatch < matchv:
                    maxind = i
                    maxmatch = matchv
            if maxmatch < treshold:
                raise Exception("excepted field: '{}' cannot found in the first row of given csv".format(field))
            self.contact_filed_idx.append(maxind)

        self.data = [] #data format : (original row, Contact extracted from row)
        self.waiting = True
        self.done_row_count = 0


    def feedback(self, idx):
        self.done_row_count += 1

        if self.done_row_count == len(self.data):
            s = StringIO()
            self.write_to(s)
            print(s.getvalue())


    def addRow(self, row):

        self.data.append(Contact(self, row, len(self.data)))

    def write_to(self, f):
        '''
        write the current data into a stream in cvs txt
        :param f: iostream
        :return:
        '''
        f.write(",".join(['"'+f+'"' for f in self.field_list])+"\n")

        for data in self.data:

            f.write(",".join(['"' + d + '"' for d in data.row]) + "\n")


def process_csv(input, output):
    '''
    accept a csv text from the input then write the filled csv data on the output ... these could be files id StringIO
    :param input: iostream
    :param output: iostream
    :return:
    '''

    reader = csv.reader(input, delimiter=',')

    first_row = True
    for row in reader:
        if first_row:
            csv_form = CSV_form(row)
            first_row = False
        else:
            csv_form.addRow(row)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        with open("sample.txt", encoding="utf-8") as f:
            with open("result.csv", "w", encoding="utf-8") as r:
                process_csv(f, r)

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])


if __name__ == "__main__":

    port = 8080
    app = make_app()
    app.listen(port)
    print("listening at {}".format(port))
    tornado.ioloop.IOLoop.current().start()


