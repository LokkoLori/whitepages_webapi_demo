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

# hardcoded api keys for first experiments
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

def got_whitepage_error(resp):
    pass

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

        self.url = ""
        self.callback = None
        self.set_url_and_callback()

    def writeback(self):

        self.row[self.form.contact_filed_idx[0]] = self.name
        self.row[self.form.contact_filed_idx[1]] = self.address
        self.row[self.form.contact_filed_idx[2]] = self.phone

        print("Response processed {}".format(self.url))

    def on_fetch_reverse_phone(self, resp):

        rdata = json.loads(resp.body)

        if rdata.get("belongs_to"):
            # todo: check if it's similar to the original data ... but what if it's not?!!
            self.name = rdata["belongs_to"][0].get("name")
        if rdata.get("current_addresses"):
            self.address = extractaddress(rdata["current_addresses"][0])

        self.writeback()

    def on_fetch_find_person(self, resp):

        rdata = json.loads(resp.body)

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

    def set_url_and_callback(self):

        # fill data by phone number, reverse phone api
        if self.phone:

            self.url = urlparams(revese_phone_url_format, api_key=self.form.rp_api_key, phone=self.phone)
            self.callback = self.on_fetch_reverse_phone
            return

        # fill data by name and address, find person api
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

        self.url = urlparams(find_person_url_format, api_key=self.form.fp_api_key,
                        name=self.name,
                        address__city=city,
                        address__postal_code=postal_code,
                        address__state_code=state,
                        address__street_line_1=street
                        )
        self.callback = self.on_fetch_find_person

    async def fetch(self):

        http_client = AsyncHTTPClient()

        print("Fetching request {}".format(self.url))

        resp = await http_client.fetch(self.url)
        return resp


'''
a class represent the csv data
'''
class CSV_form():

    def __init__(self, field_list, rp_api_key, fp_api_key, parallel_batch_size):
        self.field_list = field_list
        self.contact_filed_idx = []
        self.rp_api_key = rp_api_key
        self.fp_api_key = fp_api_key
        self.parallel_batch_size = parallel_batch_size

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
                raise tornado.web.HTTPError(400, "excepted field: '{}' cannot found in the first row of given csv".format(field))
            self.contact_filed_idx.append(maxind)

        self.rows = [] #data format : (original row, Contact extracted from row)

    def addRow(self, row):

        self.rows.append(Contact(self, row, len(self.rows)))

    def write_to(self, f):
        '''
        write the current data into a stream in cvs txt
        :param f: iostream
        :return:
        '''
        f.write(",".join(['"'+f+'"' for f in self.field_list])+"\n")

        for data in self.rows:

            f.write(",".join(['"' + d + '"' for d in data.row]) + "\n")


    async def processContacts(self):

        #here comes the multi async magic
        first = 0

        while True:
            last = first + self.parallel_batch_size
            if len(self.rows) < last:
                last = len(self.rows)
            parallel_batch = self.rows[first:last]
            try:
                responses = await tornado.gen.multi([cont.fetch() for cont in parallel_batch])
            except tornado.httpclient.HTTPClientError as cle:
                raise tornado.web.HTTPError(cle.code, cle.message)
            for i in range(len(parallel_batch)):
                parallel_batch[i].callback(responses[i])
            if last == len(self.rows):
                break
            first = last


async def process_csv(input, output, rp_api_key, fp_api_key, parallel_batch_size):
    '''
    accept a csv text from the input then write the filled csv data on the output ... these could be files id StringIO
    :param input: iostream
    :param output: iostream
    :param rp_api_key: str - reverse phone API key to whitepages
    :param fp_api_key: str - find person API kez to whitepages
    :param parallel_batch_size: int - how many paralallel whitepages request are allowed
    :return:
    '''

    reader = csv.reader(input, delimiter=',')

    linenum = 1
    width = 0

    for row in reader:
        if linenum == 1:
            csv_form = CSV_form(row, rp_api_key, fp_api_key, parallel_batch_size)
            width = len(row)
        else:
            if len(row) != width:
                raise tornado.web.HTTPError(400, "Malformed csv on line {}".format(linenum))
            csv_form.addRow(row)
        linenum += 1

    await csv_form.processContacts()
    csv_form.write_to(output)


class FormHandler(tornado.web.RequestHandler):

    def write_form(self, api_key_rp=None, api_key_fp=None):
        if not api_key_rp:
            api_key_rp = api_key_reverse_phone
        if not api_key_fp:
            api_key_fp = api_key_find_person

        body = """
        <html><head><meta charset="UTF-8"></head>
        <body><form action="/filled.csv" method="POST" enctype="multipart/form-data">
        <input type="file" name="csv"/>csv file<br/>
        <input type="text" name="api_key_rp" value="{}"/>reverse phone api key<br/>
        <input type="text" name="api_key_fp" value="{}"/>find person api key<br/>
        <input type="text" name="parallel_batch_size" value="20"/>paralell  batch size<br/>
        <input type="submit" value="Submit"/>
        </form></body></html>
        """.format(api_key_rp, api_key_fp)

        self.write(body)

    def get(self):
        self.write_form()


class ApiHandler(tornado.web.RequestHandler):

    async def post(self):

        print("INCOMING REQUEST")
        self.set_header("Content-Type", "text/csv")
        try:
            text = self.request.files["csv"][0]['body'].decode("utf-8")
            input = StringIO(text)
        except Exception as e:
            #todo: raise no input like exception
            return

        output = StringIO()

        rp_api_key = self.request.body_arguments["api_key_rp"][0].decode("utf-8")
        fp_api_key = self.request.body_arguments["api_key_fp"][0].decode("utf-8")
        parallel_batch_size = int(self.request.body_arguments["parallel_batch_size"][0].decode("utf-8"))
        await process_csv(input, output, rp_api_key, fp_api_key, parallel_batch_size)

        print("RESPOND SUCCESS")
        self.write(output.getvalue())

def make_app():
    return tornado.web.Application([
        (r"/filled.csv", ApiHandler),
        (r"/", FormHandler)
    ])


if __name__ == "__main__":

    port = 8080
    app = make_app()
    app.listen(port)
    print("listening at {}".format(port))
    tornado.ioloop.IOLoop.current().start()