import csv
from fuzzywuzzy import fuzz
from io import StringIO
import urllib.parse
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

'''
represent a contact, it has 3 fileds: name, address, phone
'''
class Contact():

    all_contacts = []

    def __init__(self, name, address, phone):
        self.name = name
        self.address = address
        self.phone = phone
        Contact.all_contacts.append(self)

        # single thread experiment
        self.get_data_from_whitepages()

    def get_data_from_whitepages(self):
        # this is the main processing method, push it into thread a pool for parallel processing...


        # fill data by phone number
        if self.phone:

            url = urlparams(revese_phone_url_format, api_key=api_key_reverse_phone, phone=self.phone)
            print("request: {}".format(url))

            with urllib.request.urlopen(url) as response:
                rdata = json.loads(response.read())

                if rdata.get("belongs_to"):
                    #todo: check if it similar to the original data ... but what if it is not?
                    self.name = rdata["belongs_to"][0].get("name")
                if rdata.get("current_addresses"):
                    self.address = extractaddress(rdata["current_addresses"][0])

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

        with urllib.request.urlopen(url) as response:
            rdata = json.loads(response.read())

            if type(rdata.get("person")) is not list or not rdata["person"]:
                return

            person = rdata["person"][0]
            if person.get("name"):
                self.name = person["name"]
            if person.get("phones") and person["phones"][0].get("phone_number"):
                self.phone = person["phones"][0]["phone_number"]
            if person.get("found_at_address"):
                self.address = extractaddress(person["found_at_address"])


'''
a class reprezent the csv data
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

    def addRow(self, row):

        self.data.append((row,
                          Contact(
                              name=row[self.contact_filed_idx[0]],
                              address=row[self.contact_filed_idx[1]],
                              phone=row[self.contact_filed_idx[2]])
                         ))

    def write_to(self, f):
        '''
        write the current data into a stream in cvs txt
        :param f: iostream
        :return:
        '''
        f.write(",".join(['"'+f+'"' for f in self.field_list])+"\n")

        for data in self.data:

            row = list(data[0])

            contact = data[1]
            row[self.contact_filed_idx[0]] = contact.name
            row[self.contact_filed_idx[1]] = contact.address
            row[self.contact_filed_idx[2]] = contact.phone

            f.write(",".join(['"' + d + '"' for d in row]) + "\n")


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

    csv_form.write_to(output)


if __name__ == "__main__":

    with open("sample.txt", encoding="utf-8") as f:
        with open("result.csv", "w", encoding="utf-8") as r:
            process_csv(f, r)