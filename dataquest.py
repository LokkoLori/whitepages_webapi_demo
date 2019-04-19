import csv
from fuzzywuzzy import fuzz
from io import StringIO
import urllib.parse
import json


# hard coded api keys for first experiments
api_key_reverse_phone = '7f365d38fb47449a985ccc972e7818aa'
api_key_find_person = '44c5b314db5b4dd0a9a7c2454fbbdd02'

revese_phone_url_format = "https://proapi.whitepages.com/3.0/phone.json?api_key={}&phone={}"


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

        # fill data by phone number
        if self.phone:
            url = revese_phone_url_format.format(urllib.parse.quote(api_key_reverse_phone), urllib.parse.quote(self.phone))

            print("request: {}".format(url))
            with urllib.request.urlopen(url) as response:
                rdata = json.loads(response.read())

                if rdata.get("belongs_to"):
                    self.name = rdata["belongs_to"][0].get("name")
                if rdata.get("current_addresses"):
                    caddr = rdata["current_addresses"][0]

                    addrd = [
                        caddr["street_line_1"],
                        caddr["street_line_2"],
                        caddr["city"],
                        caddr["state_code"] + ' ' + caddr["postal_code"]
                    ]
                    addrd = [ad for ad in addrd if ad and ad != "None"]
                    self.address = ", ".join(addrd)

            return

        #fill data by name and address, find person api
        pass



class CSV_form():


    def __init__(self, field_list):
        self.filed_list = field_list
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

        self.data.append((list(row),
                          Contact(
                              name=row[self.contact_filed_idx[0]],
                              address=row[self.contact_filed_idx[1]],
                              phone=row[self.contact_filed_idx[2]])
                         ))


def process_csv(f):

    reader = csv.reader(f, delimiter=',')

    first_row = True
    for row in reader:
        if first_row:
            csv_form = CSV_form(row)
            first_row = False
        else:
            csv_form.addRow(row)

    pass


def process_csv_text(txt):

    StringIO(txt)

if __name__ == "__main__":

    with open("sample.txt", encoding="utf-8") as f:
        process_csv(f)