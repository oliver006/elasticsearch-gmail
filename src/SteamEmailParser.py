from __future__ import print_function

import json
import re

class SteamEmailParser(object):

    def __init__(self):
        self.orderTotalRE = re.compile(r"(?<=Total:)[ \t]+(\d+.\d+)")
        self.orderItemsRE = re.compile(r"(?:\.\r\n)+")
        self.costRE = re.compile(r"(\d+\.\d+)")

    def canParse(self, email):
        try:
            if 'noreply@steampowered.com' in email['from']:
                return True
            else:
                return False
        except:
            return False

    def parse(self, email):
        body = email['body']

        if 'Thank you' in email['subject'] and 'purchase' in body:
            orderTotal = 0

            result = re.search(self.orderTotalRE, body)

            if result:
                orderTotal = float(result.groups()[0])

            email['order_details'] = {
                "order_items" : [],
                "order_total" : orderTotal,
                "merchant" : "steam"
            }

            order = re.split(self.orderItemsRE, body)[2].split('\r\n') #This parser to get order total is currently broken, gift purchases are not parsed

            costTotal = orderTotal

            costTotal = orderTotal

            for item in order:
                if '-------' in item:
                    break
                else:
                    if item == '' or ': ' not in item:
                        continue
                    splitResult = item.rpartition(':')
                    itemName = splitResult[0].strip()
                    cost = float(re.match(self.costRE, splitResult[2].strip()).groups()[0])

                email['order_details']['order_items'].append({"item":itemName, "cost":cost})
                costTotal -= cost

            if costTotal != 0:
                print("Warning order not parsed correctly, order items may be missing, or promotion may have been applied.")
                print(email['order_details'])
                print(body)

        return email
