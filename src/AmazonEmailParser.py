from __future__ import print_function

import json
import re

class AmazonEmailParser(object):

    def __init__(self):
        self.orderTotalRE = re.compile(r"(?<=Order Total:) (?:.*?)(\d+.\d+)")
        self.postageRE = re.compile(r"(?<=Postage & Packing:) (?:.*?)(\d+.\d+)")
        self.deliveryRE = re.compile(r"(?<=Delivery & Handling::) (?:.*?)(\d+.\d+)")
        self.orderItemsRE = re.compile(r"==========\r\n\r\n")
        self.costRE = re.compile(r"(\d+\.\d+)")

    def canParse(self, email):
        try:
            if 'auto-confirm@amazon' in email['from']:
                return True
            else:
                return False
        except:
            return False

    def parse(self, email):
        body = email['body']

        if 'Order Confirmation' in body:
            postage = 0
            orderTotal = 0

            result = re.search(self.orderTotalRE, body)

            if result:
                orderTotal = float(result.groups()[0])

            result = re.search(self.postageRE, body)

            if result:
                postage = float(result.groups()[0])
            else:
                result = re.search(self.deliveryRE, body)
                if result:
                    postage = float(result.groups()[0])

            email['order_details'] = {
                "order_items" : [],
                "order_total" : orderTotal,
                "postage" : postage,
                "merchant" : "amazon"
            }

            orders = re.split(self.orderItemsRE, body)[1]
            orders = orders.split('\r\n\r\n')

            #Remove first and last 3 items
            orders.pop(0)
            orders.pop()
            orders.pop()
            orders.pop()

            costTotal = orderTotal

            for item in orders:
                if 'Your estimated delivery date is:' in item or 'Your order will be sent to:' in item:
                    continue
                else:
                    lines = item.replace('_','').split('\r\n')
                    if len(lines) < 4:
                        continue
                    itemName = lines[0].strip()
                    cost = float(re.search(self.costRE, lines[1].strip()).groups()[0])
                    condition = lines[2].rpartition(':')[2].strip()
                    seller = lines[3].replace('Sold by', '').strip()

                email['order_details']['order_items'].append({"item":itemName, "cost":cost, "condition": condition, "seller": seller})
                costTotal -= cost

            if costTotal != 0:
                print("Warning order not parsed correctly, order items may be missing, or promotion may have been applied.")
                print(email['order_details'])
                print(body)

        return email
