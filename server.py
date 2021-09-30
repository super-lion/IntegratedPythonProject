#!/usr/bin/env python
# -*- coding: utf-8 -*-
from FrameworkImplementations.ManagerClass import ManagerClass

from flask import Flask, request
from urllib.parse import unquote
import json
import re
from datetime import datetime


app = Flask(__name__)
ManagerClassObj = ManagerClass()


def parse_request(req):
    """
    Parses application/json request body data into a Python dictionary
    """
    payload = req.get_data()
    payload = unquote(payload)
    payload = re.sub('payload=', '', payload)
    payload = json.loads(payload)

    return payload


@app.route('/api/place_trade', methods=['POST'])
def print_test():
    """
    Send a POST request to {environment}.tradebotinc.pagekite.me/api/place_trade with a JSON payload
    containing the necessary information to authenticate and execute the request
    """
    payload = request.get_json(request)
    ManagerClassObj.processPlaceTradeApiRequest(payload)
    return ("", 200, None)


if __name__ == '__main__' and ManagerClassObj.SystemVariablesObj['SystemState'] == 'Passive':
    app.run(debug=True, use_reloader=False)
