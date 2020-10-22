import requests
import json


def api_get_request(url):
    response = requests.get(url)
    response.raise_for_status()
    text = json.loads(response.text)
    return text
