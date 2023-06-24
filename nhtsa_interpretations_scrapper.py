from datetime import datetime
import hashlib
import requests
import simplejson as json
from bs4 import BeautifulSoup, NavigableString
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

ES = Elasticsearch(
    cloud_id="Sandbox:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNGE1NzQ5OWUwZDQ2NDJlZGI2ODI3N2U0ODQ1YjdkMjYkMzg4ZGE3Yzc0MDBkNDg5M2EyNDRlOTYyZGUxMDQ2N2U=",
    api_key="MkFFLWQ0Y0Itd3l4N0cyWnhpb3c6SndOVUtJZmxTeEd5NTJKb0JwVmRZQQ==",
    request_timeout=300,
    max_retries=1,
    retry_on_timeout=True
)


def generate_interpretations():
    baseurl = 'https://www.nhtsa.gov/'
    url = f'{baseurl}{"nhtsa-interpretation-file-search?items_per_page=50&page="}'
    for x in range(0, 100):
        r = requests.get(f"{url}{x}")

        soup = BeautifulSoup(r.content, 'html.parser')

        html_interpretations = soup.find_all('td', class_='views-field views-field-title views-field-body')

        for html_interpretation in html_interpretations:
            interpretation = {}
            cur_interpretation = ''
            for element in html_interpretation:
                if element.name == 'h4':
                    interpretation['interpretation_id'] = element.text[4:].strip()
                    continue
                elif element.name == 'a':
                    interpretation['url'] = f'{baseurl}{element.attrs["href"]}'
                    continue
                elif type(element) == NavigableString or element.name == 'br':
                    continue
                elif 'br' in list(element.children):
                    print("HEADER")
                    continue
                cur_interpretation += element.text.strip()
            interpretation['text'] = cur_interpretation
            interpretation['@timestamp'] = datetime.utcnow().strftime('%FT%T.%f')[:-3]+'Z'

            yield {
                    '_index': 'interpretations',
                    '_op_type': 'index',
                    '_id': hashlib.sha1(interpretation['interpretation_id'].encode()).hexdigest(),
                    '_source': json.dumps(interpretation)
                  }


if __name__ == '__main__':
    for success, info in streaming_bulk(client=ES, actions=generate_interpretations(), chunk_size=500):
        if not success:
            print('A document failed:', info)