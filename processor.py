from xml.etree import ElementTree

from funcy import chunks
from joblib import Parallel, delayed
import requests

from config import Config


class CovidStatesGenerationProcess:
    @classmethod
    def get_response(cls, start_date, end_date):
        url = f'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19InfStateJson?'

        params = {
            'ServiceKey': Config.SERVICE_KEY,
            'startCreateDt': start_date,
            'endCreateDt': end_date
        }

        return requests.get(url, params).text

    @classmethod
    def generate_covid_state_items(cls, tree):
        covid_states_items = tree.find('body').find('items')

        for item in covid_states_items:
            yield item

    @classmethod
    def convert_covid_states(cls, item):
        covid_state = {
            'date': item.find('stateDt').text,
            'accum_confirmed_count': item.find('decideCnt').text,
            'accum_death_count': item.find('deathCnt').text,
        }

        return covid_state

    @classmethod
    def execute(cls, start_date, end_date):
        response = cls.get_response(start_date, end_date)

        tree = ElementTree.fromstring(response)
        covid_states = []

        with Parallel(n_jobs=8, backend='loky') as parallel:
            for items in chunks(8, cls.generate_covid_state_items(tree)):
                results = parallel(
                    delayed(cls.convert_covid_states)(item) for item in items)

                covid_states.extend(results)

        return covid_states
