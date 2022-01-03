import datetime
import re
from dataclasses import dataclass

import pandas as pd

from processor import CovidStatesGenerationProcess


def to_datetime(date):
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])

    return datetime.date(year, month, day)


def validate_date(date):
    date_format = re.compile('[0-9]{8}')

    if date_format.fullmatch is None:
        return False

    try:
        to_datetime(date)
    except Exception:
        return False

    return True


@dataclass
class ColumnsNames:
    accumulative: str
    one_day_ago: str
    specific_date: str


class CovidState:
    def __init__(self, start_date=None, end_date=None):
        self.start_date = start_date
        self.end_date = end_date

        if self.start_date is None:
            self.start_date = '20200120'

        if self.end_date is None:
            self.end_date = datetime.date.today().strftime("%Y%m%d")

    @property
    def covid_state(self):
        return CovidStatesGenerationProcess.execute(self.start_date, self.end_date)

    def _get_columns_names(self, state):
        return ColumnsNames(accumulative=f'accum_{state}_count',
                            one_day_ago=f'one_day_ago_accum_{state}_count',
                            specific_date=f'{state}_count')

    def _add_state_by_date(self, df, columns_names):
        accumulative = columns_names.accumulative
        one_day_ago = columns_names.one_day_ago
        specific_date = columns_names.specific_date

        df[accumulative] = df[accumulative].astype("int")
        df[one_day_ago] = df[accumulative].shift(+1).fillna(0).astype("int")
        df[specific_date] = df.apply(lambda x: x[accumulative] - x[one_day_ago], axis=1)
        df.drop([one_day_ago], axis=1, inplace=True)

    def get_covid_state_by_date(self):
        covid_state = pd.DataFrame(self.covid_state)

        covid_state['date'] = covid_state['date'].apply(lambda x: to_datetime(x))
        covid_state.sort_values(by=['date'], ascending=True, inplace=True)

        confirmed_columns_names = self._get_columns_names(state='confirmed')
        death_columns_names = self._get_columns_names(state='death')

        self._add_state_by_date(covid_state, confirmed_columns_names)
        self._add_state_by_date(covid_state, death_columns_names)

        covid_state.reset_index(drop=True, inplace=True)
        return covid_state