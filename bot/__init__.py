import click

from utils import CovidState


@click.group(name='bot')
def bot_cli():
    pass


@bot_cli.command('write_covid_state')
@click.option('--start_date', required=True)
@click.option('--end_date', required=True)
@click.option('--output_root', required=True)
def write_covid_state(start_date, end_date, channel_id=None, request_id=None):
    covid_stat_by_date = CovidState(start_date, end_date).get_covid_state_by_date()
