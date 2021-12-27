from flask import Flask

from bot import bot_cli


def create_app():
    app = Flask(__name__)

    app.cli.add_command(bot_cli)