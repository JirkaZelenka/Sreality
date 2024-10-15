import os
import warnings

from layout import create_layout, create_scraping_layout, create_statistics_layout, create_favorite_layout, create_discounts_layout
from callbacks import register_callbacks
import dash
import dash_auth

from run import Runner

from dotenv import load_dotenv
load_dotenv()
username = os.getenv("APP_USERNAME")
password = os.getenv("APP_PASSWORD")

VALID_USERNAME_PASSWORD_PAIRS = {
    username: password
    }

runner = Runner()

data = runner.data_manager.get_all_records("price_history_new2")
print(len(data))

app = dash.Dash(__name__, suppress_callback_exceptions=True)
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)

app.layout = create_layout()

@app.callback(
    dash.dependencies.Output('page-content', 'children'),
    [dash.dependencies.Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/scraping':
        return create_scraping_layout()
    elif pathname == '/statistics':
        return create_statistics_layout()
    elif pathname == '/favorite':
        return create_favorite_layout()
    elif pathname == '/discounts':
        return create_discounts_layout()

register_callbacks(app,runner,data)

if __name__ == '__main__':
    app.run_server(debug=True, host='127.0.0.1', port=8051)
