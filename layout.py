from dash import dcc, html
import datetime as dt
from dash import dash_table


def create_layout():
    layout = html.Div([
        dcc.Location(id='url', refresh=False),

        html.Div([
            html.Div([                
                html.Img(
                    src="https://www.sreality.cz/img/logo-sreality.svg",
                    style={
                        'height': '40px',
                        'margin-right': '15px',
                        'vertical-align': 'middle'
                    }
                ),
                dcc.Link('Scraping', href='/scraping', className='nav-link'),
                html.Span(" | ", className='separator'),
                dcc.Link('Statistics', href='/statistics', className='nav-link'),
                html.Span(" | ", className='separator'),
                dcc.Link('Favorite', href='/favorite', className='nav-link'),
                html.Span(" | ", className='separator'),
                dcc.Link('Discounts', href='/discounts', className='nav-link'),
            ], style={
                'display': 'flex',
                'align-items': 'center',
                'background-color': '#000000',  # Dark blue background
                'padding': '10px 20px'
            }),
        ]),

        html.Div(id='page-content')
    ])

    return layout

def create_scraping_layout():
    layout = html.Div([
        
        html.H1("Scraping"),
    ])
    return layout

def create_statistics_layout():
    layout = html.Div([
        
        html.H1("Statistics"),
    ])
    return layout

def create_favorite_layout():
    layout = html.Div([
        
        html.H1("Favorite"),
    ])
    return layout

def create_discounts_layout():
    layout = html.Div([
        
        html.H1("Discounts"),
        
        dcc.Input(id='input-string', type='text', placeholder='Enter a string...'),
        
        html.Div([
            html.Button("Send mail", 
                        id='send-email-button', 
                        n_clicks=0,
                        style={
                            'padding': '10px 20px',       
                            'fontSize': '18px',           
                            'backgroundColor': '#007BFF', 
                            'color': 'white',             
                            'border': 'none',             
                            'borderRadius': '8px',        
                            'cursor': 'pointer',          
                            'boxShadow': '0px 4px 8px rgba(0, 0, 0, 0.2)',  
                            'transition': 'background-color 0.3s ease' 
                        },
            )],
        style={'margin-top': '10px'}
        ),
        
        html.Div(id='output-message', 
                style={'margin-top': '20px'}),
        
    ])
    return layout