from datetime import datetime, timedelta
from dash.dependencies import Input, Output, State
from dash import dcc, html
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

def register_callbacks(app, runner, data):
     
     @app.callback(
        Output('output-message', 'children'),
        [
          Input('send-email-button', 'n_clicks'),
          Input('input-string', 'value')
        ]
    )
     
     def send_email(n_clicks, text_email):
        
          if n_clicks > 0:
               current_time = datetime.now()
               try:
                    runner.mailing.send_email(subject=f"testing_{current_time}", message_text=f"pokus: {text_email}")
                    return "Sent sucessfully"
               except Exception as e:
                    return f"An error occurred: {e}"
                    
          else:
               pass
                    
