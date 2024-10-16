from datetime import datetime, timedelta
from dash.dependencies import Input, Output, State
from dash import dcc, html
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

def register_callbacks(app, runner, data):
     
     @app.callback(
        Output('output-message-email', 'children'),
        [
          Input('send-email-button', 'n_clicks'),
          Input('input-string-email', 'value')
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
          
     
     @app.callback(
        Output('output-message-favorite', 'children'),
        [
          Input('save-favorite-button', 'n_clicks'),
          Input('input-estate_id', 'value'),
          Input('input-note', 'value')
        ]
    )
     
     def save_favorite(n_clicks, estate_id, notes):
        
          if n_clicks > 0:
               current_time = datetime.now()
               #TODO: i was thinking about this being loaded outside of callbacks,
               #todo but it cannot, we need to refresh it after each click
               favs_df = runner.data_manager.get_all_favorites("saved_estates")
               if estate_id in favs_df['estate_id'].unique():
                    return f"Estate_id '{estate_id}' already in favorites."
               else:
                    try:
                         runner.data_manager.insert_new_favorite(
                              "saved_estates", estate_id, notes, current_time
                         )
                         return f"New estate_id '{estate_id}' with note '{notes}' saved sucessfully."
               
                    except Exception as e:
                         return f"An error occurred while saving estate_id '{estate_id}': {e}"                 
          else:
               pass
          
     @app.callback(
          Output('favorites-table', 'data'),
          Input('input-note', 'value')
    )
     
     def display_favorites(value):
          #? this is fake input, i do not need it
          
          saved_favs = runner.data_manager.get_all_favorites("saved_estates")
          saved_estates_id = saved_favs['estate_id'].tolist()
          
          data = runner.data_manager.get_all_records(
               "price_history_new2",
               offer_ids=saved_estates_id
               )
          data = data.merge(saved_favs[['estate_id', 'notes']], on="estate_id",  how='left')

          data = data.sort_values(by='estate_id')
          
          return data.to_dict('records')
          
          
                    
