import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pymongo
import pandas as pd
import io
import base64
import datetime

# MongoDB connection
try:
    client = pymongo.MongoClient("mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/<database-name>?retryWrites=true&w=majority")
    db = client["finance_metadata"]
except:
    print("Connection error")

static_info = db["stocks_daily"].find_one({})

# Extract required information
comp_name = static_info.get('Company Name', 'N/A')
market_cap = static_info.get('Market Cap', 'N/A')
beta_5yr = static_info.get('Beta (5Y Monthly)', 'N/A')
pe_ratio = static_info.get('PE Ratio (TTM)', 'N/A')
eps = static_info.get('EPS (TTM)', 'N/A')
earning_date = static_info.get('Earnings Date', 'N/A')
fwd_div = static_info.get('Forward Dividend & Yield', 'N/A')
ex_dividend_date = static_info.get('Ex-Dividend Date', 'N/A')
target_est = static_info.get('1y Target Est', 'N/A')

app = dash.Dash(__name__)

# Define the layout with styling
app.layout = html.Div([
    html.Div([html.Div([html.H1("Finance Data Analyzer", style={'height':'0px', 'text-align': 'center', 'margin-bottom': '20px', 'color': '#018049'})],style={'width':'40%'}),
              
            html.Div([
                html.Button('Download Data', id='download-button', n_clicks=0, style={'margin-top': '5px',  'font-size': '16px', 'background-color': '#4CAF50', 'border': 'none', 'color': 'white', 'text-align': 'center', 'text-decoration': 'none', 'display': 'inline-block', 'border-radius': '8px', 'cursor':'pointer'}),
                    dcc.Dropdown(id='download-format',
                                 options=[
                                     {'label': 'CSV', 'value': 'csv'},
                                     {'label': 'JSON', 'value': 'json'}
                                 ],
                                 value='csv',
                                 style={'width': '100px', 'margin-left':'10px', 'margin-top':'3px'})
            ], style={'display':'flex', 'width':'30%', 'text-align': 'left', 'margin-top':'15px'}),
        
        dcc.Download(id="download-data")

              ], style={'display':'flex', 'justify-content':'space-between'}),
    
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='stock-type-dropdown',
                options=[
                    {'label': 'Daily', 'value': 'daily'},
                    {'label': 'Weekly', 'value': 'weekly'},
                    {'label': 'Monthly', 'value': 'monthly'},
                    {'label': 'Yearly', 'value': 'yearly'}
                ],
                value='weekly',  # Default selected option
                style={'width': '60%', 'margin-bottom': '10px', 'margin-left': '20px'}
            ),
            dcc.Dropdown(
                id='symbol-dropdown',
                options=[
                    {'label': 'Apple Inc.', 'value': 'AAPL'},
                    {'label': 'IBM', 'value': 'IBM'},
                    {'label': 'Amazon', 'value': 'AMZN'},
                    {'label': 'Microsoft', 'value': 'MSFT'},
                    {'label': 'Tesla', 'value': 'TSLA'}
                    # Add more options as needed
                ],
                value='AAPL',  # Default selected stock symbol
                style={'width': '60%', 'margin-bottom': '10px'}
            ),
            dcc.Dropdown(
                id='timeframe-dropdown',
                options=[
                    {'label': 'Open', 'value': 'Open'},
                    {'label': 'Close', 'value': 'Close'},
                    {'label': 'High', 'value': 'High'},
                    {'label': 'Low', 'value': 'Low'},
                    {'label': 'Volume', 'value': 'Volume'}
                ],
                value=['High'],  # Default selected values
                multi=True,
                style={'width': '80%', 'margin-bottom': '20px'}
            )
        ], style={'display':'flex', 'justify-content': 'space-between', 'padding':'20px','margin-top':'50px'}),
        
        html.Div([
        html.Div([
            dcc.Graph(id='stock-graph', style={'height': '400px'}),
        ], style={'flex': '70%', 'margin-right': '20px'}),

        html.Div([
            html.Div(id = 'company-info', style={'margin-top': '20px'})
        ], style={'flex': '30%'})
    ], style={'display': 'flex', 'justify-content': 'space-between', 'margin-top': '20px'})

    ])
])

# Define callback to update graph
@app.callback(
    Output('stock-graph', 'figure'),
    [Input('stock-type-dropdown', 'value'),
     Input('symbol-dropdown', 'value'),
     Input('timeframe-dropdown', 'value')]
)
def update_graph(stock_type, symbol, values):
    collection_name = f"stocks_{stock_type}"
    
    # Fetch stock data from MongoDB
    query = {"Symbol": symbol}
    data = pd.DataFrame(list(db[collection_name].find(query, {'_id': 0})))
    
    data.set_index('Date', inplace=True)

    
    
    # Create traces for selected values
    traces = []
    for value in values:
        traces.append({
            'x': data.index,
            'y': data[value],
            'name': value.capitalize()  # Capitalize value for better display
        })
    
    return {
        'data': traces,
        'layout': {
            'title': f'{symbol} Stock Data ({stock_type.capitalize()})',
            'xaxis': {'title': 'Date', 'automargin': True},
            'yaxis': {'title': 'Value', 'automargin':True},
            'margin': {'l': 40, 'b': 40, 't': 40, 'r': 40},
            'hovermode': 'closest'
        }
    }

# Define callback to handle download
@app.callback(
    Output("download-data", "data"),
    [Input('download-button', 'n_clicks')],
    [State('stock-type-dropdown', 'value'),
     State('symbol-dropdown', 'value'),
     State('timeframe-dropdown', 'value'),
     State('download-format', 'value')],
    prevent_initial_call=True
)
def download_data(n_clicks, stock_type, symbol, values, download_format):
    if n_clicks > 0:
        collection_name = f"stocks_{stock_type}"
        
        # Fetch stock data from MongoDB
        query = {"Symbol": symbol}
        data = pd.DataFrame(list(db[collection_name].find(query, {'_id': 0})))
        data.set_index('Date', inplace=True)
        
        # Prepare data for download
        if download_format == 'csv':
            csv_string = data.to_csv(index=True, encoding='utf-8')
            file_content = csv_string
            file_extension = 'csv'
        elif download_format == 'json':
            json_string = data.to_json(orient='records')
            file_content = json_string
            file_extension = 'json'
        
        return dict(content=file_content, filename=f"{symbol}_{stock_type}_data.{file_extension}")
    
    return None

@app.callback(
    Output('company-info', 'children'),
    [Input('symbol-dropdown', 'value')]
)
def update_company_info(symbol):
    # Fetch static information from MongoDB based on selected symbol
    static_info = db["stocks_daily"].find_one({"Symbol": symbol})

    # Extract required information
    comp_name = static_info.get('Company Name', 'N/A')
    market_cap = static_info.get('Market Cap', 'N/A')
    beta_5yr = static_info.get('Beta (5Y Monthly)', 'N/A')
    pe_ratio = static_info.get('PE Ratio (TTM)', 'N/A')
    eps = static_info.get('EPS (TTM)', 'N/A')
    earning_date = static_info.get('Earnings Date', 'N/A')
    fwd_div = static_info.get('Forward Dividend & Yield', 'N/A')
    ex_dividend_date = static_info.get('Ex-Dividend Date', 'N/A')
    target_est = static_info.get('1y Target Est', 'N/A')

    # Construct HTML content for company information
    company_info_content = [
        html.H3('Company Information'),
        html.Table([
            html.Tr([html.Td(['Company Name']), html.Td(comp_name)]),
            html.Tr([html.Td(['Market Cap']), html.Td(market_cap)]),
            html.Tr([html.Td(['Beta (5Y Monthly)']), html.Td(beta_5yr)]),
            html.Tr([html.Td(['PE Ratio (TTM)']), html.Td(pe_ratio)]),
            html.Tr([html.Td(['EPS (TTM)']), html.Td(eps)]),
            html.Tr([html.Td(['Earnings Date']), html.Td(earning_date)]),
            html.Tr([html.Td(['Forward Dividend & Yield']), html.Td(fwd_div)]),
            html.Tr([html.Td(['Ex-Dividend Date']), html.Td(ex_dividend_date)]),
            html.Tr([html.Td(['1y Target Est']), html.Td(target_est)]),
        ])
    ]

    return company_info_content

if __name__ == '__main__':
    app.run_server(debug=True)
