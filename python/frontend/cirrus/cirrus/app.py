# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
from plotly import tools
from plotly.graph_objs import *

import json
import random
import time
import CirrusBundle


# Grab memory usage by process
import os
import psutil
process = psutil.Process(os.getpid())



app = dash.Dash(__name__)
app.css.append_css({
        "external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"
})

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

server = app.server

# Layout functions

def div_graph(name):
    return html.Div([


        dcc.Checklist(
            id='mapControls',
            options=[
                {'label': 'Lock Camera', 'value': 'lock'}
            ],
            values=[],
            labelClassName="mapControls",
            inputStyle={"z-index": "3"}
        ),

        dcc.Graph(
            id='logloss',
        ),

        dcc.Interval(id='logloss-update', interval=2000, n_intervals=0)
    ], style={'width':'80%', 'display': 'inline-block', 'vertical-align':'middle'})

app.layout = html.Div([



    html.Div([
        div_graph("test"),
        html.Div([
            html.P(
                'Cost ($):',
                id='cost'
            ),
            html.P(
                'Num. Lambdas: ',
                id='nlambdas'
            ),
            html.P(
                'Total Mem.: ',
                id='tmem'
            ),
            dcc.RadioItems(
                id="showmenu",
                options=[
                    {'label': 'Show All', 'value': 'all'},
                    {'label': 'Show Top Five', 'value': 'top_ten'},
                    {'label': 'Show Last Five', 'value': 'last_ten'}
                ],
                value='all'
            ),
            html.Div(id='data-panel',
                children='Click on a point and then press kill'),
            html.Button('Kill', id='kill-button'),
            html.Button('Kill All', id='kill-all-button')


        ], style={'width':'20%', 'display': 'inline-block', 'vertical-align':'middle'})

    ],
    className="container"
    )

])


# helper functions

def get_cost_per_second():
    return sum([item.cost_per_second for item in bundle.cirrus_objs])

def get_cost():
    return sum([item.total_cost for item in bundle.cirrus_objs])

def get_num_lambdas():
    return str(sum([item.get_num_lambdas() for item in bundle.cirrus_objs]))

def get_mem_usage():
    global process
    return process.memory_info().rss / 1000000

def test():
    r = lambda: random.randint(0,255)
    return 'rgb(%d, %d, %d)' % (r(),r(),r())

def get_traces(num):
    trace_lst = []
    if num == 0:
        # Get all
        for i in range(get_num_experiments()):
            xs = get_xs_for(i)
            lll = len(xs)
            trace = Scatter(
                x=xs,
                y=get_ys_for(i),
                name=get_name_for(i),
                mode='markers+lines',
                line = dict(color = bundle.get_info(i, 'color')),
                customdata =str(i) * lll
            )
            trace_lst.append(trace)
    else:
        # Get top N
        q = []

        for i in range(get_num_experiments()):

            xs = get_xs_for(i)
            ys = get_ys_for(i)
            lll = len(ys)
            trace = Scatter(
                x=xs,
                y=ys,
                name=get_name_for(i),
                mode='markers+lines',
                line = dict(color = (bundle.get_info(i, 'color'))),
                customdata= str(i) * lll
            )
            q.append((ys[-1], trace))
        q.sort(reverse=(num > 0))
        trace_lst = [item[1] for item in q[:abs(num)]]
    return trace_lst


bundle = None

dead_lst = []
frozen_lstx = {}
frozen_lsty = {}

def get_num_experiments():
    return bundle.get_number_experiments()

def get_xs_for(i):
    if i in dead_lst:
        return []

    item = bundle.cirrus_objs[i]
    out = item.get_time_loss()
    #print(out)
    frozen_lstx[i] = [tl[0] for tl in out]
    return frozen_lstx[i]

def get_ys_for(i):
    if i in dead_lst:
        return []
    item = bundle.cirrus_objs[i]
    frozen_lsty[i] = [tl[1] for tl in item.get_time_loss()]
    return frozen_lsty[i]
def get_name_for(i):
    item = bundle.cirrus_objs[i]
    return item.get_name()

def kill(i):
    dead_lst.append(i)
    bundle.kill(i)




# Callbacks

# Kill and Info logic
@app.callback(
    Output('kill-all-button', 'children'),
    [Input('kill-all-button', 'n_clicks')]
)
def killall_clicked(n_clicks):
    if (n_clicks > 0):
        for i in range(get_num_experiments()):
            bundle.kill(i)
        return "Kill All"
    return "Kill All"

@app.callback(Output('kill-button', 'style'), [Input('data-panel', 'children')])
def show_kill_button(child):
    if "Nothing" in child:
        return {'display': 'none'}
    return {'display': 'block'}

@app.callback(Output('kill-button', 'children'), [Input('data-panel', 'children')])
def set_kill_button_text(child):
    if not "Nothing" in child:
        num = child.split(" ")[2]
        return "Kill line: %s" % num
    return "Nope"

@app.callback(
    Output('data-panel', 'children'),
    [Input('logloss', 'clickData'),
     Input('kill-button', 'n_clicks_timestamp')],
    [State('data-panel', 'children')]
    #[Event('')]
)
def select_or_kill(selected_points, kill_button_ts, current_info):
    if selected_points == None:
        return "Nothing selected!"

    if kill_button_ts == None:
        kill_button_ts = 0
    last_kill_time = (time.time() * 1000.0) - kill_button_ts
    # HACK: To see if we selected something or killed something, we check to seek
    # when the last increment occured.
    if  last_kill_time > 500:
        #print(selected_points["points"][0])
        cnum = int(selected_points["points"][0]["customdata"])
        return 'Chose line: %d' % cnum
    else:
        print("Killing line")
        cnum = int(current_info.split(" ")[2])
        kill(cnum)
        return "Nothing selected!"



# Update functions

# FIXME: Combine these updates into 1 callback to 1 output.
# Update the memory term
@app.callback(
    Output('tmem', 'children'),
    [Input('logloss-update', 'n_intervals')])
def gen_cost(interval):
    child = "Mem Usage: %d MBs" % get_mem_usage()
    return child

# Update the cost term
@app.callback(
    Output('nlambdas', 'children'),
    [Input('logloss-update', 'n_intervals')])
def gen_cost(interval):
    child = "Num Lambdas: %s" % get_num_lambdas()
    return child

# Update the cost term
@app.callback(
    Output('cost', 'children'),
    [Input('logloss-update', 'n_intervals')])
def gen_cost(interval):
    child = "Current Cost: $%0.2f \n($%0.5f/sec)" % (get_cost(), get_cost_per_second())
    return child

# FIXME: Need a more sophisticated way to zoom into the graph.
@app.callback(Output('logloss', 'figure'),
    [
    Input('logloss-update', 'n_intervals'),
    Input('showmenu', 'value')
    ],
    [

    State('logloss', 'figure'),
    State('logloss', 'relayoutData'),
    State('mapControls', 'values')])
def gen_loss(interval, menu, oldfig, relayoutData, lockCamera):
    if menu=="top_ten":
        how_many = 5
    elif menu == 'last_ten':
        how_many = -5
    else:
        how_many = 0


    trace_lst = get_traces(how_many)

    if 'lock' in lockCamera:
        return oldfig
        r1 = None
        r2 = None
        if 'xaxis.range[0]' in relayoutData.keys():
            r1 = [relayoutData['xaxis.range[0]'], relayoutData['xaxis.range[1]']]
        if 'yaxis.range[0]' in relayoutData.keys():
            r2 = [relayoutData['yaxis.range[0]'], relayoutData['yaxis.range[1]']]
        layout = Layout(
            height=800,
            width=800,
            xaxis=dict(
                title='Time Elapsed (sec)',
                range=r1
            ),
            yaxis=dict(
                title="Loss",
                range=r2,

            ),
            margin=Margin(
                t=45,
                l=50,
                r=50
            ),
            showlegend=False

        )
    else:
        layout = Layout(
            height=450,
            xaxis=dict(
                title='Time Elapsed (sec)'
            ),
            yaxis=dict(
                title="Loss"

            ),
            margin=Margin(
                t=45,
                l=50,
                r=50
            ),
            showlegend=False,
            hovermode='closest'
        )


    return Figure(data=trace_lst, layout=layout)

import threading
def run_server():
    def runner():
        app.run_server(debug=False)
    t = threading.Thread(target=runner)
    t.start()

from IPython.display import IFrame
def display_dash():
    return IFrame('http://localhost:8050', width=1000, height=600)
