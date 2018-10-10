# -*- coding: utf-8 -*-
import logging
import os
import random
import time

import dash
import dash_core_components as dcc
import dash_html_components as html
import psutil
from IPython.display import IFrame
from dash.dependencies import Input, Output, State
from plotly.graph_objs import *

from core import BaseTask

process = psutil.Process(os.getpid())

app = dash.Dash(__name__)
app.css.append_css({
        "external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"
})

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

server = app.server


# Layout functions
def div_graph(name):
    return html.Div([

        dcc.Dropdown(
            id='graph-type',
            options=[
                {'label': 'Loss vs. Time', 'value': BaseTask.LOSS_VS_TIME},
                {'label': 'Updates/Second', 'value': BaseTask.UPDATES_PER_SECOND},
                {'label': 'Loss vs. Cost', 'value': BaseTask.TOTAL_LOSS_VS_TIME}
            ],
            value=BaseTask.LOSS_VS_TIME
        ),

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
                    {'label': 'Show Best Five', 'value': 'top_ten'},
                    {'label': 'Show Worst Five', 'value': 'last_ten'}
                ],
                value='all'
            ),
            html.P(id='data-panel',
                children='Click on a point and then press kill', style={"white-space": "pre-line"}),
            html.Button('Kill', id='kill-button'),
            html.Button('Kill All', id='kill-all-button')


        ], style={'width':'20%', 'display': 'inline-block', 'vertical-align':'middle'})

    ],
    className="container"
    )

])


# helper functions

def get_cost_per_second():
    out = bundle.get_cost_per_sec()
    return out

def get_cost():
    return bundle.get_cost()

def get_num_lambdas():
    return bundle.get_num_lambdas()

def get_mem_usage():
    global process
    return process.memory_info().rss / 1000000

def get_traces(num, metric=BaseTask.LOSS_VS_TIME):
    trace_lst = []
    if num == 0:
        # Get all
        for i in range(get_num_experiments()):
            xs = get_xs_for(i, metric)
            lll = len(xs)
            trace = Scatter(
                x=xs,
                y=get_ys_for(i, metric),
                name=get_name_for(i),
                mode='markers+lines',
                line = dict(color = bundle.get_info(i, 'color')),
                customdata = [str(i)] * lll
            )
            trace_lst.append(trace)
    else:
        # Get top N
        q = []

        for i in range(get_num_experiments()):

            xs = get_xs_for(i, metric)
            ys = get_ys_for(i, metric)
            lll = len(ys)
            trace = Scatter(
                x=xs,
                y=ys,
                name=get_name_for(i),
                mode='markers+lines',
                line = dict(color = (bundle.get_info(i, 'color'))),
                customdata= [str(i)] * lll
            )
            if (len(ys) > 0):
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


def get_xs_for(i, metric=BaseTask.LOSS_VS_TIME):
    return bundle.get_xs_for(i, metric)


def get_ys_for(i, metric=BaseTask.LOSS_VS_TIME):
    return bundle.get_ys_for(i, metric)


def get_name_for(i):
    out = bundle.get_name_for(i)
    return out


def kill(i):
    bundle.kill(i)


def get_info_for(i):
    return bundle.get_info_for(i)

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
)
def select_or_kill(selected_points, kill_button_ts, current_info):
    if selected_points == None:
        return "Nothing selected!"

    if kill_button_ts == None:
        kill_button_ts = 0
    last_kill_time = (time.time() * 1000.0) - kill_button_ts

    if last_kill_time > 500:
        #print selected_points
        cnum = int(selected_points["points"][0]["customdata"])
        string = 'Chose line: %d \n%s' % (cnum,  get_info_for(cnum))
        return string
    else:
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
        Input('showmenu', 'value'),
        Input('graph-type', 'value')
    ],
    [
        State('logloss', 'figure'),
        State('logloss', 'relayoutData'),
        State('mapControls', 'values')
    ])
def gen_loss(interval, menu, graph_type, oldfig, relayoutData, lockCamera):

    if menu == "top_ten":
        if graph_type == "LOSS":
            how_many = -5
        elif graph_type == "CPS":
            how_many = -5
        elif graph_type == "UPS":
            how_many = 5
    elif menu == 'last_ten':
        if graph_type == "LOSS":
            how_many = 5
        elif graph_type == "CPS":
            how_many = 5
        elif graph_type == "UPS":
            how_many = -5
    else:
        how_many = 0

    trace_lst = get_traces(how_many, metric=graph_type)

    graph_names = {
            BaseTask.LOSS_VS_TIME : "Loss", 
            BaseTask.UPDATES_PER_SECOND : "Updates/Second", 
            BaseTask.TOTAL_LOSS_VS_TIME : "Loss/Cost"}

    if 'lock' in lockCamera:
        return oldfig
    else:
        layout = Layout(
            height=450,
            xaxis=dict(
                title='Time Elapsed (sec)'
            ),
            yaxis=dict(
                title=graph_names[graph_type]

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


def display_dash():
    return IFrame('http://localhost:8050', width=1000, height=600)
