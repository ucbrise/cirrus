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


# Grab memory usage by process
import os
import psutil
process = psutil.Process(os.getpid())

app = dash.Dash(__name__)
app.css.append_css({
        "external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"
})
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

        dcc.Interval(id='logloss-update', interval=1000, n_intervals=0)
    ], style={'width':'80%', 'display': 'inline-block', 'vertical-align':'middle'})

app.layout = html.Div([

    html.Div([
        html.P("holder", id='placeholder'),
        html.H1(
            'Cirrus Training Viewer',
            id='title'
        )

    ],
        className="banner"
    ),


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
                    {'label': 'Show Top Ten', 'value': 'top_ten'},
                    {'label': 'Show Last Ten', 'value': 'last_ten'}
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

cost = 0
def get_cost():
    global cost
    cost += random.random()
    return cost

def get_num_lambdas():
    return "Not implemented"

def get_mem_usage():
    global process
    return process.memory_info().rss / 1000000

def get_traces(num):
    trace_lst = []
    if num == 0:
        # Get all
        for i in range(get_num_experiments()):
            trace = Scatter(
                x=get_xs_for(i),
                y=get_ys_for(i),
                name=get_name_for(i),
                mode='markers+lines'
            )
            trace_lst.append(trace)
    else:
        # Get top N
        q = []
        for i in range(get_num_experiments()):

            xs = get_xs_for(i)
            ys = get_ys_for(i)
            trace = Scatter(
                x=xs,
                y=ys,
                name=get_name_for(i),
                mode='markers+lines'
            )
            q.append((ys[-1], trace))
        q.sort(reverse=(num > 0))
        trace_lst = [item[1] for item in q[:num]]
    return trace_lst


bundle = []

def get_num_experiments():
    return len(bundle)

def get_xs_for(i):
    item = bundle[i]
    return [tl[0] for tl in item.get_time_loss()]

def get_ys_for(i):
    item = bundle[i]
    return [tl[1] for tl in item.get_time_loss()]

def get_name_for(i):
    item = bundle[i]
    return item.get_name()

def kill(i):
    bundle[i].kill()




# Callbacks

# Kill and Info logic
@app.callback(
    Output('kill-all-button', 'children'),
    [Input('kill-all-button', 'n_clicks')]
)
def killall_clicked(n_clicks):
    print "killall triggered!"
    if (n_clicks > 0):
        for i in range(get_num_experiments()):
            kill(i)
        return "Kill All"
    return "Kill All"

@app.callback(Output('kill-button', 'style'), [Input('data-panel', 'children')])
def show_kill_button(child):
    if "Nothing" in child:
        return {'display': 'none'}
    return {'display': 'block'}

@app.callback(Output('kill-button', 'children'), [Input('data-panel', 'children')])
def set_kill_button_text(child):
    print child
    if not "Nothing" in child:
        num = child.split(" ")[3]
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
    print selected_points

    if kill_button_ts == None:
        kill_button_ts = 0
    last_kill_time = (time.time() * 1000.0) - kill_button_ts
    print(last_kill_time)
    # HACK: To see if we selected something or killed something, we check to seek
    # when the last increment occured.
    if  last_kill_time > 100:
        print "Last action was a select"
        cnum = int(selected_points["points"][0]["curveNumber"])
        return 'Selected curve number: %d' % cnum
    else:
        print "Last action was a kill click"
        cnum = int(current_info.split(" ")[3])
        print "Killing line %d" % cnum
        kill(cnum)
        return "Nothing selected!"



# Update functions

# FIXME: Combine these updates into 1 callback to 1 output.
# Update the memory term
@app.callback(
    Output('tmem', 'children'),
    [Input('logloss-update', 'n_intervals')])
def gen_cost(interval):
    child = "Mem Usage: %d Mbs" % get_mem_usage()
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
    child = "Cost: $%0.2f" % get_cost()
    return child

# FIXME: Need a more sophisticated way to zoom into the graph.
@app.callback(Output('logloss', 'figure'),
    [
    Input('logloss-update', 'n_intervals'),
    ],
    [
    State('showmenu', 'value'),
    State('logloss', 'figure'),
    State('logloss', 'relayoutData'),
    State('mapControls', 'values')])
def gen_loss(interval, menu, oldfig, relayoutData, lockCamera):
    if menu=="top_ten":
        how_many = 2
    elif menu == 'last_ten':
        how_many = -2
    else:
        how_many = 0


    trace_lst = get_traces(how_many)

    print menu
    if 'lock' in lockCamera:
        return oldfig
        r1 = None
        r2 = None
        if 'xaxis.range[0]' in relayoutData.keys():
            r1 = [relayoutData['xaxis.range[0]'], relayoutData['xaxis.range[1]']]
        if 'yaxis.range[0]' in relayoutData.keys():
            r2 = [relayoutData['yaxis.range[0]'], relayoutData['yaxis.range[1]']]
        layout = Layout(
            height=450,
            xaxis=dict(
                title='Time Elapsed (sec)',
                range=r1
            ),
            yaxis=dict(
                title="Loss",
                range=r2
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

if __name__ == '__main__':
    app.run_server(debug=True)
