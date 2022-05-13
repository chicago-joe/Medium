import pandas as pd
import numpy as np
import sys
import os
import pyodbc
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.express as px


def StackBarPlot(
  df, 
  orderdf, 
  oFldName, 
  FldName, 
  title, 
  oArray, 
  iColors):
    
    Order = orderdf.sort_values('cn', ascending = False)[oFldName]
    data = []
    c = 0
    for i in Order:
        _df = df[df[oFldName] == i]
        trace = go.Bar(x = _df['qtr'], 
                       y = _df['cn'],
                       name = _df[FldName].unique()[0], 
                       marker_color=iColors[c])
        data.append(trace)
        c += 1
        
    layout = dict(title = title,
              barmode='stack',
              xaxis = dict( categoryorder = 'array',
                            categoryarray = oArray,
                            zeroline=False, 
                            showline=True, 
                            showgrid=False),
              yaxis = dict( zeroline=False, 
                            showline=True, 
                            showgrid=False))
              
    fig = go.Figure(data=data, layout=layout)
    
    return fig

def bwPlot(
  df,
  xName,
  yName,
  CategoryName,
  title,
  iColors):
    
    layout = dict(
      title = title,
      annotations=[])
    data = []
    c = 0
    for i in df[CategoryName].unique():
        _df = df[df[CategoryName] == i]
        trace = go.Box(x = _df[xName], 
                       y = _df[yName],
                       name = i, 
                       boxpoints=False,
                       marker_color=iColors[c])
        data.append(trace)

        layout['annotations'].append(
          go.layout.Annotation(
            x=i,
            y=df[yName].min()-.2,
            xref="x",
            yref="y",
            text="n = %s" % '{:,.0f}'.format(_df['cn'].sum()),
            showarrow=False
        )
          )
        c += 1


    fig = go.Figure(data=data, layout=layout)

    return fig