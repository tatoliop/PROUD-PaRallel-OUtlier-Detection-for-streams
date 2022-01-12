# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import h5py
import plotly
import plotly.graph_objs
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
    
#Plot dataset
def plot_data(data, dataset):
    #Plot the data
    import plotly
    import plotly.graph_objs
    
    tmpPlot = plotly.graph_objs.Scatter(x=data.x, y=data.y, mode='markers')
    #Plot fig in plotly
    plotly.offline.plot({
    "data": [tmpPlot],
    "layout": plotly.graph_objs.Layout(showlegend=False,
        height=700,
        width=1200
    )
    })
    
    #Plot clusters
def plot_cluster(data):
    #Plot the data
    import plotly
    u_labels = np.unique(data.label)
    to_plot=[]
    for i in u_labels:
        tmp = data.loc[data['label'] == i]
        trace = plotly.graph_objs.Scatter(x=tmp.x, y=tmp.y, mode='markers', name=str(i))
        to_plot.append(trace)
    #Plot fig in plotly
    plotly.offline.plot({
    "data": to_plot,
    "layout": plotly.graph_objs.Layout(showlegend=True,
        height=700,
        width=1200
    )
    })
      
#MAIN
type = "csv"
#Hardcoded dataset name
dataset = 'ground'
path = "data/raw/" + dataset + "." + type
counter = 0

if(dataset == 'smtp' or dataset == 'fc_3d_norm' or dataset == 'tao' or dataset =='html'):
    colnames = ['x', 'y', 'z']
    dtypes = {'x': 'float', 'y': 'float', 'z': 'float'}
else:
    colnames = ['x', 'y']
    dtypes = {'x': 'float', 'y': 'float'}

if(type == 'mat'):
    with h5py.File(path , 'r') as f:
        print(len(f['X'][0]))
        listX = f['X'][0]
        listY = f['X'][1]
        listZ = f['X'][2]
        data = pd.DataFrame({'x': listX,'y': listY,'z': listZ})
elif(type == 'csv'):
    data = pd.read_csv(path, names=colnames, dtype=dtypes, header=0)
    
if(counter > 0):
    data = data.head(counter)
    
if(dataset == 'glass_dode' or dataset == 'glass_dode_outliers'):    
    data = pd.read_csv(path, names=['x','y','label'], dtype=dtypes, header=0)
    plot_cluster(data)
        
#Plot data
plot_data(data, dataset)


    
