import pandas as pd
import numpy as np

    #Plot clusters
def plot_cluster(data, outliers):
    categories = {
        0: 'Isolated',
        1: 'Dense MC',
        2: 'Sparse MC',
        3: 'Dense Cluster',
        4: 'Sparse Cluster',
        5: 'Near cluster'
    }
    category_marker = {
        0: 'square',
        1: 'diamond',
        2: 'cross',
        3: 'x',
        4: 'pentagon',
        5: 'star'
        }
    category_color = {
        0: 'red',
        1: 'blue',
        2: 'green',
        3: 'yellow',
        4: 'orange',
        5: 'black'
        }
    #Plot the data
    import plotly
    labels = np.unique(outliers.label)
    to_plot = []
    inliers = data.merge(outliers, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only'].drop('_merge', axis='columns').reset_index(drop=True)
    trace = plotly.graph_objs.Scatter(x=inliers.x,
                                      y=inliers.y,
                                      mode='markers',
                                      name='Inliers'
                                    )
    
    to_plot.append(trace)
    for i in labels:
        trace_out = plotly.graph_objs.Scatter(x=outliers.loc[outliers['label'] == i].x,
                                              y=outliers.loc[outliers['label'] == i].y,
                                              mode='markers',
                                              name=categories[i],
                                              marker_symbol = category_marker[i],
                                              marker=dict(size=20,
                                                          color=category_color[i],
                                                         line=dict(
                                                             width=2
                                                             )
                                                         )
                                            )
        to_plot.append(trace_out)
    #Plot fig in plotly
    plotly.offline.plot({
    "data": to_plot,
    "layout": plotly.graph_objs.Layout(
        showlegend=True,
        height=700,
        width=1200,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            font=dict(
                size=20
                )
            ),
        yaxis = dict(tickfont = dict(size=20)),
        xaxis = dict(tickfont = dict(size=20))
    )
    })
    
#MAIN
type = "csv"
parameters = '0.15-5' #'0.25-5' or '0.15-5'
input_types = '3' #'1' or '2' or '3'
dataset_out = 'glass_outliers'
path_out = "data/glass/" + dataset_out + "_" + parameters + "_" + input_types + "." + type
dataset = 'glass_full'
path = "data/glass/" + dataset + "_" + parameters + "_" + input_types + "." + type
dtypes = {'x': 'float', 'y': 'float', 'label': 'int'}

data = pd.read_csv(path, names=['x','y','label'], dtype=dtypes, header=None)
data_out = pd.read_csv(path_out, names=['x','y','label'], dtype=dtypes, header=None)
plot_cluster(data, data_out)
    
    
    
    