import plotly.plotly as py
import plotly.graph_objs as go
import plotly

plotly.tools.set_credentials_file(username='kuracpalac123', api_key='IOGUoMy2waRhdxQ1xWAg')


trace1 = go.Scatter(x=[1,2,3],y=[1,2,3])
trace2 = go.Scatter(x=[1,2,3],y=[1,2,3])


plot_url = py.plot([trace1,trace2])


