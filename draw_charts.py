import plotly
import plotly.graph_objs as go
import plotly.express as px
from datetime import date
from collections import defaultdict


def reformat_data(data):
	''' Combines complaints that were made on the same day
	
	Keyword arguments:
	data - [(class<datetime.datetime>,int), ... ]

	return ([class<datetime.date>, ...],[ int, ... ])
	'''

	dict_ = defaultdict(int)
	for date_, count_ in data:
		date_format = date(date_.year,date_.month, date_.day)
		dict_[date_format] += count_
	return list(dict_.keys()), list(dict_.values())

def draw_chart_new_and_changed(new_data, changed_data):
	''' Draws a chart for new and changed complaints for each day
	
	new date and changed date have the following format:
	[(class<datetime.datetime>,int), ... ]
	'''

	x_new, y_new = reformat_data(new_data)
	x_changed, y_changed = reformat_data(changed_data)
	fig = go.Figure()
	fig.add_trace(go.Bar(x = x_changed, y = y_changed, name='Corrections'))
	fig.add_trace(go.Bar(x = x_new, y = y_new, name='Additions'))
	fig.show() 




def draw_chart_number_of_complaints_for_companies(c1_complaints, c2_complaints,
												  company1, company2):
	''' Draws a chart that displays the number of complaints
	    left to two companies for each day
	   
	Keyword arguments:
	c1_complaints and c2_complaints have the following format:
	[(class<datetime.date>, int), ... ]

	company1 - name of the first company
	company2 - name of the second company
	'''
	
	fig = go.Figure()
	fig.add_trace(
		go.Bar(
			x=[i[0] for i in c1_complaints], 
			y=[i[1] for i in c1_complaints],  
			name=f'Complaints for the company {company1}'
			)
		)
	fig.add_trace(
		go.Bar(
			x=[i[0] for i in c2_complaints],
			y=[i[1] for i in c2_complaints],  
			name=f'Complaints for the company {company2}'
			)
		)
	fig.update_layout(
		legend_orientation="h",
		legend=dict(x=.5, xanchor="center"),
		hovermode="x",
		margin=dict(l=0, r=0, t=0, b=0)
		)
	fig.update_traces(
		hoverinfo="all",
		hovertemplate="Date: %{x}<br>Number of complaints: %{y}"
		)
	fig.show()
