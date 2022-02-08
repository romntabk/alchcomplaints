import plotly
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd
from datetime import date
from collections import defaultdict


def reformat_data(data):
	dict_=defaultdict(int)
	for date_, count_ in data:
		date_format = date(date_.year,date_.month,date_.day)
		dict_[date_format]+=count_
	return list(dict_.keys()),list(dict_.values())

def draw_chart_new_and_changed(new_data, changed_data):
	x_new, y_new = reformat_data(new_data)
	x_changed, y_changed = reformat_data(changed_data)
	fig = go.Figure()
	fig.add_trace(go.Bar(x = x_changed, y = y_changed, name='Исправления'))
	fig.add_trace(go.Bar(x = x_new, y = y_new, name='Добавления'))
	fig.show() 


def draw_chart_number_of_complaints_for_companies(a, b):
	pass
