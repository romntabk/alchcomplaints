import plotly
import plotly.graph_objs as go
import plotly.express as px # эта на всякий, если другие графики делать
import numpy as np
import pandas as pd
from datetime import date
def my_f(new_data, changed_data):

	# dices = pd.DataFrame(np.random.randint(low=1, high=7, size=(100, 2)), columns=('Кость 1', 'Кость 2')) # просто рандом дф
	# dices['Сумма'] = dices['Кость 1'] + dices['Кость 2']
	# # Первые 5 бросков игральных костей

	# sum_counts = dices['Сумма'].value_counts().sort_index()
	# # количество выпавших сумм

	# x = np.arange(0, 5, 0.1) # тут будут даты в любом формате
	# def f(x):                # тут будут жалобы ай мин
	#     return x**2 

	# fig = go.Figure()
	# fig.add_trace(go.Scatter(x=x, y=f(x), mode='lines+markers',  name='Жалобы для компании х'))
	# fig.add_trace(go.Scatter(x=x, y=x, mode='markers', name='Жалобы для компании у'))
	# fig.update_layout(legend_orientation="h",
	#                   legend=dict(x=.5, xanchor="center"),
	#                   hovermode="x",
	#                   margin=dict(l=0, r=0, t=0, b=0))
	# fig.update_traces(hoverinfo="all", hovertemplate="Дата: %{x}<br>Количество жалоб: %{y}")
	# fig.show()


	# BAD_d_grouped1 = dices.head(10).groupby(['Сумма']).count()
	# BAD_d_grouped2 = dices.head(10).groupby(['Сумма']).count()

	# labels = BAD_d_grouped1.index
	# values = BAD_d_grouped1['Кость 1'].values
	# values2 = BAD_d_grouped2['Кость 1'].values
	# print(type(data))
	# print(type(data[0]))
	# print(type(data[0][0]))
	# print(type(data[0][1]))

	# x=[i[0] for i in data]
	# y=[i[1] for i in data]
	# print(date('2000-01-01'))

	x_new=[str(i[0])  for i in new_data]
	y_new=[i[1] for i in new_data]
	x_changed=[str(i[0]) for i in changed_data]
	y_changed=[i[1] for i in changed_data]
	fig = go.Figure()
	fig.add_trace(go.Bar(x = x_changed, y = y_changed, name='Исправления'))
	fig.add_trace(go.Bar(x = x_new, y = y_new, name='Добавления'))
	fig.show() # оно не выводит крайние нулевые значения, но вроде не страшно и лень переделывать
# my_f()