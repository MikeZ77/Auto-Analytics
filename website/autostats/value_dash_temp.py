import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import datetime

def load_layout(value_dataframe):

	return [
	html.Div(children=[
	    html.H1(
	    	children=['Dashboard - {}: {} {} {}'.format(DASHBOARD, YEAR.strip("'"),
	    		MAKE.strip("'"), MODEL.strip("'"))
	    	],
	    	style={
	    		'margin-left':'18%',
	    		'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	    		'color':'#f9f9f9',
	    		'font-size':'2.5em'
	    	}
	    ),
	    html.Img(src=image, height='130',width='200',
	    	style={
	    		'margin-top':'20px',
	    		'margin-left':'10px',
	    		'position': 'absolute',
	    		'top':'0px',
	    		'border-style': 'solid',
	    		'border-width': '3px',
	    		'border-color': '#f9f9f9'
	    	}
	    ),
		html.Div(children=[
	    	html.H3('Value Statistics',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                # 'backgroundColor':'#ebebeb',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
	        html.Div(
	        	dash_table.DataTable(
	        		data=df_table_value.to_dict('records'),
	        		columns=[{'id': c, 'name': c} for c in df_table_value.columns],
	        		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
	                style_cell_conditional=[{'if':{'column_id':c},'width':'131px'} for c in df_table_value.columns],
	        		style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
	                style_data_conditional=[{
	                    "if": {"column_id": "Value Ratios"},
	                    "backgroundColor": "#dfdfe5",
	                    },
	                ]),
	                style={'margin-bottom':'6px'}
	        ),
	        html.Div(children=[
	            html.Button(
	                id='button_price_km',
	                value='Hide',
	                children=[
	                'Price/Kilometers',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram p/km',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_p_km['full_vehicle'],
	        							y=df_hist_p_km['p/km'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'In Class P/KM',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'P/KM'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container p/km',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_price_age',
	                value='Hide',
	                children=[
	                'Price/Age',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram p/age',
	    				figure = {
	    					'data':[
	    						go.Bar(
	    								x=df_hist_p_age['full_vehicle'],
	    								y=df_hist_p_age['p/age'],
	                                    marker=dict(color='#8a0303')
	            					),
	    						],
	    					'layout': go.Layout(
	    								title = 'In Class P/AGE',
	    								xaxis={'title': 'Vehicle'},
	    								yaxis={'title': 'P/AGE'},
	    								template = 'ggplot2'
	    							),
	    				}
	                ),
	            id='container p/age',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_dealer_premium',
	                value='Hide',
	                children=[
	                'Dealer Premium',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram dealer_premium',
	    				figure = {
	    					'data':[
	    						go.Bar(
	    								x=df_hist_p_dealer_premium['full_vehicle'],
	    								y=df_hist_p_dealer_premium['dealer_premium'],
	                                    marker=dict(color='#8a0303')
	            					),
	    						],
	    					'layout': go.Layout(
	    								title = 'In Class Dealer Premium',
	    								xaxis={'title': 'Vehicle'},
	    								yaxis={'title': 'Dealer Premium(%)'},
	    								template = 'ggplot2'
	    							),
	    				}
	        	),
	            id='container dealer_premium',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_avg_physical_depr',
	                value='Hide',
	                children=[
	                'Average Physical Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram avg_physical_depr',
	    				figure = {
	    					'data':[
	    						go.Bar(
	    								x=df_hist_avg_physical_depr['full_vehicle'],
	    								y=df_hist_avg_physical_depr['avg_physical_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	    						],
	    					'layout': go.Layout(
	    								title = 'In Class Average Physical Depreciation',
	    								xaxis={'title': 'Vehicle'},
	    								yaxis={'title': 'Physical Depretiation'},
	    								template = 'ggplot2'
	    							),
	    				}
	        	),
	            id='container avg_physical_depr',
	            style={'width': '100%'}),

	        ]),

	        html.Div(
	        	dash_table.DataTable(
	                id='table1',
	        		data=df_table_depr.to_dict('records'),
	        		columns=[{'id': c, 'name': c} for c in df_table_depr.columns],
	        		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
	                style_cell_conditional=[{'if':{'column_id':c},'width':'69px'} for c in df_table_depr.columns],
	        		style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
	                style_data_conditional=[{
	                    "if": {"column_id": "Depreciation"},
	                    "backgroundColor": "#dfdfe5",
	                    }],
	        	),
	            style = {
	                'position': 'relative',
	                'top':'20px',
	                'margin-bottom':'25px'
	            }
	        ),
	        html.Div(children=[
	            html.Button(
	                id='button_1_week_depr',
	                value='Hide',
	                children=[
	                '1 Week Price Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 1_week_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_1_week_depr['full_vehicle'],
	        							y=df_hist_1_week_depr['1_week_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'One Week Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 1_week_depr',
	            style={'width': '100%'}),
	        ],
	        id='outside container 1_week_depr',
	        ),

	        html.Div(children=[
	            html.Button(
	                id='button_1_month_depr',
	                value='Hide',
	                children=[
	                '1 Month Price Depreciation',
	                #html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 1_month_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_1_month_depr['full_vehicle'],
	        							y=df_hist_1_month_depr['1_month_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'One Month Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 1_month_depr',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_3_month_depr',
	                value='Hide',
	                children=[
	                '3 Month Price Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 3_month_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_3_month_depr['full_vehicle'],
	        							y=df_hist_3_month_depr['3_month_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'Three Month Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 3_month_depr',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_6_month_depr',
	                value='Hide',
	                children=[
	                '6 Month Price Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 6_month_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_6_month_depr['full_vehicle'],
	        							y=df_hist_6_month_depr['6_month_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'Six Month Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 6_month_depr',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_1_year_depr',
	                value='Hide',
	                children=[
	                '1 Year Price Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 1_year_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_1_year_depr['full_vehicle'],
	        							y=df_hist_1_year_depr['1_year_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'One Year Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 1_year_depr',
	            style={'width': '100%'}),

	        ]),
	        html.Div(children=[
	            html.Button(
	                id='button_3_year_depr',
	                value='Hide',
	                children=[
	                '3 Year Price Depreciation',
	                # html.Img(src='plus.png')
	                ],
	                style=button_style
	            ),
	            html.Div(
	                dcc.Graph(
	                    id='histogram 3_year_depr',
	        			figure = {
	        				'data':[
	        					go.Bar(
	        							x=df_hist_3_year_depr['full_vehicle'],
	        							y=df_hist_3_year_depr['3_year_depr'],
	                                    marker=dict(color='#8a0303')
	            					),
	        					],
	        				'layout': go.Layout(
	        							title = 'Three Year Price Depreciation',
	        							xaxis={'title': 'Vehicle'},
	        							yaxis={'title': 'Depreciation(%)'},
	        							template = 'ggplot2'
	        						),
	        			}
	                ),
	            id='container 3_year_depr',
	            style={'width': '100%'}),

	        ]),
	    	html.H3('Buying Power',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                # 'backgroundColor':'#ebebeb',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
	        dcc.Graph(
	            id='buying_power_plot',
	            figure={
	                'data':[
	                    go.Scatter(x=df_buying_power['price'],
	                        y=df_buying_power['weighted_avg'],
	                        fill='tozeroy',
	                        line_color='#cc0000')
	                ],
	                'layout': go.Layout(
	                            # title = {'text':'Buying Power','y':0.80,'x':0.5},
	                            xaxis={'title': 'Vehicle Price ($CAD)'},
	                            yaxis={'title': 'Weighted AVG Price Decrease ($CAD)'},
	                            template = 'ggplot2',
	                            margin={'t': 0, 'b':5}
	                        ), 
	            },
	            # style={
	            # 'padding-top':'-40px'
	            # }
	        ),
	    	html.H3('Value by Location',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                # 'backgroundColor':'#ebebeb',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
	        dcc.Graph(
	            figure={
	            'data': traces,
				'layout': go.Layout(
							title = {'text':'<b>Boxplot Prices by Province</b>','y':0.95,'x':0.5},
	                        margin = dict(t=50),
							xaxis={'title': 'Provinces'},
							yaxis={'title': 'Price(CAD)'},
							template = 'ggplot2'
						),
	            },
	            id='boxplot_province',
				style={'width': '50%',
				'display': 'inline-block',
	            'margin-top':'20px'
				}
	        ),
	        html.Div(
	        children=[
	            dcc.Dropdown(
	                id='provinces_dropdown',
	                options=province_options,
	                value=province_options[0]['label']
	            ),
	            dcc.Graph(id='histogram cities')
	        ],
			style={'width': '50%',
			'display': 'inline-block',
			}
	        ),
	        ],
		    style={
	            'width': '80%',
	            # 'width': '101.7%',
	            'margin': 'auto',
	            'position': 'relative',
	            'top':'40px',
	            },
	        ),],

	    style={
	    	'background-color':'#404040',
	    	'padding': '30px',
	    	'padding-left':'0',
	    	'height':'100px'
	    })
	]