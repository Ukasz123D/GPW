import csv, os, urllib, requests, pyodbc, jinja2, os
import datetime as dt
import pandas as pd
import numpy as np
import colorcet as cc
from getch import pause
from bokeh.document import Document
from bokeh.embed import file_html
from bokeh.resources import CDN
from bokeh.util.browser import view
from bokeh.models import ColumnDataSource, MonthsTicker, CDSView, BooleanFilter, LinearAxis, Range1d, Title, LabelSet, Legend, LegendItem, RangeTool, HoverTool, CrosshairTool, Div, NumeralTickFormatter, DatetimeTickFormatter
from bokeh.models.widgets import Panel, Tabs, DataTable, DateFormatter, NumberFormatter, TableColumn, HTMLTemplateFormatter
from bokeh.plotting import figure, output_file, show, save
from bokeh.layouts import layout, Spacer
from bokeh.palettes import Category20c

def shellchart(width,height,title_text,title_align):
    #kwerenda do wyciągania przychodów
    #oraz przygotowanie danych do wykresu
    df = pd.read_sql("""SELECT TRIM([T].[NAZWA]) AS [NAME], SUM([T].[SEC_VALUE]) AS VAL,TRIM([M].[POZ_NAME]) AS [POZ]
                    FROM [TRANSAKCJE] AS [T], [POZ_MAPPING] AS [M]
                    WHERE [T].[POZYCJA] = [M].[POZ_ID] AND [T].[POZYCJA] IN (2,3)
                    GROUP BY [T].[NAZWA], [M].[POZ_NAME]
                    ORDER BY SUM([T].[SEC_VALUE]) DESC""",conn)
    #promień wewnętrzego pustego pola
    inner_radius = max(width,height)/12
    #promień zewnętrzny wykresu
    outer_radius = max(width,height)/2
    #obliczanie kątów
    df['COUNTER']=df['VAL']/max(df['VAL'])
    min_angle=2.0*np.pi/sum(df['COUNTER'])
    df['START']=0
    for i, row in df.iterrows():
        if i == 0:
            pass
        else:
            df.loc[i,'START'] = df.loc[i-1,'COUNTER']*min_angle+df.loc[i-1,'START']   
    df['STOP']=df['COUNTER']*min_angle+df['START']
    #obliczanie promieni
    df['VAL_RAD']=df.VAL*(outer_radius-inner_radius)/max(df.VAL)+inner_radius
    df['INNER']=inner_radius
    #kolory sekcji
    df.loc[df['POZ']=='Dywidenda','POZ_COL'] = colors[0]
    df.loc[df['POZ']=='Odsetki','POZ_COL'] = colors[1]
    source = ColumnDataSource(data=dict(
        NAME=df['NAME'].values,
        VAL=df['VAL'].values,
        POZ=df['POZ'].values,
        POZ_COL=df['POZ_COL'].values,
        START=df['START'].values,
        STOP=df['STOP'].values,
        VAL_RAD=df['VAL_RAD'].values,
        INNER=df['INNER'].values
        ))
    p = figure(plot_width=width, plot_height=height,
        x_axis_type=None, y_axis_type=None,
        x_range=(-width/2.5, width/1.75), y_range=(-height/3, height/2), toolbar_location=None)
    p1 = p.annular_wedge(x=0, y=0, inner_radius='INNER', outer_radius='VAL_RAD',
                    start_angle='START', end_angle='STOP',
                    color='POZ_COL', source=source)
    hover = HoverTool(tooltips=[
            ("Typ przychodu", "@POZ"),
            ("Źródło", "@NAME"),
            ("Wartość", "@VAL{0,0.00}"),
            ], renderers=[p1])
    p.add_tools(hover)
    # podziały między sekcjami
    p.annular_wedge(x=0, y=0, inner_radius='INNER', outer_radius='VAL_RAD',
                    start_angle='STOP', end_angle='STOP',
                    color='white', source=source)
    p.border_fill_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    p.outline_line_color = None
    return p

#-------------------------------------------------------------------------------------------------------------------
#KOD GŁÓWNY
#-------------------------------------------------------------------------------------------------------------------

tstart = dt.datetime.now()
print('Start: ' + tstart.strftime('%H:%M:%S'))

print('Łączenie z bazą...')
#połączenie z bazą
conn_str = ('Driver={SQL Server};'
            'Server=LEGION\MS3DOIT;'
            'Database=GPW;'
            'Trusted_Connection=yes;')
conn = pyodbc.connect(conn_str)

colors = ["#165493", "#8D8D8D", "#CC0000"] ##niebieski, szary, czerwony

print("Przygotowywanie ...")
p = shellchart(600,600,'Przychody z dywidend i odsetek według źródła','center')

#zapis
print("Zapis...")
output_file("Shellchart.html", title = "Shellchart")
save(p)

show(p)

tstop = dt.datetime.now()
tdiff = tstop - tstart
hours, reminder = divmod(tdiff.total_seconds(), 3600)
minutes, seconds = divmod(reminder, 60)
print('Koniec: ' + tstop.strftime('%H:%M:%S'))
print('Zadanie zakończone w czasie %02i:%02i:%02i' % (hours, minutes, seconds))
