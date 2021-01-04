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

#--------------------------------
#wykres wartość portfela w czasie
#--------------------------------
#funkcje dodatkowe

#funkcja na stylizowanie wykresu
def style_plot(p, source):
    #obie osie
    p.axis.minor_tick_in = 0
    p.axis.minor_tick_out = 0
    #etytkiety osi
    p.yaxis.formatter = NumeralTickFormatter(format="0 a")
    p.xaxis.formatter = DatetimeTickFormatter(days=["%d %B %G"],
                                              months=["%b %G"],
                                              years = ['%Y'])
    #os X
    p.yaxis.axis_label= "Wartość w tysiącach PLN"
    p.xaxis.major_label_orientation = np.pi / 4
    p.xaxis.ticker = MonthsTicker(months=list(range(1,13)))
    return p

#funkcja główna na wykres wartości portfela w czasie
def walletintime(width,height,title_text,title_align):
    df = pd.read_sql("""SELECT [P].[DATA], TRIM([P].[NAZWA]) AS [TYP], [SEC_COST] AS [KOSZT], [SEC_VALUE] AS [WARTOSC]
                    FROM [PORTFEL_IN_TIME_VW] AS [P]
                    WHERE [P].[NAZWA] IN ('AKCJE','OBLIGACJE','RAZEM')
                    ORDER BY [DATA] ASC
                    OPTION (MAXRECURSION 0)""",conn)

    df = pd.pivot_table(df, index='DATA', columns='TYP', values=['KOSZT','WARTOSC'], aggfunc=sum, fill_value=0)
    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    df.reset_index(inplace=True)
    df['LP'] = np.arange(len(df))
    df['DATA'] = pd.to_datetime(df['DATA'])

    #wstepna budowa wykresu
    #ustalanie wysokosci wykresu głownego vs selektora
    mainheight = int(height/4*3)
    selheight = height - mainheight - 10

    p = figure(plot_width=width, plot_height=mainheight,
               x_range=(min(df['DATA']), max(df['DATA'])+ pd.DateOffset(5)),
               x_axis_type="datetime", toolbar_location=None)
    i = -1
    for col_name in df:
        if (col_name[:len('WARTOSC')] == 'WARTOSC') or (col_name[:len('KOSZT')] == 'KOSZT'):
            if i == (len(df.columns)-2)/2-1:
                i=0
            else:
                i=i+1
            if col_name[:len('WARTOSC')] == 'WARTOSC':
                line_dash='solid'
            elif col_name[:len('KOSZT')] == 'KOSZT':
                line_dash='dashed'
            p.line(x='DATA', y=col_name, line_width=2,
                   line_dash=line_dash, line_color=colors[i],
                   source=df, name=col_name)
    #stylizowanie wykresu           
    p = style_plot(p, df)
    #budowa tooltipów
    for r in p.renderers:
        if r.name in ['WARTOSC_AKCJE','WARTOSC_OBLIGACJE']:
            hover = HoverTool(
                tooltips=[
                    ('', r.name.replace('WARTOSC_','')),
                    ('Koszt', '@'+r.name.replace('WARTOSC','KOSZT')+'{0,0.00}'),
                    ('Wartość', '@'+r.name+'{0,0.00}'),
                    ],
                mode='vline',
                renderers = [r]
            )
            p.add_tools(hover)
        elif r.name == 'WARTOSC_RAZEM':
            hover = HoverTool(
                tooltips=[
                    ('', r.name.replace('WARTOSC_','')),
                    ('Data', '@DATA{%F}'),
                    ('Koszt', '@KOSZT_RAZEM{0,0.00}'),
                    ('Wartość', '@WARTOSC_RAZEM{0,0.00}'),
                    ],
                formatters={'@DATA': 'datetime'},
                mode='vline',
                renderers = [r]
            )
            p.add_tools(hover)
    #budowa legendy
    for r in p.renderers:
        if r.name == "WARTOSC_AKCJE":
            legend_1 = LegendItem(label="Wartość", renderers=[r], index=0)
        elif r.name == "KOSZT_AKCJE":
            legend_2 = LegendItem(label="Koszt", renderers=[r], index=0)
    legend = Legend(items=[legend_1, legend_2])
    p.add_layout(legend)
    p.legend.location = 'top_left'
    p.legend.border_line_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    p.outline_line_color = None

    #wykres dodatkowy do ustawiania zakresu dat
    select = figure(plot_width=width, plot_height=selheight, y_range=p.y_range,
                    x_range=(min(df['DATA']), max(df['DATA'])),
                    x_axis_type='datetime', y_axis_type=None, toolbar_location=None)

    range_tool = RangeTool(x_range=p.x_range)
    range_tool.overlay.fill_color = 'navy'
    range_tool.overlay.fill_alpha = 0.2

    select.line(x='DATA', y='WARTOSC_RAZEM', source=df)
    select.ygrid.grid_line_color = None
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool
    select.xaxis.formatter = DatetimeTickFormatter(days=["%d %B %G"],
                                          months=["%b %G"],
                                          years = ['%Y'])
    select.xaxis.major_label_orientation = np.pi / 4
    select.xaxis.ticker = MonthsTicker(months=list(range(1,13)))
    select.outline_line_color = None

    return layout(
                children=[
                    [p],
                    [select]
                    ]
                )

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
p = walletintime(790,680,'Wartość portfela w czasie','center')

#zapis
print("Zapis...")
output_file("Walletintime.html", title = "Walletintime")
save(p)

show(p)

tstop = dt.datetime.now()
tdiff = tstop - tstart
hours, reminder = divmod(tdiff.total_seconds(), 3600)
minutes, seconds = divmod(reminder, 60)
print('Koniec: ' + tstop.strftime('%H:%M:%S'))
print('Zadanie zakończone w czasie %02i:%02i:%02i' % (hours, minutes, seconds))
