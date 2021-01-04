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

def dict_builders(): 
    #budowa słownika dla tak/nie
    y_list = ("tak","t","prawda","yes","y")
    n_list = ("nie","n","fałsz","falsz","no","n")

    global yn_dict
    yn_dict = {}

    for item in n_list:
        yn_dict[item.upper()] = 0

    for item in y_list:
        yn_dict[item.upper()] = 1

#funkcja pobierająca informację od użytkownika i wprawdzająca względem zdefiniowanego słownika
def dict_prompt(input_desc, input_dict):
    success = None
    while success is None:
        try:
            user_input = input(input_desc+"\n")
            if user_input.upper() in input_dict:
                success = True
                return input_dict.get(user_input.upper())
            else:
                print('Nie rozumiem, spróbuj ponownie.')
        except:
            pass

#funkcje wspólne
def finalize_table(t, header, edit, reorder, sort):
    t.index_position = None
    t.header_row = header
    t.editable = edit
    t.reorderable = reorder
    t.sortable = sort
    return t

#dashboard

#funkcje budujące składniki dashboardu
#------
#TABELE
#------
#---------------------------
#tabela z zawartością portfela
#---------------------------
def wallettable(width,height):
    #kwerenda do wyciągania przychodów
    df = pd.read_sql("""SELECT * FROM [PORTFEL_VW]""",conn)
    #dane
    source = ColumnDataSource(data=df)
    #formatowanie warunkowe
    cndtemplate="""
    <div style="color:<%= 
        (function colorfromint(){
            if(value < 0){
                return("red")}
            else{return("green")}
            }()) %>"> 
    <%= (value).toFixed(2) %></div>
    """
    cndformatter = HTMLTemplateFormatter(template=cndtemplate)
    #kolumny
    columns = [
            TableColumn(field="NAZWA", title="Nazwa"),
            TableColumn(field="CURR_COUNT", title="Pakiet"),
            TableColumn(field="PUR_PRICE", title="Zakup",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="PRICE_TODAY", title="Obecnie",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="PRICE_MIN", title="Próg",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="CURR_COST", title="Koszt całk.",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="CURR_VALUE", title="Wart. całk.",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="TAX", title="Podatek",
                        formatter=NumberFormatter(format='0,0.00')),
            TableColumn(field="YIELD", title="Zysk",
                        formatter=cndformatter),
            TableColumn(field="%", title="%",
                        formatter=NumberFormatter(format='0,0.00%'))
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, False)
    return t

#-----------------
#data aktualizacji
#-----------------
def updatetable(width,height):
    #kwerenda do wyciągania przychodów
    df = pd.read_sql("""SELECT MAX(DATA) AS [DATA], 'GPW' AS [RYNEK] FROM [NOTOWANIA_GPW]
                        UNION
                        SELECT MAX(DATA) AS [DATA], 'New Connect' AS [RYNEK] FROM [NOTOWANIA_NC]
                        UNION
                        SELECT MAX(DATA) AS [DATA], 'Catalyst' AS [RYNEK] FROM [NOTOWANIA_CATALYST]""",conn)
    #dane
    source = ColumnDataSource(data=df)
    #kolumny
    columns = [
            TableColumn(field="RYNEK", title="Rynek"),
            TableColumn(field="DATA", title="Data")
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, False)
    return t

#--------------------------------------
#nadchodzące wypłaty dywidend i odsetek
#--------------------------------------
def exppremium(width,height):
    #kwerenda do wyciągania przychodów
    df = pd.read_sql("""SELECT [DATA], [NAZWA], [VALUE] FROM
                        (
                        --PLANOWANE DYWIDENDY
                        SELECT [N].[DATA_WYP] AS [DATA], [N].[NAZWA], CAST(ROUND([N].[VAL]*[P].[SEC_COUNT]*0.81,2) AS DECIMAL(10,2)) AS [VALUE]
                        FROM [CURR_PORTFEL_VW] AS [P], [PREMIUM] AS [N] 
                        WHERE [P].[NAZWA] = [N].[NAZWA]
                        AND [P].[SEC_TYPE] = 0

                        UNION

                        --PLANOWANE ODSETKI
                        SELECT [N].[DATA_WYP] AS [DATA], [N].[NAZWA], CAST(ROUND([N].[VAL]*[P].[SEC_COUNT]*0.81,2) AS DECIMAL(10,2)) AS [VALUE]
                        FROM [CURR_PORTFEL_VW] AS [P], [PREMIUM] AS [N] 
                        WHERE [P].[NAZWA] = [N].[NAZWA]
                        AND [P].[SEC_TYPE] = 1
                        ) AS [TT]
                        ORDER BY [DATA]""",conn)
    #dane
    source = ColumnDataSource(data=df)
    #kolumny
    columns = [
            TableColumn(field="DATA", title="Data"),
            TableColumn(field="NAZWA", title="Spółka"),
            TableColumn(field="VALUE", title="Wartość")
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, False)
    return t

#--------------------------------------
#wynik zabawy w inwestowanie
#--------------------------------------

def outcome(width,height):
    df = pd.read_sql("""SELECT [A].[COL], [A].[VALUE]
                        FROM
                        (
                        SELECT [T1].[Koszt], [T1].[Wartość], [T2].[Przychody], [T3].[Premium]
                        FROM
                        (SELECT -SUM([CURR_COST]) AS [Koszt], SUM([CURR_VALUE]) AS [Wartość] FROM [PORTFEL_VW]) AS [T1],
                        (SELECT SUM([SEC_VALUE]) AS [Przychody] FROM [TRANSAKCJE] WHERE [POZYCJA] IN (2,3)) AS [T2],
						(SELECT [ROK], SUM([VAL]) AS [Premium] FROM [PREMIUM_VW] WHERE [ROK] = YEAR(GETDATE()) GROUP BY [ROK]) AS [T3]) AS [T]
                        CROSS APPLY (VALUES ('Koszt', [Koszt]),
											('Wartość',[Wartość]),
											('Bilans',[Koszt] + [Wartość]),
											('Przychody',[Przychody]),
											('Wynik',[Koszt] + [Wartość] + [Przychody]),
											('Przychody w tym roku',[Premium]),
											('ROI',-[Premium]/[Koszt]*100)
											) AS [A] ([COL],[VALUE])""",conn)
        
    #dane
    source = ColumnDataSource(data=df)
    #kolumny
    columns = [
            TableColumn(field="COL", title='Podsumowanie'),
            TableColumn(field="VALUE", title='', width = int(width / 2 * 3), formatter=NumberFormatter(format='(0,0.00)'))
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, False)
    return t

#------------------------------------------------
#tabela nadchodzacych możliwości zakupu obligacji
#------------------------------------------------

def catalysator(width,height):
    df = pd.read_sql("""WITH [T] AS (
                                    SELECT [NAZWA], MIN([NEXT_PAY]) AS [NEXT_PAY] FROM [CATALYSATOR]
                                    WHERE [NEXT_PAY] < DATEADD(DAY, 30, GETDATE())
                                    GROUP BY [NAZWA]
                                    )

                        SELECT [C].[NEXT_PAY] AS [TERMIN], [C].[NAZWA], [ISIN], [CURR_OPR]/100 AS [%], [LICZBA_POZ_OKRESOW] AS [LPO],
                                "RODZAJ" = CASE
                                        WHEN [FREQ_WYPLAT] = 3 THEN 'Kwartalne'
                                        WHEN [FREQ_WYPLAT] = 6 THEN 'Półroczne'
                                END
                        FROM [CATALYSATOR] AS [C], [T]
                        WHERE [C].[NAZWA] = [T].[NAZWA]
                        AND [C].[NEXT_PAY] = [T].[NEXT_PAY]
                        ORDER BY [C].[NEXT_PAY]""",conn)
       
    #dane
    source = ColumnDataSource(data=df)
    #kolumny
    columns = [
            TableColumn(field="TERMIN", title="Termin", width=125),
            TableColumn(field="NAZWA", title="Emitent"),
            TableColumn(field="ISIN", title="ISIN", width=120),
            TableColumn(field="RODZAJ", title="Rodzaj", width=120),
            TableColumn(field="%", title="%", formatter=NumberFormatter(format='0.00%'), width=50),
            TableColumn(field="LPO", title="Poz. wypłat", width=50),
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, True)
    t.index_position = None
    return t

#----------------------------------------------------------
#tabela nachodzących możliwości zakupu spółek dywidendowych
#----------------------------------------------------------

def divdeamon(width,height):
    df = pd.read_sql("""WITH [N] AS (
				SELECT [ISIN], [ZAMKNIECIE] FROM [NOTOWANIA_GPW]
				WHERE [DATA] = (SELECT MAX(DATA) FROM [NOTOWANIA_GPW])
				)

                        SELECT [D].[DATA_PRAW] AS [TERMIN], [D].[NAZWA], [D].[ISIN], [D].[DATA_WYP] AS [WYPŁATA], [D].[VAL] AS [DYWIDENDA], [N].[ZAMKNIECIE] AS [KURS], [D].[VAL] / [N].[ZAMKNIECIE] AS [%]
                        FROM [DYWIDENDY] AS [D], [N]
                        WHERE [D].[ISIN] = [N].[ISIN]
                        AND [D].[DATA_PRAW] < DATEADD(DAY, 30, GETDATE())
                        AND [D].[DATA_PRAW] > DATEADD(DAY, 3, GETDATE())
                        ORDER BY [D].[DATA_PRAW]""",conn)
       
    #dane
    source = ColumnDataSource(data=df)
    #kolumny
    columns = [
            TableColumn(field="TERMIN", title="Termin", width=125),
            TableColumn(field="NAZWA", title="Spółka"),
            TableColumn(field="ISIN", title="ISIN", width=200),
            TableColumn(field="WYPŁATA", title="Data wypłaty", width=125),
            TableColumn(field="DYWIDENDA", title="Dywidenda", formatter=NumberFormatter(format='0.00'), width=85),
            TableColumn(field="KURS", title="Kurs", formatter=NumberFormatter(format='0.00'), width=65), 
            TableColumn(field="%", title="%", formatter=NumberFormatter(format='0.00%'), width=65),
        ]
    #tabela
    t = DataTable(source=source, columns=columns, width=width, height=height)
    t = finalize_table(t, True, False, False, True)
    return t

#-------------------------------------------------------------------------------------------------------------------
#WYKRESY
#-------------------------------------------------------------------------------------------------------------------
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

#-------------------------
#wykres przychodów w czasie
#-------------------------
def premiumintime(width,height,title_text,title_align):
    #kwerenda do wyciągania przychodów
    df = pd.read_sql("""SELECT [ROK], [VAL], [POZ] FROM [PREMIUM_VW]""",conn)
    #przestawienie wyniku
    df = df.pivot(index='ROK', columns='POZ', values='VAL')
    df = df.fillna(0)
    df = df.reset_index()
    for col in ['ROK','Odsetki','Odsetki planowane','Dywidenda','Dywidenda planowana']:
        if not col in df.columns:
            df[col] = 0
    #budowa wykresu
    #dane
    source = ColumnDataSource(data=dict(y=df['ROK'].values,
                                        x1=df['Odsetki'].values,
                                        x2=df['Odsetki planowane'].values,
                                        x3=df['Dywidenda'].values,
                                        x4=df['Dywidenda planowana'].values))
    #układ wszpółrzędnych
    p = figure(plot_width=width, plot_height=height, toolbar_location=None)
    p.ygrid.grid_line_width = 0
    #wykres
    renderers = p.hbar_stack(['x1', 'x2', 'x3', 'x4'], y='y', height=0.8, color=["#8D8D8D", "#b2b2b2", "#165493", "#217edd"], source=source, )
    #tooltips
    for r in renderers:
        if r.name == "x1":
            n = "Odsetki"
            v = "@x1{(0,0.00)}"
            legend_1 = LegendItem(label=n, renderers=[r], index=0)
        elif r.name == "x2":
            n = "Odsetki (planowane)"
            v = "@x2{(0,0.00)}"
        elif r.name == "x3":
            n = "Dywidendy"
            v = "@x3{(0,0.00)}"
            legend_2 = LegendItem(label=n, renderers=[r], index=0)
        else:
            n = "Dywidendy (planowane)"
            v = "@x4{(0,0.00)}"
        hover = HoverTool(tooltips=[
            ("Typ przychodu", n),
            ("Wartość", v),
            ], renderers=[r])
        p.add_tools(hover)
    #os Y
    p.yaxis.axis_label= "Lata"
    p.yaxis.ticker.max_interval = 1
    #os X
    p.xaxis.axis_label= "Przychody w PLN"
    p.xaxis.formatter = NumeralTickFormatter(format="0,0")
    p.x_range.start = 0
    #obie osie
    p.axis.minor_tick_in = 0
    p.axis.minor_tick_out = 0
    p.title.text = title_text
    p.title.align = title_align
    p.outline_line_color = None
    return p

#---------------------------------------------------
#wizualizacja źródła środków - nowe vs reinwestowane
#---------------------------------------------------
def reinvest(width, height, title_text,title_align):
    #kwerenda do wyciągania przychodów
    df = pd.read_sql("""SELECT [NEW] AS 'Nowe środki',
                               [PREMIUM] AS 'Odsetki / Dywidendy',
                               [TRANS] AS 'Kupno / Sprzedaż' FROM [REINVEST_VW]""",conn)
    #przestawienie wyniku
    df = df.transpose().reset_index().rename(columns = {'index': 'TYPE', 0: 'VAL'})
    #wyliczenie kątów
    obrot = np.pi/2
    df['PCNT'] = round(df['VAL']/df['VAL'].sum(),4)
    df['PCNT_LBL'] = round(df['VAL']/df['VAL'].sum(),4)*100
    df['ANGLE'] = df['VAL']/df['VAL'].sum() * 2*np.pi
    df['COLOR'] = colors
    #obracanie wykresu kolowego
    df['INS'] = 0
    df['INE'] = df['ANGLE']
    df['ROTS'] = obrot
    df['ROTE'] = df['ANGLE'] + obrot
    for i, row in df.iterrows():
        if i == 0:
            if df.loc[i,'ROTS'] < 2*np.pi:
                df.loc[i,'START'] = df.loc[i,'ROTS']
            else:
                df.loc[i,'START'] = df.loc[i,'ROTS'] - 2*np.pi

            if df.loc[i,'ROTE'] < 2*np.pi:
                df.loc[i,'STOP'] = df.loc[i,'ROTE']
            else:
                df.loc[i,'STOP'] = df.loc[i,'ROTE'] - 2*np.pi        
        else:
            df.loc[i,'INS'] = df.loc[i-1,'INS'] + df.loc[i-1,'ANGLE']
            df.loc[i,'INE'] = df.loc[i-1,'INE'] + df.loc[i,'ANGLE']

            df.loc[i,'ROTS'] = df.loc[i-1,'ROTS'] + df.loc[i-1,'ANGLE']
            df.loc[i,'ROTE'] = df.loc[i-1,'ROTE'] + df.loc[i,'ANGLE']
            
            if df.loc[i,'ROTS'] < 2*np.pi:
                df.loc[i,'START'] = df.loc[i,'ROTS']
            else:
                df.loc[i,'START'] = df.loc[i,'ROTS'] - 2*np.pi
                
            if df.loc[i,'ROTE'] < 2*np.pi:
                df.loc[i,'STOP'] = df.loc[i,'ROTE']
            else:
                df.loc[i,'STOP'] = df.loc[i,'ROTE'] - 2*np.pi
    source = ColumnDataSource(df)
    p = figure(plot_height=height, plot_width=width, toolbar_location=None, x_range=(-.4, 1.0))
    r = p.wedge(x=0, y=1, radius=0.35,
            start_angle='START', end_angle='STOP',
            line_color="white", fill_color='COLOR', source=source)
    #tooltip
    hover = HoverTool(
                tooltips=[
                    ('Źródło', '@TYPE'),
                    ('Wartość', '@VAL{0,0.00}'),
                    ('Procent', '@PCNT{0.00%}'),
                    ]
            )
    p.add_tools(hover)
    #budowa i dodawanie legendy
    legend = Legend(items=[LegendItem(label=dict(field='TYPE') ,renderers = [r])], location = 'center_right')
    p.add_layout(legend)
    p.legend.border_line_color = None
    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None
    p.title.text = title_text
    p.title.align = title_align
    p.outline_line_color = None
    return p

#---------------------------
#wykres skorupiaka - przody z dywidend i odsetek wg źródłowej spółki
#---------------------------
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

#---------------------------------
#historyczny rozkład wypłat w roku
#---------------------------------

def mapmonth(month):
    month = dt.datetime(2000, month, 1)
    return month.strftime('%b') 

def premiumfreq(width,height,title_text,title_align):
    df = pd.read_sql("""WITH [T] AS (SELECT [DATA],[NAZWA],[SEC_TYPE_NAME]
			FROM [TRANSAKCJE] AS [T], [SEC_MAPPING] AS [M]
			WHERE [T].[SEC_TYPE] = [M].[SEC_TYPE_ID] 
			AND [T].[POZYCJA] IN (2,3)
			)
                        SELECT MONTH([DATA]) AS [MONTH], COUNT([NAZWA]) AS [COUNT], TRIM([SEC_TYPE_NAME]) AS [TYPE]
                        FROM [T]
                        GROUP BY MONTH([DATA]), [SEC_TYPE_NAME]""",conn)
    #przestawianie ramki danych
    df = pd.pivot_table(df, index='MONTH', columns='TYPE', values=['COUNT'], aggfunc=sum, fill_value=0)
    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    df.reset_index(inplace=True)
    df['LOC_Akcje'] = df['MONTH'] - 0.33
    df['LOC_Obligacje'] = df['MONTH'] + 0.33 
    df['MONTH'] = df['MONTH'].apply(mapmonth)
    df['COLOR_Akcje'] = colors[0]
    df['COLOR_Obligacje'] = colors[1]
    for typ in ['Akcje','Obligacje']:
        minval = 0
        maxval = df['COUNT_'+typ].max()
        df['COUNT_'+typ] = (df['COUNT_'+typ] - minval)/(maxval - minval)
    #ustalanie źródła danych dla wykresów
    source = ColumnDataSource(df)
    #budowa wykresu
    p = figure(x_range=df['MONTH'], plot_width=width, plot_height=height, toolbar_location=None)
    #budowa słupków
    for typ in ['Akcje','Obligacje']:
        top = 'COUNT_'+typ
        col = 'COLOR_'+typ
        loc = 'LOC_'+typ
        p.vbar(name = typ, x=loc, top=top, width=0.25, color = col, source = source)
    #budowa legendy
    for r in p.renderers:
        if r.name == "Akcje":
            legend_1 = LegendItem(label="Dywidendy    ", renderers=[r], index=0)
        elif r.name == "Obligacje":
            legend_2 = LegendItem(label="Odsetki", renderers=[r], index=0)
    legend = Legend(items=[legend_1, legend_2])
    p.add_layout(legend,'below')
    p.legend.location = 'center'
    p.legend.orientation = 'horizontal'
    p.legend.border_line_color = None
    #stylizacja osi
    p.y_range.start = 0
    p.yaxis.visible = False
    p.ygrid.grid_line_width = 0
    p.outline_line_color = None
    p.border_fill_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    return p

#---------------------
#Treemapa dla portfela
#---------------------
def normalize_sizes(sizes, dx, dy):
    total_size = sum(sizes)
    total_area = dx * dy
    sizes = list(map(float, sizes))
    sizes = list(map(lambda size: size * total_area / total_size, sizes))
    return sizes
   
def layoutrow(sizes, x, y, dx, dy):
    covered_area = sum(sizes)
    width = covered_area / dy
    rects = []
    for size in sizes:
        rects.append({'x': x, 'y': y, 'dx': width, 'dy': size / width})
        y += size / width
    return rects

def layoutcol(sizes, x, y, dx, dy):
    covered_area = sum(sizes)
    height = covered_area / dx
    rects = []
    for size in sizes:
        rects.append({'x': x, 'y': y, 'dx': size / height, 'dy': height})
        x += size / height
    return rects

def layoutall(sizes, x, y, dx, dy):
    return layoutrow(sizes, x, y, dx, dy) if dx >= dy else layoutcol(sizes, x, y, dx, dy)

def leftoverrow(sizes, x, y, dx, dy):
    covered_area = sum(sizes)
    width = covered_area / dy
    leftover_x = x + width
    leftover_y = y
    leftover_dx = dx - width
    leftover_dy = dy
    return (leftover_x, leftover_y, leftover_dx, leftover_dy)

def leftovercol(sizes, x, y, dx, dy):
    covered_area = sum(sizes)
    height = covered_area / dx
    leftover_x = x
    leftover_y = y + height
    leftover_dx = dx
    leftover_dy = dy - height
    return (leftover_x, leftover_y, leftover_dx, leftover_dy)

def leftover(sizes, x, y, dx, dy):
    return leftoverrow(sizes, x, y, dx, dy) if dx >= dy else leftovercol(sizes, x, y, dx, dy)
   
def worst_ratio(sizes, x, y, dx, dy):
    return max([max(rect['dx'] / rect['dy'], rect['dy'] / rect['dx']) for rect in layoutall(sizes, x, y, dx, dy)])

def squarify(sizes, x, y, dx, dy):
    sizes = list(map(float, sizes))
    if len(sizes) == 0:
        return []
    if len(sizes) == 1:
        return layoutall(sizes, x, y, dx, dy)
    # figure out where 'split' should be
    i = 1
    while i < len(sizes) and worst_ratio(sizes[:i], x, y, dx, dy) >= worst_ratio(sizes[:(i+1)], x, y, dx, dy):
        i += 1
    current = sizes[:i]
    remaining = sizes[i:]
    (leftover_x, leftover_y, leftover_dx, leftover_dy) = leftover(current, x, y, dx, dy)
    return layoutall(current, x, y, dx, dy) + squarify(remaining, leftover_x, leftover_y, leftover_dx, leftover_dy)

#kod główny na treemap
#---------------------
def treemap(width,height,title_text,title_align):
    df = pd.read_sql("""SELECT [Z].[DATA], TRIM([Z].[NAZWA]) AS [NAZWA], [M].[SEC_TYPE_NAME], [Z].[ZMIANA]
              FROM [ZMIANA_VW] AS [Z], [SEC_MAPPING] AS [M]
              WHERE [Z].[SEC_TYPE] = [M].[SEC_TYPE_ID]
              ORDER BY [Z].[ZMIANA] DESC""",conn)

    #ustalanie kolorów
    maxabs = max(df['ZMIANA'].abs())
    df['ZMIANA_ABS+1'] = df['ZMIANA'].abs() + 1
    for i, row in df.iterrows():
        cindex = int((abs(df.loc[i,'ZMIANA'])/maxabs)*128 - 1)
        if df.loc[i,'ZMIANA'] < 0:
            cindex = cindex + 127
        elif df.loc[i,'ZMIANA'] == 0:
            cindex = 128
        elif df.loc[i,'ZMIANA'] > 0:
            cindex = 128 - cindex
        df.loc[i,'COLOR'] = cc.CET_D3[cindex]
    
    #normalizacja wartości dla 
    x = 0.
    y = 0.
    norm_x = width
    norm_y = height

    #ustalanie źródła danych
    source = ColumnDataSource(df)
    source.data['ZMIANA_%'] = ["{}%".format(x) for x in source.data['ZMIANA']]
    values = normalize_sizes(source.data['ZMIANA_ABS+1'], width, height)
    #budowanie prostokątów
    rects = squarify(values, x, y, width, height)
    X = [rect['x'] for rect in rects]
    Y = [rect['y'] for rect in rects]
    dX = [rect['dx'] for rect in rects]
    dY = [ rect['dy'] for rect in rects]
    XdX = []
    YdY = []
    for i in range(len(X)):
        XdX.append(X[i]+dX[i])
        YdY.append(Y[i]+dY[i])
        #przygotowanie punktów dla etykiet - środki prostokątów
        Xlab = []
        Ylab = []
        for r in rects:
            x, y, dx, dy = r['x'], r['y'], r['dx'], r['dy']
            Xlab.append(x+dx/2)
            Ylab.append(y+dy/2)
        #przygotowanie danych dla ostatecznej mapy
        plotsource = ColumnDataSource(
            data=dict(
                Xlab = Xlab,
                Ylab = Ylab,
                NAZWA = source.data['NAZWA'],
                COLOR = source.data['COLOR'],
                ZMIANA = source.data['ZMIANA_%']
            )
        )

    #budowanie mapy
    p = figure(plot_width=width, plot_height=height, toolbar_location=None)
    p.quad(top=YdY, bottom=Y, left=X, right=XdX, color=plotsource.data['COLOR'])
     
    labels1 = LabelSet(x='Xlab', y='Ylab', text='NAZWA', level='glyph',
        text_font_style='bold', text_font_size='8pt', text_color='white', text_align = 'center',
        source=plotsource, render_mode='css',)

    labels2 = LabelSet(x='Xlab', y='Ylab', text='ZMIANA', level='glyph',
        text_font_style='bold', text_font_size='8pt', text_color='white', text_align = 'center',
        y_offset = -15, source=plotsource, render_mode='css',)

    p.add_layout(labels1)
    p.add_layout(labels2)

    p.ygrid.grid_line_width = 0
    p.xgrid.grid_line_width = 0
    p.axis.visible = False
    p.border_fill_color = None
    p.outline_line_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    return p

#-----------------------------------------------
#nadchodzące dywidendy - historia notowań spółek
#-----------------------------------------------

def line_plot(p, source, col_name, color):
    p.line(name=col_name, x='DATA', y=col_name, line_width=2, line_dash='solid', source=source, color=color)
    return p

def dot_marker(p, source, col_name, color):
    circle_filter = [True if val != 0 else False for val in source.data[col_name]]
    circle_view = CDSView(source = source, filters=[BooleanFilter(circle_filter)])
    p.circle(name=col_name, x='DATA', y=col_name, size=10, source=source, color=color, view=circle_view)
    return p

#kod główny na wykres
def dividend_analysis(width,height,title_text,title_align):
    df = pd.read_sql("""SELECT [N].[DATA], [N].[NAZWA], [N].[ZAMKNIECIE] AS [VAL],
                    SUM(IIF([N].[DATA] = [D].[DATA_PRAW],[N].[ZAMKNIECIE],0)) AS [PRAWO],
                    SUM(IIF([N].[DATA] = [D].[DATA_BEZDYW],[N].[ZAMKNIECIE],0)) AS [BEZ],
                    SUM(IIF([N].[DATA] = [D].[DATA_WYP],[N].[ZAMKNIECIE],0)) AS [WYP],
                    SUM(IIF([N].[DATA] = [D].[DATA_WZA],[N].[ZAMKNIECIE],0)) AS [WZA]
                    FROM [NOTOWANIA_GPW] AS [N], [DYWIDENDY] AS [D]
                    WHERE [N].[ISIN] = [D].[ISIN] 
                    AND [N].[ISIN] IN (
                            SELECT [ISIN]
                            FROM [DYWIDENDY]
                            WHERE [DATA_PRAW] > DATEADD(DAY, 7, GETDATE())
                            )
                    GROUP BY [N].[DATA], [N].[NAZWA], [N].[ZAMKNIECIE]
                    ORDER BY [NAZWA], [DATA]""",conn)

    df = pd.pivot_table(df, index='DATA', columns='NAZWA', values=['VAL','BEZ','WYP','WZA','PRAWO'], aggfunc=sum, fill_value=0)
    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    df.reset_index(inplace=True)
    df['DATA'] = pd.to_datetime(df['DATA'])
    source = ColumnDataSource(df)
    p = figure(plot_width=width, plot_height=height,
               x_range=(min(df['DATA']), max(df['DATA'])),
               x_axis_type='datetime', toolbar_location=None)
    i=0
    for col_name in df.columns:
        if (col_name[:len('VAL')] == 'VAL'):
            p = line_plot(p, source, col_name, Category20c[20][i])
            i = i + 1
        if (col_name[:len('BEZ')] == 'BEZ'):
            p = dot_marker(p, source, col_name, "red")
        if (col_name[:len('WYP')] == 'WYP'):
            p = dot_marker(p, source, col_name, "green")
        if (col_name[:len('WZA')] == 'WZA'):
            p = dot_marker(p, source, col_name, "purple")
        if (col_name[:len('PRAWO')] == 'PRAWO'):
            p = dot_marker(p, source, col_name, "grey")

    legenditems = []
    markeritems = []
    i = [0,0,0,0]
    for r in p.renderers:
        if r.name[:len('VAL')] == 'VAL':
            legenditems.append(LegendItem(label=r.name[len('VAL_'):], renderers=[r], index=0))
        if r.name[:len('BEZ')] == 'BEZ' and i[0] == 0:
            i[0] = 1
            markeritems.append(LegendItem(label="Notowanie bez dywidendy", renderers=[r], index=0))
        if r.name[:len('WYP')] == 'WYP' and i[1] == 0:
            i[1] = 1
            markeritems.append(LegendItem(label="Wypłata", renderers=[r], index=0))
        if r.name[:len('WZA')] == 'WZA' and i[2] == 0:
            i[2] = 1
            markeritems.append(LegendItem(label="WZA", renderers=[r], index=0))
        if r.name[:len('PRAWO')] == 'PRAWO' and i[3] == 0:
            i[3] = 1
            markeritems.append(LegendItem(label="Ustalenie praw", renderers=[r], index=0))

    legend = Legend(items=legenditems)
    markers = Legend(items=markeritems)

    p.add_layout(legend,'above')
    p.add_layout(markers,'below')
    p.legend.location = 'center'
    p.legend.orientation = 'horizontal'
    p.legend.click_policy = 'hide'
    p.legend.border_line_color = None
    p.axis.minor_tick_in = 0
    p.axis.minor_tick_out = 0
    #etytkiety osi
    p.xaxis.formatter = DatetimeTickFormatter(days=["%d %B %G"],
                                              months=["%b %G"],
                                              years = ['%Y'])
    p.xaxis.ticker = MonthsTicker(months=list(range(1,13)))
    p.xaxis.major_label_orientation = np.pi / 4
    p.add_tools(CrosshairTool())
    p.border_fill_color = None
    p.outline_line_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    return p

#--------------------------------
#ROI oraz koszt portfela w czasie
#--------------------------------

def roi_cost(width,height,title_text,title_align):
    df = pd.read_sql("""SELECT YEAR([P].[DATA]) AS [ROK], [P].[SEC_COST] AS [KOSZT], [PREMIUM].[PREMIUM] FROM
                        [PORTFEL_IN_TIME_VW] AS [P],
                        (SELECT YEAR([DATA]) AS [ROK], MAX([DATA]) AS [MAXDATA] FROM [PORTFEL_IN_TIME_VW] GROUP BY YEAR([DATA])) AS [DATES],
                        (SELECT [ROK], SUM([VAL]) AS [PREMIUM] FROM [PREMIUM_VW] GROUP BY [ROK]) AS [PREMIUM]
                        WHERE [P].[NAZWA] = 'Razem'
                        AND [P].[DATA] = [DATES].[MAXDATA]
                        AND YEAR([P].[DATA]) = [PREMIUM].[ROK]
                        OPTION (MAXRECURSION 0)""",conn)
    #wyliczenie ROI
    df['ROI'] = df['PREMIUM']/df['KOSZT']
    #dane
    source = ColumnDataSource(data=df)
    p = figure(x_range=(min(df['ROK'])-0.35, max(df['ROK'])+0.35), plot_width=width, plot_height=height,
                toolbar_location=None)

    #oś dodatkowa dla ROI
    p.extra_y_ranges = {'roi': Range1d(start=0, end=max(df['ROI'])*1.5)}
    p.add_layout(LinearAxis(y_range_name='roi'), 'right')

    #budowa słupków
    p.vbar(name = 'KOSZT', x='ROK', top='KOSZT', width=0.5, color = colors[0], source = source)
    #linia z ROI
    p.line(name = 'ROI', x='ROK', y='ROI', line_width=5, color = colors[1], source = source, y_range_name = 'roi')

    hover = HoverTool(
    tooltips=[
        ('', '@ROK'),
        ('Koszt', '@KOSZT{0,0.00}'),
        ('Przychody', '@PREMIUM{0,0.00}'),
        ('ROI', '@ROI{0,0.00%}')
        ],
        mode = 'mouse',
        renderers = [p.renderers[0]]
    )
    p.add_tools(hover)
    p.yaxis[0].formatter = NumeralTickFormatter(format="0,00 a")
    p.yaxis[0].axis_label= "Koszt portfela"
    p.yaxis[1].formatter = NumeralTickFormatter(format="0.00 %")
    p.yaxis[1].axis_label= "Stopa zwrotu"
    p.xaxis.ticker.max_interval = 1
    p.xaxis.ticker.min_interval = 1    
    p.axis.minor_tick_in = 0
    p.axis.minor_tick_out = 0
    p.ygrid.grid_line_width = 0
    p.xgrid.grid_line_width = 0
    p.outline_line_color = None
    p.border_fill_color = None
    #tytuł
    p.title.text = title_text
    p.title.align = title_align
    return p

#-----------------
#droga do wolności
#-----------------

def freedom(width,height):

    df = pd.read_sql("""SELECT YEAR([P].[DATA]) AS [ROK], [P].[SEC_COST] AS [KOSZT], [P].[SEC_VALUE] AS [WARTOŚĆ], [PREMIUM].[PREMIUM]
                    FROM
                    [PORTFEL_IN_TIME_VW] AS [P],
                    (SELECT YEAR([DATA]) AS [ROK], MAX([DATA]) AS [MAXDATA] FROM [PORTFEL_IN_TIME_VW] GROUP BY YEAR([DATA])) AS [DATES],
                    (SELECT [ROK], SUM([VAL]) AS [PREMIUM] FROM [PREMIUM_VW] GROUP BY [ROK]) AS [PREMIUM]
                    WHERE [P].[NAZWA] = 'Razem'
                    AND [P].[DATA] = [DATES].[MAXDATA]
                    AND YEAR([P].[DATA]) = [PREMIUM].[ROK]
                    OPTION (MAXRECURSION 0)""",conn)

    df['COLOR'] = colors[0]
    target = {'ROK': 2050, 'KOSZT': np.nan, 'WARTOŚĆ': 1000000, 'PREMIUM': 24000, 'COLOR': colors[1]}
    df = df.append(target, ignore_index=True)
    
    #dane
    source = ColumnDataSource(data=df)

    p1 = figure(x_range=(min(df['ROK'])-0.5, max(df['ROK'])+0.5), y_range=(0, 1000050), plot_width=int(width/2), plot_height=height, toolbar_location=None)

    p1.vbar(name = 'Wartość', x='ROK', top='WARTOŚĆ', width=0.5, color = 'COLOR', source = source)

    p1.yaxis.ticker.max_interval = 100000
    p1.yaxis.ticker.min_interval = 100000
    p1.yaxis.formatter = NumeralTickFormatter(format="0,00 a")
    p1.yaxis.axis_label = 'Droga do miliona'

    p2 = figure(x_range=(min(df['ROK'])-0.5, max(df['ROK'])+0.5), y_range=(0, 25000), plot_width=int(width/2), plot_height=height, toolbar_location=None)

    p2.vbar(name = 'Premium', x='ROK', top='PREMIUM', width=0.5, color = 'COLOR', source = source)

    p2.yaxis.ticker.max_interval = 1000
    p2.yaxis.ticker.min_interval = 1000
    p2.yaxis.formatter = NumeralTickFormatter(format="0,00 a")
    p2.yaxis.axis_label = 'Droga do 2k miesięcznie'

    for p in [p1,p2]:    
        p.xaxis.ticker.max_interval = 1
        p.xaxis.ticker.min_interval = 1
        p.xaxis.major_label_orientation = np.pi / 4
        p.axis.minor_tick_in = 0
        p.axis.minor_tick_out = 0
        p.ygrid.grid_line_width = 0
        p.xgrid.grid_line_width = 0
        p.outline_line_color = None
        p.border_fill_color = None

    return layout(children=[
                        [p1,p2]
                        ]
                    )

#-------------------------------------------------------------------------------------------------------------------
#INNE
#-------------------------------------------------------------------------------------------------------------------

#-----
#baner
#-----

def baner(width,height,img):
    div = Div(text="""<img src="""+img+""" alt="Baner" width=""" + str(width) + """ height=""" + str(height) + """>""",
    width=width, height=height)
    return div

#------
#napisy
#------

def text(width,height,txt):
    div = Div(text=txt,
    width=width, height=height)
    return div

#-----------
#placeholder
#-----------

def placeholder(width, height):
    plc = Spacer(width=width,height=height)
    return plc

#-------------------------------------------------------------------------------------------------------------------
#KOD GŁÓWNY
#-------------------------------------------------------------------------------------------------------------------

tstart = dt.datetime.now()
print('Start: ' + tstart.strftime('%H:%M:%S'))

#ustalenie czy aktualizacja ma być pełna czy ma pomijać dywidendy i obligacje
dict_builders()
confirm = dict_prompt("Czy aktualizować informacje o dywidendach i obligacjach?",yn_dict)
if confirm == 1:
    for codename in ['DividendHistory','Catalysator']:
        os.system('C:\\Users\\Ukasz\\Documents\\Firma\\GPW\\' + codename + '.py')   

#aktualizacja danych w bazie
for codename in ['InputGPW','InputNC','InputCATALYST','PlannedDividends','PlannedPremium']:
    os.system('C:\\Users\\Ukasz\\Documents\\Firma\\GPW\\' + codename + '.py')

print('Łączenie z bazą...')
#połączenie z bazą
conn_str = ('Driver={SQL Server};'
            'Server=LEGION\MS3DOIT;'
            'Database=GPW;'
            'Trusted_Connection=yes;')
conn = pyodbc.connect(conn_str)

print('Przygotowywanie wykresów...')
#globalne ustawienia kolorów
colors = ["#165493", "#8D8D8D", "#CC0000"] ##niebieski, szary, czerwony
footer_text = '3Doit.pl Opracowane w języku Python przy użyciu bibliotek Pandas oraz Bokeh, wrzesień 2020'

#konstrukcja zakładek
print('Zakładka 1...')
lay1a = layout(
        children=[
            [treemap(810,260,'Zmiany kursów w ostatnim dniu notowań','center')],
            [wallettable(790,400), placeholder(20,400)]
        ]
    )

lay1 = layout(
        children=[
            [baner(1200,100,'baner_portfel.png'), text(200,100,'Ostatnia aktualizacja notowań:'), updatetable(200,100)],
            [lay1a, walletintime(790,680,'Wartość portfela w czasie','center')],
            [text(1600,20,footer_text)]
        ]
    )

print('Zakładka 2...')
lay2aa = layout(
        children=[
            [text(200,20,'Nadchodzące dywidendy i odsetki')],
            [exppremium(200,300)]
            ]
    )

lay2a = layout(
        children=[
            [premiumintime(720,340,'Przychody z dywidend i odsetek po latach','center'), lay2aa],
            [reinvest(400,340,'Źródła finasowania portfela','center'), premiumfreq(520,330,'Historyczny rozkład wypłat','center')]
        ]
    )

lay2 = layout(
        children=[
            [baner(1600,100,'baner_rentier.png')],
            [shellchart(680,680,'Przychody z dywidend i odsetek według źródła','center'), lay2a], 
            [text(1600,20,footer_text)]
        ]
    )

print('Zakładka 3...')
lay3a = layout(
        children=[
            [text(590,20,'Możliwości zakupu akcji spółek dywidendowych')],
            [divdeamon(590,170)],
            [text(590,20,'Możliwości zakupu obligacji')],
            [catalysator(590,450)],
            [placeholder(590,20)]
            ]
    )

lay3 = layout(
        children=[
            [baner(1600,100,'baner_wspomaganie.png')],
            [dividend_analysis(1000,680,'Historia notowań spółek z bliskim terminem ustalenia praw do dywidendy','center'), placeholder(10,680), lay3a],
            [text(1600,20,footer_text)]
        ]
    )

print('Zakładka 4...')
lay4 = layout(
        children=[
            [baner(1600,100,'baner_wynik.png')],
            [outcome(300,340),roi_cost(1300,340,'ROI w czasie','center')],
            [freedom(1600,340)],
            [text(1600,20,footer_text)]
            ]
    )

tab1 = Panel(child=lay1, title="Portfel")
tab2 = Panel(child=lay2, title="Jak zostaliśmy rentierami")
tab3 = Panel(child=lay3, title="Wspomaganie")
tab4 = Panel(child=lay4, title="Wynik")
             
#konstrukcja dashboardu
print('Finalizacja...')
tabs = Tabs(tabs=[tab1, tab2, tab3, tab4])

#zapis
output_file("Dashboard.html", title = "Inwestycje")
save(tabs)

tstop = dt.datetime.now()
tdiff = tstop - tstart
hours, reminder = divmod(tdiff.total_seconds(), 3600)
minutes, seconds = divmod(reminder, 60)
print('Koniec: ' + tstop.strftime('%H:%M:%S'))
print('Zadanie zakończone w czasie %02i:%02i:%02i' % (hours, minutes, seconds))
