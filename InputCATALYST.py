import csv, os, urllib, requests
import datetime as dt
import pandas as pd
import pyodbc

#ściąganie plików z danymi z gpw.pl oraz przygotowanie importu do bazy danych

#procedura ściagania plików
def download_data(lastdate, currdate):
    print('Ściąganie danych...')
    global dwnfiles
    dwnfiles = []
    #ustalenie listy dni od ostatniej aktualizacji do dziś
    delta = currdate - lastdate + dt.timedelta(days=-1)
    for i in range(delta.days +1):
        chkdate = lastdate + dt.timedelta(days=i+1)
        if (chkdate.weekday() <=4 and chkdate not in nsdset): #pominięcie weekendu i dni bez sesji
            url_of_file = 'https://gpwcatalyst.pl/pub/CATALYST/statystyki/statystyki_dzienne/catalyst_' + str(chkdate).replace('-','') + '.xls'
            outfilename = str(chkdate)+'_obligacje.xls'
            dwnfiles.append(outfilename)
            urllib.request.urlretrieve(url_of_file, outfilename) 

#____________________________________________________________________________________________________________________
#kod główny
print('Aktualizacja danych o wynikach sesji CATALYST...')

#połączenie z bazą
conn_str = ('Driver={SQL Server};'
            'Server=LEGION\MS3DOIT;'
            'Database=GPW;'
            'Trusted_Connection=yes;')
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

#zmienne pomocnicze i folder roboczy
os.chdir('C:\\Users\\Ukasz\\Documents\\Firma\\GPW\\')

#utworzenie setu dla listy dni bez sesji, lista dni przechowywania w tabeli [DNI_BEZ_SESJI]
nsdset = set()
cursor.execute('SELECT [DATA] FROM [GPW].[dbo].[DNI_BEZ_SESJI]')
for row in cursor:
    nsdset.add(dt.date.fromisoformat(row.DATA))

#ustalenie daty ostatniej aktalizacji (ostatniego notowania) z tabeli z notowaniami w bazie
cursor.execute('SELECT MAX([DATA]) AS [DATA] FROM [GPW].[dbo].[NOTOWANIA_CATALYST]')

#jeżeli data nie istnieje - poczatek roku 2020
try:
    lastdate = dt.date.fromisoformat(cursor.fetchone()[0])
except:
    lastdate = dt.date.fromisoformat('2019-01-01')

#jeżeli przed 19:30 to ostatni dzień to wczoraj
currtime = dt.datetime.now()
checktime = currtime.replace(hour=19, minute=0, second=0, microsecond=0)
currdate = dt.date.today()
if currtime < checktime:
    currdate =  currdate + dt.timedelta(days=-1)

combfile = 'Combined_' + str(currdate)

#ściąganie danych dla ustalonej listy dni
download_data(lastdate, currdate)

#ustalenie master listy ISIN + LP na podstawie danych w bazie
mf = pd.read_sql('SELECT MAX([LP]) as [LP], [ISIN] FROM [NOTOWANIA_CATALYST] GROUP BY [ISIN]',cnxn)

#utworzenie nowej ramki dla komsolidacji ściągniętych plików
newdf = pd.DataFrame()
j = 0

#pętla przez ściągnięte pliki
print('Aktualizacja i łączenie plików...')
for file in dwnfiles:
    print(file)
    j = j + 1
    #tylko zakładka "instrumenty"
    df = pd.read_excel(file, 'instrumenty')
    #start na wierszy 11-tym
    df = df.loc[10:]
    #usunięcie zbędnych kolumn
    if len(df.columns) == 24:
        cols=[0,1,2,5,6,9,10,12,17,18,19,21,23]
    else:
        cols=[0,1,2,5,8,9,11,16,17,18,20,22]
    df = df.drop(df.columns[cols],axis=1)
    #dodanie nazw kolumn
    df.columns = ["QUANT","RYNEK","ISIN","NAZWA","KURS ZAMKNIECIA","ZMIANA","KURS OTWARCIA","KURS MIN","KURS MAX","WOLUMEN","WOLUMEN_PAKIET"]
    #print(df[["QUANT","RYNEK","ISIN","NAZWA"]])
    df["KURS OTWARCIA"] = pd.to_numeric(df["KURS OTWARCIA"].replace('---',0))
    df["KURS MIN"] = pd.to_numeric(df["KURS MIN"].replace('---',0))
    df["KURS MAX"] = pd.to_numeric(df["KURS MAX"].replace('---',0))
    df["WOLUMEN"] = pd.to_numeric(df["WOLUMEN"]) + pd.to_numeric(df["WOLUMEN_PAKIET"])
    #usunięcie wierszy pustych i danych z rynków BS
    IndexToDrop = df[(df['RYNEK']=='BS ASO')].index
    df.drop(IndexToDrop, inplace=True)
    IndexToDrop = df[(df['RYNEK']=='BS RR')].index
    df.drop(IndexToDrop, inplace=True)
    df.dropna(subset = ["ISIN"], inplace=True)
    #dodanie pól z liczbą porządkową i datą
    df.insert(0, 'LP', 0)
    df['DATA'] = dt.date.fromisoformat(file[:10])
    df = df.reset_index(drop=True)
    #dla każdej spółki, znalezienie LP, dodanie 1, dopisanie do nowego zestawienia
    for i in range (0, df.shape[0]):
        #szukanie LP po ISIN, jeżeli brak - dopianie ISIN do master listy
        try:
            df.iat[i,0] = mf.lookup([mf[mf['ISIN'] == df.at[i,'ISIN']].index.item()],['LP'])[0] + j
        except:
            print('New entity: ' + df.at[i,'NAZWA'])
            newline = [-j+1,df.at[i,'ISIN']]
            mf.loc[len(mf)] = newline
            df.iat[i,0] = mf.lookup([mf[mf['ISIN'] == df.at[i,'ISIN']].index.item()],['LP'])[0] + j
    #dopisanie aktualnego pliku do zestawienia
    newdf = newdf.append(df, ignore_index = True)
    #usunięcie procesowanego pliku
    os.remove(file)

#zapis tymczasowego pliku do uploadu
print('Zapis pliku do uploadu...')

if not newdf.empty:
    newdf.to_csv(combfile + '_MSSQL.csv', index = False)

    #import do bazy
    print('Importowanie...')
    imp_df = pd.read_csv(combfile + '_MSSQL.csv')
    for index,row in imp_df.iterrows():
        cursor.execute('INSERT INTO dbo.[NOTOWANIA_CATALYST]([LP],[DATA],[NAZWA],[ISIN],[OTWARCIE],[MAX],[MIN],[ZAMKNIECIE],[ZMIANA],[WOLUMEN],[QUANT]) values (?,?,?,?,?,?,?,?,?,?,?)', 
                        row['LP'], 
                        row['DATA'], 
                        row['NAZWA'],
                        row['ISIN'],
                        row['KURS OTWARCIA'],
                        row['KURS MAX'],
                        row['KURS MIN'],
                        row['KURS ZAMKNIECIA'],
                        row['ZMIANA'],
                        row['WOLUMEN'],
                        row['QUANT'])

    #usuwanie tymczasowego pliku
    print('Usuwanie pliku tymczasowego...')
    os.remove(combfile + '_MSSQL.csv')

print('Finalizacja...')
#aktualizacja tabeli notowania - aktualizacja nazw jezeli ulegly zmianie w czasie
#kwerenda wyszykująca listę ISIN dla których istnieje w bazie wiecej niż jedna nazwa + najnowszą nazwę dla danego ISIN
qry = """WITH [TWRONG] AS (SELECT [ISIN], COUNT([NAZWA]) AS CNT
        FROM
        (SELECT DISTINCT [ISIN], [NAZWA]
        FROM [NOTOWANIA_CATALYST]) AS TDST
        GROUP BY [ISIN]
        HAVING COUNT([NAZWA]) <> 1
        )

        SELECT [ISIN], [NAZWA] FROM [NOTOWANIA_CATALYST]
        WHERE [DATA] IN (SELECT MAX([DATA]) FROM [NOTOWANIA_CATALYST])
        AND [ISIN] IN (SELECT [ISIN] FROM [TWRONG])"""

#zapis wyniku kwerendy to tymczasowej ramki
tempdf = pd.read_sql(qry,cnxn)
tempdf['NAZWA'] = tempdf['NAZWA'].str.strip()
#pętla przez wyniki i wykonanie aktualizacji
for i in range (0, tempdf.shape[0]):
    qry = "UPDATE [NOTOWANIA_CATALYST] SET [NAZWA]='"+tempdf.iat[i,1]+"' WHERE [ISIN]='"+tempdf.iat[i,0]+"'"
    print(tempdf.iat[i,0]+' zmiana nazwy na '+tempdf.iat[i,1])
    cursor.execute(qry)

#zamykanie połączenia
cnxn.commit()
cursor.close()
cnxn.close()
