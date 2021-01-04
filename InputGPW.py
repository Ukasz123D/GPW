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
            url_of_file = 'https://www.gpw.pl/archiwum-notowan?fetch=1&type=10&instrument=&date=' + str(chkdate)
            outfilename = str(chkdate)+'_akcje.xls'
            dwnfiles.append(outfilename)
            urllib.request.urlretrieve(url_of_file, outfilename) 

#____________________________________________________________________________________________________________________
#kod główny
print('Aktualizacja danych o wynikach sesji GPW...')

#połączenie z bazą
conn_str = ('Driver={SQL Server};'
            'Server=LEGION\MS3DOIT;'
            'Database=GPW;'
            'Trusted_Connection=yes;')
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

#zmienne pomocnicze i folder roboczy
os.chdir('C:\\Users\\Ukasz\\Documents\\Firma\\GPW\\')
masterfile = 'GPWMSSQLMasterFile.xls'
masterfilebck = 'GPWMSSQLMasterFile_backup.xls'

#utworzenie setu dla listy dni bez sesji, lista dni przechowywania w tabeli [DNI_BEZ_SESJI]
nsdset = set()
cursor.execute('SELECT [DATA] FROM [GPW].[dbo].[DNI_BEZ_SESJI]')
for row in cursor:
    nsdset.add(dt.date.fromisoformat(row.DATA))

#ustalenie daty ostatniej aktalizacji (ostatniego notowania) z tabeli z notowaniami w bazie
cursor.execute('SELECT MAX([DATA]) AS [DATA] FROM [GPW].[dbo].[NOTOWANIA_GPW]')

#jeżeli data nie istnieje - poczatek roku 2020
try:
    lastdate = dt.date.fromisoformat(cursor.fetchone()[0])
except:
    lastdate = dt.date.fromisoformat('2020-01-01')

#jeżeli przed 19:30 to ostatni dzień to wczoraj
currtime = dt.datetime.now()
checktime = currtime.replace(hour=19, minute=0, second=0, microsecond=0)
currdate = dt.date.today()
if currtime < checktime:
    currdate =  currdate + dt.timedelta(days=-1)

combfile = 'Combined_' + str(currdate)

print(lastdate)
print(currdate)

#ściąganie danych dla ustalonej listy dni
download_data(lastdate, currdate)

#ustalenie master listy ISIN + LP na podstawie danych w bazie
mf = pd.read_sql('SELECT MAX([LP]) as [LP], [ISIN] FROM [NOTOWANIA_GPW] GROUP BY [ISIN]',cnxn)

#utworzenie nowej ramki dla komsolidacji ściągniętych plików
newdf = pd.DataFrame()
j = 0

#pętla przez ściągnięte pliki
print('Aktualizacja i łączenie plików...')
for file in dwnfiles:
    print(file)
    j = j + 1
    df = pd.read_excel(file)
    #usunięcie zbędnych kolumn
    df = df.drop(['Waluta', 'Liczba Transakcji','Obrót','Liczba otwartych pozycji','Wartość otwartych pozycji','Cena nominalna'],axis=1)
    #zmiana wielkości znaków w nagłówkach na drukowane
    df.columns = df.columns.str.upper()
    df.insert(0, 'LP', '')
    #usunięcie polskich znaków z nagłówka
    df.rename(columns={"KURS ZAMKNIĘCIA":"KURS ZAMKNIECIA"}, inplace=True)
    #dla każdej spółki, znalezienie LP, dodanie 1, dopisanie do nowego zestawienia
    for i in range (0, df.shape[0]):
        #szukanie LP po ISIN, jeżeli brak - dopianie ISIN do master listy
        try:
            df.iat[i,0] = mf.lookup([mf[mf['ISIN'] == df.at[i,'ISIN']].index.item()],['LP'])[0] + j
        except:
            print('Nowa spółka: ' + df.at[i,'NAZWA'])
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
        cursor.execute('INSERT INTO dbo.[NOTOWANIA_GPW]([LP],[DATA],[NAZWA],[ISIN],[OTWARCIE],[MAX],[MIN],[ZAMKNIECIE],[ZMIANA],[WOLUMEN]) values (?,?,?,?,?,?,?,?,?,?)', 
                        row['LP'], 
                        row['DATA'], 
                        row['NAZWA'],
                        row['ISIN'],
                        row['KURS OTWARCIA'],
                        row['KURS MAX'],
                        row['KURS MIN'],
                        row['KURS ZAMKNIECIA'],
                        row['ZMIANA'],
                        row['WOLUMEN'])

    #usuwanie tymczasowego pliku
    print('Usuwanie pliku tymczasowego...')
    os.remove(combfile + '_MSSQL.csv')

print('Finalizacja...')
#aktualizacja tabeli notowania - aktualizacja nazw jezeli ulegly zmianie w czasie
#kwerenda wyszykująca listę ISIN dla których istnieje w bazie wiecej niż jedna nazwa + najnowszą nazwę dla danego ISIN
qry = """WITH [TWRONG] AS (SELECT [ISIN], COUNT([NAZWA]) AS CNT
        FROM
        (SELECT DISTINCT [ISIN], [NAZWA]
        FROM [NOTOWANIA_GPW]) AS TDST
        GROUP BY [ISIN]
        HAVING COUNT([NAZWA]) <> 1
        )

        SELECT [ISIN], [NAZWA] FROM [NOTOWANIA_GPW]
        WHERE [DATA] IN (SELECT MAX([DATA]) FROM [NOTOWANIA_GPW])
        AND [ISIN] IN (SELECT [ISIN] FROM [TWRONG])"""

#zapis wyniku kwerendy to tymczasowej ramki
tempdf = pd.read_sql(qry,cnxn)
tempdf['NAZWA'] = tempdf['NAZWA'].str.strip()
#pętla przez wyniki i wykonanie aktualizacji
for i in range (0, tempdf.shape[0]):
    qry = "UPDATE [NOTOWANIA_GPW] SET [NAZWA]='"+tempdf.iat[i,1]+"' WHERE [ISIN]='"+tempdf.iat[i,0]+"'"
    print(tempdf.iat[i,0]+' zmiana nazwy na '+tempdf.iat[i,1])
    cursor.execute(qry)

#zamykanie połączenia
cnxn.commit()
cursor.close()
cnxn.close()
