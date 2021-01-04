from selenium import webdriver
from dateutil import relativedelta
from getch import pause
import sys, bs4, requests, re, os, time, datetime, pyodbc, pandas as pd, numpy as np

def date_check(date):
    if len(date) == 0:
        date = '1753-01-01'
    return date

def parse_regex_one(RegexStr, WebsiteStr):
    out = None
    ParseStr = re.compile(RegexStr)
    out = ParseStr.search(WebsiteStr)
    return out

def parse_regex_all(RegexStr, WebsiteStr):
    out = None
    ParseStr = re.compile(RegexStr)
    out = ParseStr.findall(WebsiteStr)
    return out

def get_info(nazwa, isin):
    res = requests.get('https://www.stockwatch.pl/gpw/' + nazwa + ',notowania,dywidendy.aspx')
    try:
        res.raise_for_status()
    except:
        print(nazwa)
    resSoup = bs4.BeautifulSoup(res.text, 'html.parser')
    www = str(resSoup.find_all("table", id="DividendsTab"))
    resultlist = parse_regex_all('"stcm">(.?|.+)<|aspx">(.+)<\/a|c">(.?|.+)<\/',www)
    infodf = pd.DataFrame(columns=['ISIN','NAZWA','DATA_PRAW','DATA_BEZDYW','DATA_WYP','DATA_WZA','VAL','RATE'])
    for i in range(0,len(resultlist),7):
        try:
            tempdf = pd.DataFrame()
            tempdf['ISIN'] = [isin.upper()]
            tempdf['NAZWA'] = [resultlist[i][1]]
            tempdf['DATA_PRAW'] = [date_check(resultlist[i+3][2])]
            tempdf['DATA_BEZDYW'] = [date_check(resultlist[i+4][2])]
            tempdf['DATA_WYP'] = [date_check(resultlist[i+5][0])]
            tempdf['DATA_WZA'] = [date_check(resultlist[i+6][2])]
            tempdf['VAL'] = [float(resultlist[i+1][2].replace(",",".").strip())]
            tempdf['RATE'] = [float(resultlist[i+2][2][:-1].replace(",",".").strip())]
            infodf = pd.concat([infodf,tempdf])
        except:
            print(resultlist)
    return infodf

#kod główny
print('Aktualizacja informacji o dywidendach...')

#połączenie z bazą
print('Łączenie z bazą danych')
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=LEGION\MS3DOIT;'
                        'Database=GPW;'
                        'Trusted_Connection=yes;')
#pobieranie danych
print('Ustalenie listy spółek...')
df = pd.read_sql("""SELECT DISTINCT TRIM([NAZWA]) AS [NAZWA], [ISIN]
                    FROM [GPW].[dbo].[NOTOWANIA_GPW]""",conn)

browser = webdriver.Chrome()
browser.set_window_size(0, 0)

resultdf = pd.DataFrame()

#zbieranie danych
print('Zbieranie danych...')
for i, row in df.iterrows():
    j = (i + 1) / (len(df.index) + 1)
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j))
    sys.stdout.flush()
    nazwa = df['NAZWA'].values[i].lower()
    isin = df['ISIN'].values[i].lower()
    newdf = get_info(nazwa, isin)
    resultdf = pd.concat([resultdf,newdf])

print('Zamykam przeglądarkę.')
browser.close()
browser.quit()

#zapisywnie tymczasowego pliku
tempdf = pd.DataFrame(resultdf)
tempdf.to_csv('DD.csv', index = False)
print('\nTymczasowy plik csv z wynikami zapisany.')

#czyszczenie tabeli
print('Czyszczenie tabeli.')
cursor = conn.cursor()
cursor.execute('DELETE FROM dbo.[DYWIDENDY]')
conn.commit()

#wgrywanie do bazy
print('Ładowanie do bazy....')
imp_df = pd.read_csv('DD.csv')
##imp_df = imp_df.dropna(subset=['ISIN', 'NAZWA', 'DATA_PRAW', 'DATA_BEZDYW', 'DATA_WYP'])
for index,row in imp_df.iterrows():
    cursor.execute('INSERT INTO dbo.[DYWIDENDY]([ISIN],[NAZWA],[DATA_PRAW],[DATA_BEZDYW],[DATA_WYP],[DATA_WZA],[VAL],[RATE]) VALUES (?,?,?,?,?,?,?,?)',
                    row['ISIN'], 
                    row['NAZWA'], 
                    row['DATA_PRAW'],
                    row['DATA_BEZDYW'],
                    row['DATA_WYP'],
                    row['DATA_WZA'],
                    row['VAL'],
                    row['RATE'])

#usuwanie tymczasowych dat w bazie
tables = ['DATA_PRAW','DATA_BEZDYW','DATA_WYP','DATA_WZA']
for table in tables:
    cursor.execute("UPDATE dbo.[DYWIDENDY] SET ["+table+"] = null WHERE ["+table+"] = '1753-01-01'")

#usuwanie tymczasowego pliku
os.remove('DD.csv')

#zamykanie połączenia
conn.commit()
cursor.close()
conn.close()

print('Koniec. Wynik załadowany do tabeli DYWIDENDY. Tymczasowy plik usunięty.')
