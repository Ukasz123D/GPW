from selenium import webdriver
from dateutil import relativedelta
from getch import pause
import sys, bs4, requests, re, os, time, datetime, pyodbc, pandas as pd, numpy as np

#funkcja znajdująca wyrażenia regularne w treści strony
def parse_regex_all(RegexStr, WebsiteStr):
    out = None
    ParseStr = re.compile(RegexStr)
    out = ParseStr.findall(WebsiteStr)
    return out

def get_info(nazwa):
    res = requests.get('https://www.stockwatch.pl/gpw/' + nazwa + ',notowania,dywidendy.aspx')
    res.raise_for_status()
    resSoup = bs4.BeautifulSoup(res.text, 'html.parser')
    www = str(resSoup.find_all("table", id="DividendsTab"))
    resultlist = parse_regex_all('"stcm">(.?|.+)<|aspx">(.+)<\/a|c">(.?|.+)<\/',www)
    infodf = pd.DataFrame(columns=['NAZWA','DATA_WYP','VAL'])
    for i in range(0,len(resultlist),7):
        try:
            tempdf = pd.DataFrame()
            tempdf['NAZWA'] = [resultlist[i][1]]
            tempdf['DATA_WYP'] = [resultlist[i+5][0]]
            tempdf['VAL'] = [float(resultlist[i+1][2].replace(",",".").strip())]
            infodf = pd.concat([infodf,tempdf])
        except:
            print(resultlist)
    return infodf

#kod główny
tstart = datetime.datetime.now()
print('Start: ' + tstart.strftime('%H:%M:%S'))

#połączenie z bazą
print('Łączenie z bazą danych')
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=LEGION\MS3DOIT;'
                        'Database=GPW;'
                        'Trusted_Connection=yes;')

#odpalenie przeglądarki
print('Otwieram przeglądarkę.')
browser = webdriver.Chrome()
browser.set_window_size(0, 0)

#pobieranie danych
print('Znajdowanie listy notowanych spółek...')
df = pd.read_sql("""SELECT TRIM([NAZWA]) AS [NAZWA] FROM [CURR_PORTFEL_VW] WHERE [SEC_TYPE] = 0""",conn)
print('Szukam informacji kolejnych wypłatach dla poszczególnych emisji - to trochę potrwa.')
resdf = pd.DataFrame(columns=['NAZWA','DATA_WYP','VAL'])

print('Zbieranie danych o planowanych wypłatach...')
for i, row in df.iterrows():
    j = (i + 1) / (len(df.index))
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j))
    sys.stdout.flush()
    infodf = get_info(df['NAZWA'].values[i])
    resdf = pd.concat([resdf,infodf])

resdf = resdf[(resdf['DATA_WYP'] > datetime.datetime.now().strftime('%Y-%m-%d'))]

#zapisywnie tymczasowego pliku
resdf.to_csv('PremiumPlanned.csv', index = False)
print('\nTymczasowy plik csv z wynikami zapisany.')

cursor = conn.cursor()

#czyszczenie tabeli
print('Czyszczenie tabeli.')
cursor.execute('DELETE FROM [PREMIUM] WHERE [NAZWA] IN (SELECT [NAZWA] FROM [CURR_PORTFEL_VW] WHERE [SEC_TYPE] = 0)')
conn.commit()

#wgrywanie do bazy
print('Ładowanie do bazy.')
imp_df = pd.read_csv('PremiumPlanned.csv')
for index,row in imp_df.iterrows():
    cursor.execute('INSERT INTO dbo.[PREMIUM]([NAZWA],[DATA_WYP],[VAL]) VALUES (?,?,?)',
                    row['NAZWA'], 
                    row['DATA_WYP'], 
                    row['VAL'])
  
#usuwanie tymczasowego pliku
os.remove('PremiumPlanned.csv')

#zamykanie połączenia
conn.commit()
cursor.close()
conn.close()

print('Zamykam przeglądarkę.')
browser.close()
browser.quit()

print('Koniec. Wynik załadowany do tabeli PREMIUM. Tymczasowy plik usunięty.')
tstop = datetime.datetime.now()
tdiff = tstop - tstart
hours, reminder = divmod(tdiff.total_seconds(), 3600)
minutes, seconds = divmod(reminder, 60)
print('Stop: ' + tstop.strftime('%H:%M:%S'))
print('Zadanie wykonane w czasie: %02i:%02i:%02i' % (hours, minutes, seconds))
