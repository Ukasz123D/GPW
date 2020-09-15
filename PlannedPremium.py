from selenium import webdriver
from dateutil import relativedelta
from getch import pause
import sys, bs4, requests, re, os, time, datetime, pyodbc, pandas as pd, numpy as np

#________________________________________________________________________________________________________________
#definicja funkcji

#funkcja znajdująca wyrażenia regularne w treści strony
def parse_regex(RegexStr, WebsiteStr):
    out = None
    ParseStr = re.compile(RegexStr)
    out = ParseStr.search(WebsiteStr)
    return out

#funkcja zbierająca informacje ze strony dedykowanej konkretnej emisji obligacji
def get_info(obkod):
    infodf = pd.DataFrame(columns=['NAZWA','DATA_WYP','VAL'])
    res = requests.get('https://obligacje.pl/pl/obligacja/' + df['NAZWA'].values[i])
    res.raise_for_status()
    resSoup = bs4.BeautifulSoup(res.text, 'html.parser')
    #szukanie podstawowych informacji
    www = str(resSoup.select('tr'))   
    #wartosc nominalna obligacji oraz waluta
    nom = parse_regex('(<th>Wartość nominalna:</th>\n<td>)(.*)(<)',www)
    nominalRegex = re.compile('(\d+)(\D{3})')
    nominal = float(nominalRegex.search(nom.group(2).replace(" ", "")).group(1))
    #oprocentowanie bieżące
    val = parse_regex('(<th>Oprocentowanie w bieżącym okresie:</th>\n<td>)(.*)(<)',www).group(2)
    #szukanie dat okresów odsetkowych, ustalenia praw i wykupu oraz powiązanych
    www = resSoup.select('ul')
    datesRegex = re.compile('\d+-\d{2}-\d{2}')
    for index, f in enumerate(www):
        if str(f)[:8] == '<ul><li>':
            k = index
            break
    #dni wypłaty
    paydates = datesRegex.findall(str(www[k+2]))
    #częstotliwość jako ilośc miesięcy między okresami wypłat gdzieś wewnątrz listy dat wypłat - często drugi okres jest szybciej niż pozostałe
    freq = relativedelta.relativedelta(datetime.datetime.strptime(paydates[2],'%Y-%m-%d'),datetime.datetime.strptime(paydates[1],'%Y-%m-%d')).months
    val = round(float(val.replace('%','').replace(',','.'))/12*freq/100 * nominal,2)
    #rezultat wyszukiwania
    for p in range(0,len(paydates)):
        try:
            tempdf = pd.DataFrame()
            tempdf.at[p, 'NAZWA'] = df['NAZWA'].values[i]
            tempdf.at[p, 'DATA_WYP']= paydates[p]
            tempdf.at[p, 'VAL']= val
            infodf = pd.concat([infodf,tempdf])
        except:
            pass
    return infodf

#_____________________________________________________________________________________________________________
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
print('Znajdowanie listy posiadanych obligacji...')
df = pd.read_sql("""SELECT TRIM([NAZWA]) AS [NAZWA] FROM [CURR_PORTFEL_VW] WHERE [SEC_TYPE] = 1""",conn)
print('Szukam informacji kolejnych wypłatach dla poszczególnych emisji - to trochę potrwa.')
resdf = pd.DataFrame(columns=['NAZWA','DATA_WYP','VAL'])

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
cursor.execute('DELETE FROM [PREMIUM] WHERE [NAZWA] IN (SELECT [NAZWA] FROM [CURR_PORTFEL_VW] WHERE [SEC_TYPE] = 1)')
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
