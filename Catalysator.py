from selenium import webdriver
from dateutil import relativedelta
from getch import pause
import sys, bs4, requests, re, os, time, datetime, pyodbc, pandas as pd

#________________________________________________________________________________________________________________
#definicja funkcji

#funkcja znajdująca najbliższą datę do obecnej w ramach listy dat
def next_date(dates): 
    rdt = None
    for dt in dates:
        if dt >= datetime.datetime.now().strftime('%Y-%m-%d'):
            rdt = dt
            break
    if rdt == None:
        return '1753-01-01'
    else:
        return rdt

#funkcja znajdująca wyrażenia regularne w treści strony
def parse_regex(RegexStr, WebsiteStr):
    out = None
    ParseStr = re.compile(RegexStr)
    out = ParseStr.search(WebsiteStr)
    return out

#funkcja zbierająca informacje ze strony dedykowanej konkretnej emisji obligacji
def get_info(obkod):
    res = requests.get('https://obligacje.pl/pl/obligacja/' + obkod)
    res.raise_for_status()
    resSoup = bs4.BeautifulSoup(res.text, 'html.parser')
    #szukanie podstawowych informacji
    www = str(resSoup.select('tr'))   
    #nazwa eminenta
    emitent = parse_regex('(<th>Emitent:</th>\n<td>)(.*)(<)',www).group(2)
    #wartosc nominalna obligacji oraz waluta
    nom = parse_regex('(<th>Wartość nominalna:</th>\n<td>)(.*)(<)',www)
    nominalnaRegex = re.compile('(\d+)(\D{3})')
    nominalna = nominalnaRegex.search(nom.group(2).replace(" ", "")).group(1)
    waluta = nominalnaRegex.search(nom.group(2).replace(" ", "")).group(2)
    #oprocentowanie
    op = parse_regex('(<th>Oprocentowanie:</th>\n<td>)(.*)(<)',www)
    oproRegex = re.compile('(stałe|zmienne|obligacje zerokuponowe)(\s)(.*)')
    typ = oproRegex.search(op.group(2)).group(1)
    oprocentowanie = oproRegex.search(op.group(2)).group(3)
    oprocentowanie = oprocentowanie.replace('  ',' ')
    #oprocentowanie bieżące
    opbiezace = parse_regex('(<th>Oprocentowanie w bieżącym okresie:</th>\n<td>)(.*)(<)',www).group(2)
    opbiezace = float(opbiezace.replace('%','').replace(',','.'))
    #szukanie dat okresów odsetkowych, ustalenia praw i wykupu oraz powiązanych
    www = resSoup.select('ul')
    datesRegex = re.compile('\d+-\d{2}-\d{2}')
    if typ != 'obligacje zerokuponowe':
        for index, f in enumerate(www):
            if str(f)[:8] == '<ul><li>':
                i = index
                break
        odsdates = datesRegex.findall(str(www[i])) #dni okresów odsetkowych
        uprdates = datesRegex.findall(str(www[i+1])) #dni ustalenia praw
        wypdates = datesRegex.findall(str(www[i+2])) #dni wypłaty
        wykdate = datesRegex.search(str(www[i+3])) #data wykupu
        wyk_date = str(wykdate[0])
        period_cnt = str(len(odsdates)) #ilość okresów odsetkowych
        left_pay_cnt = 0 #ilość pozostałych wypłat
        for wypdate in wypdates:
            if wypdate >= datetime.datetime.now().strftime('%Y-%m-%d'):
                left_pay_cnt += 1
        #częstotliwość jako ilośc miesięcy między okresami odsetkowymi gdzieś wewnątrz listy okresów odsetkowych - często drugi okres jest szybciej niż pozostałe
        freq = relativedelta.relativedelta(datetime.datetime.strptime(odsdates[2],'%Y-%m-%d'),datetime.datetime.strptime(odsdates[1],'%Y-%m-%d')).months
        next_wyp = next_date(wypdates) #najbliższa wypłata
        next_upr = next_date(uprdates) #najbliższa data ustalenia praw do odsetek
    else: #alternatywne przypisanie wartości jeżeli obligacje są zerokuponowe
        wykdate = datesRegex.search(str(www[5]))
        wyk_date = str(wykdate[0])
        period_cnt = '1'
        left_pay_cnt = '1'
        freq = 1
        next_wyp = wyk_date
        next_upr = wyk_date
    #rezultat wyszukiwania
    try:
        wyn_list = [emitent,
                    obkod,
                    nominalna,
                    waluta,
                    typ,
                    oprocentowanie,
                    opbiezace,
                    period_cnt,
                    str(freq),
                    str(left_pay_cnt),
                    next_wyp,
                    next_upr,
                    wyk_date]
    except Exception as e:
        print(e)
    return wyn_list

#_____________________________________________________________________________________________________________
#kod główny

print('Aktualizacja informacji o obligacjach...')

#odpalenie przeglądarki
print('Otwieram przeglądarkę.')
browser = webdriver.Chrome()
browser.set_window_size(0, 0)

#znalezenie listy aktualnie notowanych na rynku Catalyst obligacji korporacyjnych
browser.get('https://gpwcatalyst.pl/notowania-obligacji-obligacje-korporacyjne')
print('\nZnajduję listę emisji obligacji korporacyjnych - aktualnie notowanych na rynku Catalyst.')
ob_list = browser.find_elements_by_xpath("//*[contains(@href, 'o-instrumentach-instrument?nazwa=')]")
print('Znaleziono',len(ob_list)-1,'emisji.')

#konstrukcja wyniku wyszukiwania
wynik = []

print('Szukam informacji o poszczególnych emisjach - to trochę potrwa.')
for ob in ob_list[:-1]:
    j = (ob_list.index(ob) + 1) / (len(ob_list)-1)
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j))
    sys.stdout.flush()
    info = get_info(ob.text)
    wynik.append(info)

#zapisywnie tymczasowego pliku
newdf = pd.DataFrame(wynik)
newdf.to_csv('CATALYSATOR.csv', index = False)
print('\nTymczasowy plik csv z wynikami zapisany.')

print('Łączenie z bazą.')
#połączenie z bazą
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LEGION\MS3DOIT;'
                      'Database=GPW;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

#czyszczenie tabeli
print('Czyszczenie tabeli.')
cursor.execute('DELETE FROM dbo.[CATALYSATOR]')
conn.commit()

#wgrywanie do bazy
print('Ładowanie do bazy.')
imp_df = pd.read_csv('CATALYSATOR.csv')
for index,row in imp_df.iterrows():
    cursor.execute('INSERT INTO dbo.[CATALYSATOR]([NAZWA],[ISIN],[WART_NOM],[WALUTA],[TYP_OPR],[BAZA_OPR],[CURR_OPR],[LICZBA_OKRESOW],[FREQ_WYPLAT],[LICZBA_POZ_OKRESOW],[NEXT_PAY],[NEXT_PAY_PRAWA],[WYKUP]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    row[0], 
                    row[1], 
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12])

#usuwanie tymczasowych dat w bazie
cursor.execute("UPDATE dbo.[CATALYSATOR] SET [NEXT_PAY_PRAWA] = null WHERE [NEXT_PAY_PRAWA] = '1753-01-01'")
cursor.execute("UPDATE dbo.[CATALYSATOR] SET [NEXT_PAY] = null WHERE [NEXT_PAY] = '1753-01-01'")
cursor.execute("UPDATE dbo.[CATALYSATOR] SET [WYKUP] = null WHERE [WYKUP] = '1753-01-01'")
    
#usuwanie tymczasowego pliku
os.remove('CATALYSATOR.csv')

#zamykanie połączenia
conn.commit()
cursor.close()
conn.close()

print('Zamykam przeglądarkę.')
browser.close()
browser.quit()

print('Koniec. Wynik załadowany do tabeli CATALYSATOR. Tymczasowy plik usunięty.')
