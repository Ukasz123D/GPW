import pyodbc
import datetime as dt
import re
from getch import pause

#zestaw funkcji do budowania słowników
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


    #budowa słownika dla kupna i sprzedaży
    buy_list = ("kupno","kup","kupilem","kupiłem","buy","k","b")
    sell_list = ("sprzedaż","sprzedaz","sprzedaj","sprzedałem","sprzedalem","sell","s")
    roi_list = ("dywidenda","d","odsetki","o")    
    
    global bs_dict
    bs_dict = {}

    for item in buy_list:
        bs_dict[item.upper()] = 0

    for item in sell_list:
        bs_dict[item.upper()] = 1

    for item in roi_list:
        bs_dict[item.upper()] = 2

    #budowa słownika dla rodzaju papieru wartościowego
    s_list = ("akcji","akcje","shares","akcja","share","a","s")
    b_list = ("obligacji","obligacje","obligacja","bond","bonds","o","b")

    global t_dict
    t_dict = {}

    for item in s_list:
        t_dict[item.upper()] = 0

    for item in b_list:
        t_dict[item.upper()] = 1

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

#funkcja pobierająca input of użytkownika w przypadku,gdy odpowiedź musi mieć określony format
def reg_prompt(desc, input_def, input_regex):
    print(desc)
    success = None
    while success is None:
        try:
            user_input = input(input_def+'\n')
            pattern = re.compile(input_regex)
            if bool(pattern.match(user_input)):
                success = True
                return user_input.upper()
            else:
                print('Wprowadzone informacje nie pasują do wymaganego formatu, spróbuj ponownie.')
        except:
            pass

#funkcja ładująca wynik inputów do bazy
def db_connect(data, nazwa, pozycja, sec_type, sec_count, sec_value, ipo_flag):
    conn = pyodbc.connect('Driver={SQL Server};'
			  'Server=3DOIT\MS3DOIT;'
			  'Database=GPW;'
			  'Trysted_Connection=yes;')
    cursor = conn.cursor()

    #wgrywanie do bazy
    cursor.execute("INSERT INTO GPW.dbo.TRANSAKCJE VALUES ('"+data+"','"+nazwa+"',"+str(pozycja)+","+str(sec_type)+","+str(sec_count)+","+str(sec_value)+","+str(ipo_flag)+")")
    conn.commit()
    
#-----------
#kod główny

#start
dict_builders()

print("Witaj!")

restart = True

while restart == True:
    try:
        
        confirm = dict_prompt("Rejestrujemy nową transakcję?",yn_dict)
        if confirm != 1:
            pause("Dzięki, do zobaczenia!")
            quit()

        #potwierdzenie rodzaju papieru wartościowego
        ttyp = dict_prompt("Dotyczyła akcji, czy obligacji?",t_dict)
        
        #potwierdzenie pozycji
        tpozycja = dict_prompt("Jakiego rodzaju była to transakcja?\n(Kupno, sprzedaż, dywidenda, odsetki)", bs_dict)

        if tpozycja == 2 and ttyp == 1:
            tpozycja = 3
        
        #potwierdzenie daty
        tdate = reg_prompt("Kiedy dokonałeś transakcji?",
                          "Podaj datę w formacie: rrrr-mm-dd.",
                          "[0-9]{4}-[0-9]{2}-[0-9]{2}")

        #potwierdzenie, czy było to IPO w przypadku kupna akcji
        if ttyp == 0 and tpozycja == 0:
            tipo = dict_prompt("Czy zakup odbył się w ramach IPO?", yn_dict)
        elif tpozycja not in (0,1):
            tipo = "NULL"
        else:
            tipo = 0

        #potwierdzenie nazwy papieru wartościowego
        tname = input("Podaj nazwę waloru.\n").upper()

        if tpozycja in (0,1):
            #potwierdzenie wolumenu
            tcount = reg_prompt("Jakiej liczby walorów dotyczyła transakcja?",
                                "Podaj liczbę całkowitą.",
                                "[0-9]+")

            #potwierdzenie ceny jednostkowej
            tvalue = reg_prompt("Podaj cenę jednostkową.",
                                "Podaj cenę za walor, używając kropki jako separatora dziesiętnego.",
                                "[0-9]+(\.[0-9]{1,2})?")
        else:
            tcount = ""
            tvalue = reg_prompt("Jaką kwotę otrzymałeś?",
                                "Podaj kwotę, używając kropki jako separatora dziesiętnego.",
                                "[0-9]+(\.[0-9]{1,2})?")
        
        #potwierdzenie transakji
        if tpozycja == 0:
            tpoz = "kupiłeś"
        elif tpozycja == 1:
            tpoz = "sprzedałeś"
        else:
            tpoz = "otrzymałeś"

        if ttyp == 0 and tpozycja in (0,1):
            ttp = "akcje"
        elif ttyp == 1 and tpozycja in (0,1):
            ttp = "obligacje"
        elif tpozycja == 2:
            ttp = "dywidendę od"
        else:
            ttp = "odsetki od"

        if tpozycja in (0,1):
            tcount = " w ilości " + tcount
            tval = "po cenie jednostkowej "+tvalue+" PLN."
        else:
            tcount = "NULL"
            tval = "o wartości "+tvalue+" PLN."

        print("Dnia "+tdate+" "+tpoz+" "+ttp+" "+tname+tcount+" "+tval)

        confirm = dict_prompt("Potwierdzasz?",yn_dict)

        if confirm == 0:
            print("To jeszcze raz...")
            raise
        elif confirm == 1:
            print("Transakcja zostanie załadowana do bazy.")
            db_connect(tdate, tname, tpozycja, ttyp, tcount, tvalue, tipo)
            print("Zrobione.\n")

        confirm = dict_prompt("Kolejna transakcja?", yn_dict)
        if confirm == 0:
            restart = False
            
    except:
       pass

print("Dzięki za skorzystanie!")
pause("Program zakończony, wciścij dowolny klawisz by wyjść.")
