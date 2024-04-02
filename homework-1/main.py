"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import datetime


def get_new_cell(old_cell):
    """
    Преобразует строки в формат int и date, если встречает
    """

    cell_text = old_cell.strip('"')

    if cell_text.isdigit():
        new_cell = int(cell_text)
    elif check_is_it_date(cell_text):
        dt = datetime.datetime.strptime(cell_text, '%Y-%m-%d')
        new_cell = dt.date()
    else:
        new_cell = cell_text

    return new_cell


def check_is_it_date(text):
    """
    Проверяет на соответствие формату даты %Y-%m-%d
    """

    return (len(text) == 10 and
            text[4] == '-' and
            text[7] == '-' and
            (text[:4] + text[5:7] + text[8:]).isdigit())


def split_row(text):
    """
    Разделяет строку по запятым
    Если в тексте есть запятая, а затем пробел, это будет одна ячейка


    # Для работы с csv файлами можно воспользоваться библиотекой "csv".
    # Пример:
    with connection.cursor() as cursor:
        with open('north_data\\employees_data.csv') as csv_file:
            header = next(csv_file) # служит для пропуска первой строчки, которая является заголовком.
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                query = '''INSERT INTO employees
                VALUES (%s, %s, %s, %s, %s, %s)'''
                cursor.execute(query, row)
    """

    row = []
    index = text.find(',')
    sub_text = text

    while index >= 0:
        # пока находим запятые:
        # если кусок не начинается с пробела, это новая ячейка
        # иначе добавляем его к предыдущей ячейке

        sub_row = sub_text[:index]
        if len(sub_row) == 0 or sub_row[0] != ' ':
            row.append(sub_row)
        else:
            row[-1] = ",".join([row[-1], sub_row])

        sub_text = sub_text[index+1:]
        index = sub_text.find(',')

    # последний кусок, когда уже не нашлось запятых
    if len(sub_text) == 0 or sub_text[0] != ' ':
        row.append(sub_text)
    else:
        row[-1] = ",".join([row[-1], sub_text])

    return row


def get_data_from_file(filename):
    """
    Получает таблицу из файла (-> матрица ячеек, список списков)
    """

    with open(filename) as f:
        text = f.read()

    rows = text.split('\n')
    data = []

    for row in rows:
        # split_row - разделяет строку по запятым
        # get_new_cell - преобразует в формат int и date
        cells = [get_new_cell(cell) for cell in split_row(row)]

        data.append(cells)

    return data[1:-1]


def fill_data_in_db():
    """
    Обращается к файлам
    Записывает данные в БД
    """

    customers_data = get_data_from_file('./north_data/customers_data.csv')
    employees_data = get_data_from_file('./north_data/employees_data.csv')
    orders_data = get_data_from_file('./north_data/orders_data.csv')

    customers_query = "INSERT INTO customers VALUES (%s, %s, %s)"
    employees_query = "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)"
    orders_query = "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)"

    conn_params = {
        "host": "localhost",
        "database": "north",
        "user": "postgres",
        "password": "admin"
    }

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.executemany(customers_query, customers_data)
            cur.executemany(employees_query, employees_data)
            cur.executemany(orders_query, orders_data)

            # вывод на экран
            #cur.execute("SELECT * FROM customers, employees, orders LIMIT 3")
            #rows = cur.fetchall()
            #for row in rows:
            #    print(row)

        conn.commit()

    return True


fill_data_in_db()

# tests

#data = get_data_from_file('./north_data/customers_data.csv')
#[print(row) for row in data]

#a = get_data_from_file('./north_data/orders_data.csv')
#[print(b) for b in a]

#a = get_data_from_file('./north_data/employees_data.csv')
#[print(b) for b in a]

#a = split_row('2,"Andrew","Fuller","Vice President, Sales","1952-02-19","Andrew received his BTS commercial in 1974 and a Ph.D. in international marketing from the University of Dallas in 1981.  He is fluent in French and Italian and reads German.  He joined the company as a sales representative, was promoted to sales manager in January 1992 and to vice president of sales in March 1993.  Andrew is a member of the Sales Management Roundtable, the Seattle Chamber of Commerce, and the Pacific Rim Importers Association."')
#a = split_row('10249,"TOMSP",6,"1996-07-05","Münster"')
#[print(b) for b in a]
#print(len(a))

###############
