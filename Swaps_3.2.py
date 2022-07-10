import pandas as pd, numpy as np, pandasql as ps, math, time
from datetime import datetime, timedelta
from colorama import Fore, init

pd.set_option('display.max_colwidth', None)

init(autoreset=True)
start_time = time.time()  # Подсчет времени работы программы
current_datetime = datetime.now().time().strftime('%H:%M:%S')  # Момент запуска программы
print('Время запуска:', current_datetime)


def time_period():
    m = (time.time() - start_time) // 60
    s = (time.time() - start_time) % 60
    if m >= 60:
        h = m // 60
        out = "Время: " + str(int(h)) + " ч " + str(int(m-h*60)) + " мин"
        return out
    else:
        out = "Время: " + str(int(m)) + " мин " + str(round(s)) + " сек"
        return out


### Выгрузка основных данных из csv
file_csv = r'C:\My_files\Yakimov Dmitry\Swaps\SQN_FISS1_ALL_2_X_202206101229.csv'
df0 = pd.read_csv(file_csv, delimiter=';', encoding='cp1251')

### Выгрузка столбца ORFOID таблицы rubars01.DEXTAB из csv
# q_ORFOID = """select ORFOID from rubars01.DEXTAB where [ORFOID] is not null"""
file_ORFOID = r'C:\My_files\Yakimov Dmitry\Swaps\DEXTAB_202206101251.csv'
df_ORFOID = pd.read_csv(file_ORFOID, delimiter=';', encoding='cp1251')

### Выгрузка данных по времени из SAS
file_time = r'C:\My_files\Yakimov Dmitry\Swaps\SAS_time.xlsx'
df_time = pd.read_excel(file_time, engine='openpyxl',
                        usecols=['DEAL_NUMBER', 'swap_wss', 'ORIG_ENTRY_TIME'],
                        sheet_name='SAS_time')
print(Fore.GREEN + 'Part 1 completed')


### Проверка и удаление строк с NaN
def no_NaN(df_time):
    for i in range(df_time.shape[0]):
        if np.isnan(df_time['swap_wss'][i]):
            df_time.drop(index=i, inplace=True)
    return df_time
no_NaN(df_time)


### Формирование столбца дата-время
def date_time():
    dt_list = []
    for i in range(df_time.shape[0]):
        year = int(str(df_time['swap_wss'][i])[0:4])
        month = int(str(df_time['swap_wss'][i])[4:6])
        day = int(str(df_time['swap_wss'][i])[6:8])

        if len(str(df_time['ORIG_ENTRY_TIME'][i])) < 10:
            hour = int(str(df_time['ORIG_ENTRY_TIME'][i])[0])
            minute = int(str(df_time['ORIG_ENTRY_TIME'][i])[1] + str(df_time['ORIG_ENTRY_TIME'][i])[3])
            second = int(str(df_time['ORIG_ENTRY_TIME'][i])[4:6])
        else:
            hour = int(str(df_time['ORIG_ENTRY_TIME'][i])[0:2])
            minute = int(str(df_time['ORIG_ENTRY_TIME'][i])[2] + str(df_time['ORIG_ENTRY_TIME'][i])[4])
            second = int(str(df_time['ORIG_ENTRY_TIME'][i])[5:7])

        dt = datetime(year, month, day, hour, minute, second)
        dt_list.append(dt)

    df_time.insert(df_time.shape[-1], 'date_time', dt_list, True)
    return df_time


### Сопоставление TIME с данными из DEXTAB
df_ORFOID = pd.merge(df_ORFOID, date_time(), left_on=['ORFOID'], right_on=['DEAL_NUMBER'], how='inner')
df_ORFOID.drop(['DEAL_NUMBER', 'swap_wss', 'ORIG_ENTRY_TIME'], axis=1, inplace=True)

### Выделение в датафрейм строк, у которых CNUM = 100000
q1 = """select * from df0 where [CNUM] = '100000'"""
df1 = ps.sqldf(q1, locals())
df1.index = range(0, df1.shape[0])

### Список клиентов, с которыми сопоставляем
clients_list = ['110428', '110176', '110063', '110001', '110009', '100407',
                '110420', '110029', '100254', '110081', '100249', '200300',
                '200382', '200218', '200506', '200001', '201317', '200567',
                '200286', '200585', '200062', '200169', '200002', '200101',
                '200225', '110438', '100135', '200010', '110256', '100126',
                '100024', '110018', '100021', '100022', '100124', '301882',
                '110581', '200193', '200155', '110432', '100006', '100139',
                '100113', '100142', '110145', '100058', '200140', '110583',
                '200021', '110586', '100149', '100062', '100050', '110628',
                '100164', '100129', '110122', '100168', '110592', '110127',
                '110251', '100060', '100037', '110461', '110474', '200237',
                '100112', '110249', '110632', '110259', '110378', '110588',
                '100045', '200118', '200424', '110472', '110392', '110633',
                '100136', '200216', '110102', '200341', '200030', '100147',
                '100109', '110095', '100094', '200246', '200613', '100025',
                '100117', '100122', '200475', '110028', '100182', '110634',
                '110416', '200627', '200129', '110391', '200469', '100144',
                '200028', '200282', '100151', '110285', '200309', '100143',
                '100145', '110503', '100040', '110578', '110440', '100176',
                '200435', '200197', '100191', '100131', '200393', '200505',
                '200552', '200059', '200535', '300098', 'BAAC5E', '100202',
                '110104', '301604', '200615', '200296', '200540', '100093',
                '110101', '300078', '100118', '200588', '200043', '200278',
                '200052', '200312', '110058', '200022', '200522', '100154',
                '200267', '110349', '200457']
clients_list.sort()

### Создание датафрейма без CNUM = 100000 и содержащих CNUM = clients_list
q2 = """select * from df0 where [CNUM] != '100000'"""
df2 = ps.sqldf(q2, locals())
df2 = df2[df2['CNUM'].isin(clients_list)]
df2.index = range(0, df2.shape[0])

### Добавление столбца EXRT1-EXRT
df1['EXRT1-EXRT'] = df1['EXRT1'] - df1['EXRT']
df2['EXRT1-EXRT'] = df2['EXRT1'] - df2['EXRT']

### Добавление столбца date_time
df1 = pd.merge(df1, df_ORFOID, left_on=['ORFOID'], right_on=['ORFOID'], how='inner')
df2 = pd.merge(df2, df_ORFOID, left_on=['ORFOID'], right_on=['ORFOID'], how='inner')
print(Fore.GREEN + 'Part 2 completed')
print(time_period())


### Функция расчета квартиля с проверкой на вхождение в интервал
def kvart(df_temp):
    def mean(*numbers):
        return float(sum(numbers)) / len(numbers)

    kwartil_min, kwartil_max = [], []
    df_temp = df_temp.sort_values(by=['EXRT1-EXRT'], ascending=True)
    df_temp.index = range(0, df_temp.shape[0])

    quotient = df_temp.shape[0] / 4
    if quotient % 4 == 0:
        min_ = mean(df_temp['EXRT1-EXRT'][int(quotient) - 1], df_temp['EXRT1-EXRT'][int(quotient)])
    else:
        min_ = df_temp['EXRT1-EXRT'][int(quotient)]

    product = df_temp.shape[0] * 0.75
    if product % 1 == 0:
        max_ = mean(df_temp['EXRT1-EXRT'][int(product) - 1], df_temp['EXRT1-EXRT'][int(product)])
    else:
        max_ = df_temp['EXRT1-EXRT'][int(product)]

    for k in range(df_temp.shape[0]):
        kwartil_min.append(min_)
        kwartil_max.append(max_)

    ### Проверка на вхождение цены в интервал
    col_True = []
    for n in range(df_temp.shape[0]):
        if (math.isclose(df_temp['EXRT1-EXRT'][n], min_) or (df_temp['EXRT1-EXRT'][n] > min_)) and \
                (math.isclose(df_temp['EXRT1-EXRT'][n], max_) or (df_temp['EXRT1-EXRT'][n] < max_)):
            col_True.append(True)
        else:
            col_True.append(False)

    df_temp.insert(df_temp.shape[1], 'kwartil_min', kwartil_min, True)
    df_temp.insert(df_temp.shape[1], 'kwartil_max', kwartil_max, True)
    df_temp.insert(df_temp.shape[1], 'col_True', col_True, True)
    return df_temp


##### Допустим, пользователь вводит любые часы, минуты, секунды
td_hour = int(input('Введите целое число - часы: '))
td_minute = int(input('Введите целое число - минуты: '))
td_second = int(input('Введите целое число - секунды: '))
out_of_day = str(input('Выходим за пределы суток? Введите yes/no: '))
command = out_of_day


def time_delta(td_hour, td_minute, td_second):
    td = timedelta(hours=td_hour, minutes=td_minute, seconds=td_second)
    return td


#### Если не выходить за пределы суток
if out_of_day == 'no':
    print(Fore.CYAN + 'Не выходим за пределы суток')

    ### Если проверка по времени вперед и назад
    def iter_to_back():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            date = df1['DDAT'][i]
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if abs(time_1 - time_2) <= time_delta(td_hour, td_minute, td_second) \
                        and df2['DDAT'][j] == date and df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df
    # print(iter_to_back().to_string())


    ### Если проверка по времени только вперед
    def iter_to():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            date = df1['DDAT'][i]
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if time_2 >= time_1 and time_2 - time_1 <= time_delta(td_hour, td_minute, td_second) and \
                        df2['DDAT'][j] == date and df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df
    # print(iter_to().to_string())


    ### Если проверка по времени только назад
    def iter_back():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            date = df1['DDAT'][i]
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if time_1 >= time_2 and time_1 - time_2 <= time_delta(td_hour, td_minute, td_second) and \
                        df2['DDAT'][j] == date and df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df
    # print(iter_back().to_string())


    ### Запуск конкретного сценария
    var = input("""Отбор по времени в обе стороны = both
Отбор по времени только вперед = to
Отбор по времени только назад = back?\n""")
    if var == 'both':
        name = 'both'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_both = iter_to_back()
        print(swap_both.to_string())
    elif var == 'to':
        name = 'to'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_to = iter_to()
        print(swap_to.to_string())
    elif var == 'back':
        name = 'back'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_back = iter_back()
        print(swap_back.to_string())
    else:
        print(Fore.RED + 'Введен некорректный параметр. Допустимо both/to/back. '
                         'Выполнение прекращается. Запустите программу заново')
        quit()
    print(f'Итерирование_{command}_{name} выполнено')
    print(time_period())


#### Если выходить за пределы суток
elif out_of_day == 'yes':
    print(Fore.CYAN + 'Выходим за пределы суток')

    def iter_to_back_2():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if abs(time_1 - time_2) <= time_delta(td_hour, td_minute, td_second) \
                        and df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df
    # print(iter_to_back_2().to_string())

    ### Если проверка по времени только вперед
    def iter_to_2():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if time_2 >= time_1 and time_2 - time_1 <= time_delta(td_hour, td_minute, td_second) and \
                        df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df

    # print(iter_to_2().to_string())

    ### Если проверка по времени только назад
    def iter_back_2():
        df = pd.DataFrame()

        for i in range(df1.shape[0]):
            df_one_row, df_temp = pd.DataFrame(), pd.DataFrame()
            n_days = df1['N_DAYS'][i]
            curr1 = df1['PUCY'][i]
            curr2 = df1['SLCY'][i]
            time_1 = df1['date_time'][i]

            for j in range(df2.shape[0]):
                time_2 = df2['date_time'][j]

                if time_1 >= time_2 and time_1 - time_2 <= time_delta(td_hour, td_minute, td_second) and \
                        df2['N_DAYS'][j] == n_days and \
                        ((df2['PUCY'][j] == curr1 and df2['SLCY'][j] == curr2) or
                         (df2['PUCY'][j] == curr2 and df2['SLCY'][j] == curr1)):
                    df_one_row = pd.DataFrame(data=df2.loc[j, :]).transpose()
                    df_temp = df_temp.append(df_one_row)
                    df_temp.index = range(0, df_temp.shape[0])

                ### Расчет квартиля и значений min и max
                if j == df2.shape[0] - 1:
                    if df_temp.shape[0] > 0:
                        kvart(df_temp)
                        df = df.append(kvart(df_temp))

        df = df.loc[~df['DEALID'].duplicated(keep='first')]
        df.index = range(0, df.shape[0])
        return df
    # print(iter_back().to_string())

    ### Запуск конкретного сценария
    var = input("""Отбор по времени в обе стороны = both
Отбор по времени только вперед = to
Отбор по времени только назад = back?\n""")
    if var == 'both':
        name = 'both'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_both = iter_to_back_2()
        print(swap_both.to_string())
    elif var == 'to':
        name = 'to'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_to = iter_to_2()
        print(swap_to.to_string())
    elif var == 'back':
        name = 'back'
        print(Fore.CYAN + f'Выбран вариант: {name}')
        swap_back = iter_back_2()
        print(swap_back.to_string())
    else:
        print(Fore.RED + 'Введен некорректный параметр. Допустимо both/to/back. '
                         'Выполнение прекращается. Запустите программу заново')
        quit()
    print(f'Итерирование_{command}_{name} выполнено')
    print(time_period())

else:
    print(Fore.RED + 'Введен некорректный параметр. Допустимо yes/no. '
                     'Выполнение прекращается. Запустите программу заново')
    quit()


print(Fore.GREEN + 'Part 3 completed')


### Запись данных в excel-файл
def file_writing():
    file_final = f'C:\\My_files\\Yakimov Dmitry\\Swaps\\the_swaps_3.2_{command}_{name}.xlsx'


    # def with_wb_df1_df2():
    #     df1.to_excel(wb, sheet_name='df1', index=False)
    #     sheet_df = wb.sheets['df1']
    #     sheet_df.autofilter(0, 0, df1.shape[0], df1.shape[1] - 1)
    #     sheet_df.set_column('C:C', 14)
    #     sheet_df.set_column('D:D', 10)
    #     sheet_df.set_column('J:J', 14)
    #     sheet_df.set_column('K:K', 17.6)
    #
    #     df2.to_excel(wb, sheet_name='df2', index=False)
    #     sheet_df = wb.sheets['df2']
    #     sheet_df.autofilter(0, 0, df2.shape[0], df2.shape[1] - 1)
    #     sheet_df.set_column('C:C', 14)
    #     sheet_df.set_column('D:D', 10)
    #     sheet_df.set_column('J:J', 14)
    #     sheet_df.set_column('K:K', 17.6)


    if name == 'both':
        with pd.ExcelWriter(file_final, engine='xlsxwriter') as wb:
            swap_both.to_excel(wb, sheet_name='swaps', index=False)
            sheet_df = wb.sheets['swaps']
            sheet_df.autofilter(0, 0, swap_both.shape[0], swap_both.shape[1] - 1)
            sheet_df.set_column('C:C', 14)
            sheet_df.set_column('D:D', 10)
            sheet_df.set_column('J:J', 14)
            sheet_df.set_column('L:M', 14)
            sheet_df.set_column('K:K', 17.6)
            # with_wb_df1_df2()

    elif name == 'to':
        with pd.ExcelWriter(file_final, engine='xlsxwriter') as wb:
            swap_to.to_excel(wb, sheet_name='swaps', index=False)
            sheet_df = wb.sheets['swaps']
            sheet_df.autofilter(0, 0, swap_to.shape[0], swap_to.shape[1] - 1)
            sheet_df.set_column('C:C', 14)
            sheet_df.set_column('D:D', 10)
            sheet_df.set_column('J:J', 14)
            sheet_df.set_column('L:M', 14)
            sheet_df.set_column('K:K', 17.6)
            # with_wb_df1_df2()

    elif name == 'back':
        with pd.ExcelWriter(file_final, engine='xlsxwriter') as wb:
            swap_back.to_excel(wb, sheet_name='swaps', index=False)
            sheet_df = wb.sheets['swaps']
            sheet_df.autofilter(0, 0, swap_back.shape[0], swap_back.shape[1] - 1)
            sheet_df.set_column('C:C', 14)
            sheet_df.set_column('D:D', 10)
            sheet_df.set_column('J:J', 14)
            sheet_df.set_column('L:M', 14)
            sheet_df.set_column('K:K', 17.6)
            # with_wb_df1_df2()

file_writing()
print(time_period())
print('End')











