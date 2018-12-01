import pandas as pd
import numpy as np


# Унификация имен признаков в редакциях Top500
def top500_attributes_rename(df):
    for col in df.columns:
        if col == 'Processors':
            df.rename(columns={'Processors': 'Total Cores'}, inplace=True)
        if col == 'Cores':
            df.rename(columns={'Cores': 'Total Cores'}, inplace=True)
        if col == 'Rmax':
            df.rename(columns={'Rmax': 'RMax'}, inplace=True)
        if col == 'Rpeak':
            df.rename(columns={'Rpeak': 'RPeak'}, inplace=True)


# Унификация названий стран
def countries_unification(df):
    col = ''
    if 'Country' in df.columns:             # Country - признак в редакциях Top500 и HID
        col = 'Country'
    elif 'Country Name' in df.columns:      # Country Name - признак в TWB
        col = 'Country Name'
    if col == '':
        return

    df.loc[df[col] == 'Korea, Rep.', col] = 'South Korea'
    df.loc[df[col] == 'Korea, South', col] = 'South Korea'
    df.loc[df[col] == 'Saudia Arabia', col] = 'Saudi Arabia'
    df.loc[df[col] == 'Russia', col] = 'Russian Federation'
    df.loc[df[col] == 'Vietnam', col] = 'Viet Nam'
    df.loc[df[col] == 'Macau', col] = 'Macao'
    df.loc[df[col] == 'Syria', col] = 'Syrian Arab Republic'
    df.loc[df[col] == 'Trinidad And Tobago', col] = 'Trinidad and Tobago'
    df.loc[df[col] == 'Palestine', col] = 'Palestine'
    df.loc[df[col] == "Cote D'Ivoire", col] = 'Côte d’Ivoire'
    df.loc[df[col] == 'Zambia', col] = 'Zambia'
    df.loc[df[col] == 'Venezuela, RB', col] = 'Venezuela'
    df.loc[df[col] == 'Slovak Republic', col] = 'Slovakia'
    df.loc[df[col] == 'Hong Kong SAR, China', col] = 'Hong Kong'
    df.loc[df[col] == 'Egypt, Arab Rep.', col] = 'Egypt'
    df.loc[df[col] == 'Czechia', col] = 'Czech Republic'


# Формирование статистики из рейтинга Top500 по каждой стране
def get_top500_info_by_countries(df):
    countries = dict()

    for i in df.index:
        cur_country = df.loc[i, 'Country']
        cur_cores = df.loc[i, 'Total Cores']
        cur_rmax = df.loc[i, 'RMax']
        cur_rpeak = df.loc[i, 'RPeak']
        if cur_country not in countries.keys():
            countries[cur_country] = {'Total Cores': cur_cores, 'RMax': cur_rmax, 'RPeak': cur_rpeak, 'Num of Systems': 1}
        else:
            countries[cur_country]['Total Cores'] += cur_cores
            countries[cur_country]['RMax'] += cur_rmax
            countries[cur_country]['RPeak'] += cur_rpeak
            countries[cur_country]['Num of Systems'] += 1
    return countries


# Создание словаря страна - код страны
def get_country_to_code_dict():
    d = dict()
    df_tmp = pd.read_csv('Country_To_Iso.csv')
    for i in df_tmp.index:
        d[df_tmp.loc[i, 'Country']] = df_tmp.loc[i, 'Iso3']
    return d


# Добавление статистики из TWB и OECD в DataFrame
def add_ecomomics_statistics(df, twb_input_files, oecd_input_files, hid_input_file):
    year = df.loc[0, 'Year']

    # Нет данных за 2018
    if year == '2018':
        year = '2017'

    for file in twb_input_files:
        df_tmp = pd.read_csv(file)
        attribute_name = df_tmp.loc[0, 'Indicator Name']

        countries_unification(df_tmp)

        countries_in_file = df_tmp['Country Name'].tolist()

        for i in df.index:
            if df.loc[i, 'Country'] not in countries_in_file:
                df.loc[i, attribute_name] = np.nan
                continue
            df.loc[i, attribute_name] = df_tmp.loc[df.loc[i, 'Country'] == df_tmp['Country Name'], year].values[0]

    for file in oecd_input_files:
        df_tmp = pd.read_csv(file, index_col=0)
        attribute_name = df_tmp.loc[0, 'Indicator Name']

        countries_unification(df_tmp)

        for i in df.index:
            if df.loc[i, 'Country'] not in country_to_code_dict.keys():
                df.loc[i, attribute_name] = 'ISO not found'
                continue
            if country_to_code_dict[df.loc[i, 'Country']] not in df_tmp['Country Code'].tolist():
                df.loc[i, attribute_name] = np.nan
                continue
            df.loc[i, attribute_name] = df_tmp.loc[df_tmp['Country Code'] == country_to_code_dict[df.loc[i, 'Country']], year].values[0]

    df_tmp = pd.read_csv(hid_input_file)
    attribute_name = 'HDI'

    countries_unification(df_tmp)

    countries_in_file = df_tmp['Country'].tolist()

    for i in df.index:
        if df.loc[i, 'Country'] not in countries_in_file:
            df.loc[i, attribute_name] = np.nan
            continue
        df.loc[i, attribute_name] = df_tmp.loc[df.loc[i, 'Country'] == df_tmp['Country'], year].values[0]


# Изменение абсолютных значений на квартили (квантили в общем случае)
def get_quantiles(df, columns):
    for col in columns:
        df_tmp = df.dropna(subset=[col])

        # indexes_for_drop = df_tmp[df_tmp[col].astype(object) == 'no info'].index
        # df_tmp = df_tmp.drop(indexes_for_drop)

        if len(df_tmp) == 0:
            continue

        # th1, th2, th3, th4 = df_tmp[col].quantile([0.2, 0.4, 0.6, 0.8]) # [0.25, 0.5, 0.75]
        # for i in df_tmp.index:
        #     if df_tmp.loc[i, col] >= th4:
        #         df.loc[i, col] = col + ': 1'
        #     elif df_tmp.loc[i, col] >= th3:
        #         df.loc[i, col] = col + ': 2'
        #     elif df_tmp.loc[i, col] >= th2:
        #         df.loc[i, col] = col + ': 3'
        #     elif df_tmp.loc[i, col] >= th1:
        #         df.loc[i, col] = col + ': 4'
        #     else:
        #         df.loc[i, col] = col + ': 5'

        df[col] = pd.qcut(df_tmp[col].rank(method='first'), 10, labels=[col + ': 10', col + ': 9', col + ': 8', col + ': 7', col + ': 6',
                                                                        col + ': 5', col + ': 4', col + ': 3', col + ': 2', col + ': 1'])

        # df[col] = pd.qcut(df_tmp[col].rank(method='first'), 5,
        #                   labels=[col + ': 5', col + ': 4', col + ': 3', col + ': 2', col + ': 1'])


# Отладочная функция для вывода списка стран, которые присутствуют с статистике, но нет данных
def debug_get_countries_with_no_info(file):
    df = pd.read_csv(file)
    countries_with_no_info = []
    for col in df.columns:
        countries_with_no_info += (df.loc[df[col].astype(object) == 'no info', 'Country']).tolist()
    countries_with_no_info = pd.unique(countries_with_no_info)
    for c in countries_with_no_info:
        print(c)


def main():
    df_total = pd.DataFrame()
    scp_iterator = 0

    for file in top500_files:
        year = int(file[7:11])          # Получение года из названия файла редакции Top500
        df = pd.read_csv(file)
        df['Num of Systems'] = np.nan

        top500_attributes_rename(df)
        countries_unification(df)

        top500_info_by_countries = get_top500_info_by_countries(df)

        df = df[top500_attributes]
        df = pd.DataFrame(data=[d for d in top500_info_by_countries.values()], dtype=float)
        df.insert(0, 'Country', [c for c in top500_info_by_countries.keys()])
        df.insert(1, 'Year', str(year))
        df['Num of Systems'] = df['Num of Systems'].apply(int)

        add_ecomomics_statistics(df, twb_files, oecd_files, hid_file)

        # df = df.reindex()

        df.to_csv(file[:-4] + '_Result.csv')

        df_scp = pd.read_csv(scopus_files[scp_iterator], index_col=0)
        scp_iterator += 1

        df['Country'] = df['Country'].astype(str)
        df[['RMax', 'RPeak', 'GDPpC', 'HDI']] = df[['RMax', 'RPeak', 'GDPpC', 'HDI']].astype(float)
        df[['Year', 'Num of Systems', 'Total Cores']] = df[['Year', 'Num of Systems', 'Total Cores']].astype(int)
        df_scp['Country'] = df_scp['Country'].astype(str)
        df_scp[['Year', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10']] = df_scp[['Year', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10']].astype(int)

        # df['Year'] = df['Year'].apply(int)
        # df_scp['Year'] = df_scp['Year'].apply(int)

        df = pd.merge(df, df_scp)

        for i in df.index:
            df.loc[i, 'Country'] = 'Country: ' + str(df.loc[i, 'Country'])
            df.loc[i, 'Year'] = 'Year: ' + str(df.loc[i, 'Year'])

        get_quantiles(df, df.columns[2:])

        df_total = pd.concat([df_total, df], ignore_index=True)
    df_total.to_csv(output_file)

#==================================
# Старт программы
#==================================
top500_files = ['TOP500_199311.csv', 'TOP500_199411.csv', 'TOP500_199511.csv', 'TOP500_199611.csv',
                      'TOP500_199711.csv', 'TOP500_199811.csv', 'TOP500_199911.csv', 'TOP500_200011.csv',
                      'TOP500_200111.csv', 'TOP500_200211.csv', 'TOP500_200311.csv', 'TOP500_200411.csv',
                      'TOP500_200511.csv', 'TOP500_200611.csv', 'TOP500_200711.csv', 'TOP500_200811.csv',
                      'TOP500_200911.csv', 'TOP500_201011.csv', 'TOP500_201111.csv', 'TOP500_201211.csv',
                'TOP500_201311.csv', 'TOP500_201411.csv', 'TOP500_201511.csv', 'TOP500_201611.csv',
                'TOP500_201711.csv', 'TOP500_201811.csv']
scopus_files = ['TOP500_199311_Scopus.csv', 'TOP500_199411_Scopus.csv', 'TOP500_199511_Scopus.csv', 'TOP500_199611_Scopus.csv',
                'TOP500_199711_Scopus.csv', 'TOP500_199811_Scopus.csv', 'TOP500_199911_Scopus.csv', 'TOP500_200011_Scopus.csv',
                'TOP500_200111_Scopus.csv', 'TOP500_200211_Scopus.csv', 'TOP500_200311_Scopus.csv', 'TOP500_200411_Scopus.csv',
                'TOP500_200511_Scopus.csv', 'TOP500_200611_Scopus.csv', 'TOP500_200711_Scopus.csv', 'TOP500_200811_Scopus.csv',
                'TOP500_200911_Scopus.csv', 'TOP500_201011_Scopus.csv', 'TOP500_201111_Scopus.csv', 'TOP500_201211_Scopus.csv',
                'TOP500_201311_Scopus.csv', 'TOP500_201411_Scopus.csv', 'TOP500_201511_Scopus.csv', 'TOP500_201611_Scopus.csv',
                'TOP500_201711_Scopus.csv', 'TOP500_201811_Scopus.csv']
twb_files = ['TWB_GDPperCapita_Result.csv']
oecd_files = []
hid_file = 'Human_Development_Index_Result.csv'
output_file = 'FINAL.csv'

top500_attributes = ['Country', 'Total Cores', 'RMax', 'RPeak', 'Num of Systems']

country_to_code_dict = get_country_to_code_dict()
if __name__ == '__main__':
    main()
    debug_get_countries_with_no_info(output_file)