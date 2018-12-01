import requests
import pandas as pd
from top500 import countries_unification
from top500 import top500_files


# Получение списков идентификаторов источников, входящих в каждый квартиль
def get_quartiles_scopus_ids(year):
    df = pd.read_csv('Scopus_Sources_2011-2017_Result.csv')

    if year < 2011:
        year = 2011
    if year == 2018:
        year = 2017

    q1 = pd.unique(df[(df['Quartile'] == 'Quartile 1') & (df['Year'] == year)]['Scopus SourceID'])
    q2 = pd.unique(df[(df['Quartile'] == 'Quartile 2') & (df['Year'] == year)]['Scopus SourceID'])
    q3 = pd.unique(df[(df['Quartile'] == 'Quartile 3') & (df['Year'] == year)]['Scopus SourceID'])
    q4 = pd.unique(df[(df['Quartile'] == 'Quartile 4') & (df['Year'] == year)]['Scopus SourceID'])
    top10 = pd.unique(df[(df['Top 10% (CiteScore Percentile)'] == 'Top 10%') & (df['Year'] == year)]['Scopus SourceID'])

    return q1, q2, q3, q4, top10


# Получение общего количества публикаций для данного квартиля для данного года для всех источников этого квартиля
def get_publications_amount_in_quartile(q_cur, country, year, batch_size, api_keys_index):
    start = 0
    end = batch_size
    publications_amount = 0

    max_sources_number = len(q_cur) # Всего источников в квартиле len(q_cur)

    while (start != max_sources_number):
        query = get_query_for_quartiles(q_cur, start, end, country, year)
        publications_amount_for_query, api_keys_index = get_publication_amount_for_query(query, api_keys_index)

        publications_amount += publications_amount_for_query

        start = end
        if end + batch_size > max_sources_number:
            end = max_sources_number
        else:
            end += batch_size

    return publications_amount, api_keys_index


# Формирование строки запроса для API
def get_query_for_quartiles(q_cur, start, end, country, year):
    query = '?query=('

    for id in q_cur[start:end]:
        query += 'SOURCE-ID({0}) OR '.format(id)

    query = query[:-4] + ') AND AFFILCOUNTRY({0})'.format(country)
    query += ' AND PUBYEAR IS {0}'.format(year)
    query += '&count=1&field=dc:identifier&suppressNavLinks=true'   # Уменьшение кол-ва возвращаемой лишней информации

    return query


# Получение количества публикаций по сформированному запросу
def get_publication_amount_for_query(query, api_keys_index):
    api_key = api_keys[api_keys_index]
    url = 'https://api.elsevier.com/content/search/scopus'

    respond = requests.get(url + query, headers={'Accept': 'application/json',
                                                 'X-ELS-APIKey': api_key})
    if respond.status_code == 429:
        print('=======Quota Exceeded=======')
        for i in range(api_keys_index, len(api_keys)):
            print('Switch to the next API key')
            api_keys_index += 1
            print('API key index: ', api_keys_index)
            api_key = api_keys[api_keys_index]
            respond = requests.get(url + query, headers={'Accept': 'application/json',
                                                        'X-ELS-APIKey': api_key})
            if respond.status_code != 429:
                print('OK')
                break
    respond = respond.json()
    try:
        return int(respond['search-results']['opensearch:totalResults']), api_keys_index
    except:
        print("=======Exception=======")
        print("Respond is: ", respond)
        print("Status code is: ", respond['service-error']['status']['statusCode'])
        for i in range(3):
            respond = requests.get(url + query, headers={'Accept': 'application/json',
                                                         'X-ELS-APIKey': api_key})
            respond = respond.json()
            print("Repeated query {0}: {1}".format(i + 1, respond))
            if 'search-results' in respond.keys():
                return int(respond['search-results']['opensearch:totalResults']), api_keys_index
        print("3 repetitions are failed")
        return int(respond['search-results']['opensearch:totalResults']), api_keys_index


def make_total_scopus_file_absolute(scopus_files):
    total_df = pd.DataFrame()
    for file in scopus_files:
        df_tmp = pd.read_csv(file, index_col=0)
        total_df = pd.concat([total_df, df_tmp], ignore_index=True)
    total_df.to_csv("TOP500_Total_Scopus_Absolute.csv")


def main():
    #df_total = pd.DataFrame()
    # scopus_files = []
    api_keys_index = 0

    batch_size = 100                # Количество источников в одном обращении к API
    quartiles_conformity = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}

    for file in top500_files:
        year = int(file[7:11])      # Получение года из навзвания файла редакции Top500
        buf_file_name = file[:-4] + 'BUF.csv'

        q1, q2, q3, q4, top10 = get_quartiles_scopus_ids(year)
        qs_list = [q1, q2, q3, q4]

        df = pd.read_csv(file)
        countries_unification(df)
        countries = pd.unique(df['Country'])

        df = pd.DataFrame(columns=['Country', 'Year', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10'])
        index_in_df = 0

        print('Year {0}, list {1}'.format(year, file))   # Вывод прогресса программы в консоль
        print(countries)                                 # Всего уникальных стран в редакции

        for country in countries:
            quartile_index = 0

            for q_cur in qs_list:
                quartile_index += 1
                print('Quartile {0} in {1}'.format(quartiles_conformity[quartile_index], country)) # Вывод прогресса программы в консоль

                publications_amount, api_keys_index = get_publications_amount_in_quartile(q_cur, country, year, batch_size, api_keys_index)
                df.loc[index_in_df, quartiles_conformity[quartile_index]] = publications_amount

            print('Top10 in {0}'.format(country))           # Вывод прогресса программы в консоль
            publications_amount, api_keys_index = get_publications_amount_in_quartile(top10, country, year, batch_size, api_keys_index)
            df.loc[index_in_df, 'Top10'] = publications_amount

            df.loc[index_in_df, 'Country'] = country
            df.loc[index_in_df, 'Year'] = year

            df.to_csv(buf_file_name)

            index_in_df += 1

        output_file_name = file[:-4] + '_Scopus.csv'
        # scopus_files.append(output_file_name)
        df.to_csv(output_file_name)             # Файл для результата по одному году (для одной редакции)

        # df_total = pd.concat([df_total, df], ignore_index=True)

    # df_total.to_csv('TOP500_Total_Scopus.csv')
    # make_total_scopus_file_absolute(scopus_files)


#==================================
# Старт программы
#==================================
if __name__ == '__main__':
    api_keys = ['a81273085526d8d422827b51581a0c56',
                'b1c38a8eb0be4524581bb9776b938fb3',
                'b435f5549c65636cd0342197dfe37c7a',
                '733f94eeaba96e8073e9c98215c7b81f',
                '837623ff091922f6fc5ec000bdfbddb6',
                '1305349d19c41c11c9965bbbcd5815e5',
                '2563364480be7937ddf869fe908520d8',
                '73359a984c770f938294ec0f47decc51',
                'c590163633c3e78bfb4a9de1aef9a6d1',
                '8958497f0ab6a9bde40d5422670f0d68']
    main()