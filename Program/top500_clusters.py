import pandas as pd
import numpy as np

# Изменение абсолютных значений на квартили (квантили в общем случае)
def get_quantiles(df, columns):
    for col in columns:
        df_tmp = df.dropna(subset=[col])
        if len(df_tmp) == 0:
            continue
        if len(df) >= 10:
            df[col] = pd.qcut(df_tmp[col].rank(method='first'), 10,
                          labels=[col + ': 10', col + ': 9', col + ': 8', col + ': 7', col + ': 6',
                                  col + ': 5', col + ': 4', col + ': 3', col + ': 2', col + ': 1'])
        else:
            df[col] = pd.qcut(df_tmp[col].rank(method='first'), len(df),
                            labels=[col + ': ' + str(i + 1) for i in reversed(range(len(df.index)))])

def get_groups_by_clusters():
    files = ['../Top500 clusters num of systems/TOP500_199311_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199411_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199511_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199611_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199711_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199811_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_199911_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200011_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200111_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200211_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200311_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200411_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200511_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200611_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200711_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200811_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_200911_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201011_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201111_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201211_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201311_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201411_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201511_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201611_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201711_Result_And_Scopus_Clusters.csv',
             '../Top500 clusters num of systems/TOP500_201811_Result_And_Scopus_Clusters.csv']
    df0_total = pd.DataFrame(
        columns=['Country', 'Year', 'Num of Systems', 'RMax', 'RPeak', 'Total Cores', 'GDPpC', 'HDI', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10', 'Cluster'])
    df1_total = pd.DataFrame(
        columns=['Country', 'Year', 'Num of Systems', 'RMax', 'RPeak', 'Total Cores', 'GDPpC', 'HDI', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10', 'Cluster'])
    df2_total = pd.DataFrame(
        columns=['Country', 'Year', 'Num of Systems', 'RMax', 'RPeak', 'Total Cores', 'GDPpC', 'HDI', 'Q1', 'Q2', 'Q3', 'Q4', 'Top10', 'Cluster'])

    for file in files:
        df_tmp = pd.read_csv(file)
        for i in df_tmp.index:
            if df_tmp.loc[i, 'Cluster'] == 'cluster_0':
                df0_total = df0_total.append(df_tmp.loc[i], ignore_index=True)
            elif df_tmp.loc[i, 'Cluster'] == 'cluster_1':
                if file == '../Top500 clusters num of systems/TOP500_200611_Result_And_Scopus_Clusters.csv':
                    df2_total = df2_total.append(df_tmp.loc[i], ignore_index=True)
                else:
                    df1_total = df1_total.append(df_tmp.loc[i], ignore_index=True)
            elif df_tmp.loc[i, 'Cluster'] == 'cluster_2':
                if file == '../Top500 clusters num of systems/TOP500_200611_Result_And_Scopus_Clusters.csv':
                    df1_total = df1_total.append(df_tmp.loc[i], ignore_index=True)
                else:
                    df2_total = df2_total.append(df_tmp.loc[i], ignore_index=True)

    dfs = [df0_total, df1_total, df2_total]
    dtype0 = {'Country': 'str', 'Year': 'str', 'Num of Systems': 'int64', 'RMax': 'float64', 'RPeak': 'float64', 'Total Cores': 'int64',
              'GDPpC': 'float64', 'HDI': 'float64', 'Q1': 'int64', 'Q2': 'int64', 'Q3': 'int64', 'Q4': 'int64',
              'Top10': 'int64', 'Cluster': 'str'}
    for i in range(len(dfs)):
        dfs[i] = dfs[i].astype(dtype0)
    return dfs


def main():
    dfs = get_groups_by_clusters()
    years = [i for i in range(1993, 2019)]
    index = 0

    for df in dfs:
        df_total_cluster = pd.DataFrame(columns=df.columns)

        for year in years:
            sub_df = df[df['Year'] == 'Year: ' + str(year)]
            get_quantiles(sub_df, ['Num of Systems','RMax','RPeak','Total Cores','GDPpC','HDI','Q1','Q2','Q3','Q4','Top10'])
            df_total_cluster = pd.concat([df_total_cluster, sub_df], ignore_index=True)

        df_total_cluster.to_csv('FINAL_CLUSTER_' + str(index) + '.csv')
        index += 1

#==================================
# Старт программы
#==================================
if __name__ == '__main__':
    main()