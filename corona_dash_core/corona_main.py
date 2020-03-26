import dask.dataframe as dd
import pandas as pd
from sklearn.cluster import KMeans
import numpy as np
import plotly.graph_objects as go
from sklearn.preprocessing import OrdinalEncoder, LabelBinarizer


class corona_dash_class():

    def __init__(self):

        df_confirmed = dd.read_csv('data/novel-corona-virus-2019-dataset/time_series_2019-ncov-Confirmed*.csv',
                                   dtype={'ID': 'float64', 'data_moderator_initials': 'object',
                                          'wuhan(0)_not_wuhan(1)': 'float64'})
        self.df_confirmed = df_confirmed

        df_deaths = dd.read_csv('data/novel-corona-virus-2019-dataset/time_series_2019-ncov-Deaths*.csv')
        self.df_deaths = df_deaths

        df_recovered = dd.read_csv('data/novel-corona-virus-2019-dataset/time_series_2019-ncov-Recovered*.csv')
        self.df_recovered = df_recovered

        df_stats = pd.read_csv('data/novel-corona-virus-2019-dataset/COVID19_line_list_data.csv')
        self.df_stats = df_stats

    # def get_confirmed(self):
    #     df = self.df_confirmed
    #     print(len(df))
    #     # df2 = df[['Lat', 'Long', 'Country/Region','3/21/20']]
    #
    #     df.groupby('Country/Region').sum()
    #     df = df.compute()
    #     # df1=df[df['Country/Region']=='Japan']
    #     print(df)
    #     print(df.describe())
    #
    #     json_converted = df.to_json(orient='records')
    #     return json_converted
    #
    # def get_deaths(self):
    #     df = self.df_deaths
    #     print(len(df))
    #     # df2 = df[['Lat', 'Long', 'Country/Region','3/21/20']]
    #
    #     df.groupby('Country/Region').sum()
    #     df = df.compute()
    #     # df1=df[df['Country/Region']=='Japan']
    #     print(df)
    #     print(df.describe())
    #
    #     json_converted = df.to_json(orient='records')
    #     return json_converted

    def combined(self):
        df_deaths = self.df_deaths
        df_confirmed = self.df_confirmed
        df_recovered = self.df_recovered

        deaths_df = df_deaths[[str(df_deaths.columns[len(df_deaths.columns) - 1])]]

        confirmed_df = df_confirmed[[str(df_confirmed.columns[len(df_confirmed.columns) - 1])]]

        recovered_df = df_recovered[[str(df_recovered.columns[len(df_recovered.columns) - 1])]]

        init_df = df_deaths[['Lat', 'Long', 'Country/Region']].compute()
        init_df['deaths'] = deaths_df.compute()
        init_df['confirmed'] = confirmed_df.compute()
        init_df['recovered'] = recovered_df.compute()

        group_df = init_df[['Country/Region', 'deaths', 'confirmed', 'recovered']]
        group_df = group_df.groupby('Country/Region').sum()
        group_df['country'] = group_df.index
       # print(group_df)

        location_df = init_df[['Lat', 'Long']]
        location_df['country'] = init_df[['Country/Region']]
       # print(location_df)

        result_df = pd.merge(group_df, location_df, on='country')
        result_df = result_df.reset_index(drop=True)
       # print(result_df)
#
        result_df = result_df.sort_values(by=['confirmed'], ascending=False)
        result_df = result_df.reset_index(drop=True)
        #print(result_df)

        # init_df['country']=init_df.index
        # init_df = init_df.reset_index(drop=True)

        # init_df=init_df.sort_values(by=['confirmed'],ascending=False)

        json_converted = result_df.to_json(orient='records')
        return json_converted

    def combined_table(self):
        df_deaths = self.df_deaths
        df_confirmed = self.df_confirmed
        df_recovered = self.df_recovered

        deaths_df = df_deaths[[str(df_deaths.columns[len(df_deaths.columns) - 1])]]

        confirmed_df = df_confirmed[[str(df_confirmed.columns[len(df_confirmed.columns) - 1])]]

        recovered_df = df_recovered[[str(df_recovered.columns[len(df_recovered.columns) - 1])]]

        init_df = df_deaths[['Lat', 'Long', 'Country/Region']].compute()
        init_df['Deaths'] = deaths_df.compute()
        init_df['Confirmed'] = confirmed_df.compute()
        init_df['Recovered'] = recovered_df.compute()

        group_df = init_df[['Country/Region', 'Deaths', 'Confirmed', 'Recovered']]
        group_df = group_df.groupby('Country/Region').sum()
        group_df['Country'] = group_df.index
        group_df = group_df[['Country', 'Deaths', 'Confirmed', 'Recovered']]
        #print(group_df)

        result_df = group_df.sort_values(by=['Confirmed'], ascending=False)
        result_df = result_df.reset_index(drop=True)
        #print(result_df)

        json_converted = result_df.to_json(orient='records')
        return json_converted

    def precentage_of_death_by_age(self):

        df = self.df_stats
        data = df[['age', 'gender', 'death']].dropna()
        print(data)
        data = data[data['death'] != '0']
        # data=data[data['sex']=='male']

        for i in range(len(data)):
            print(data['death'].iloc[i])
            try:
                if float(data['death'].iloc[i]) != 0:
                    data['death'].iloc[i] = 1
            except:
                data['death'].iloc[i] = 1
        print(data)

        death_list = data['death'].tolist()
        gender_list = data['gender'].tolist()
        age = data['age']
        age_list = age.tolist()

        for x in range(len(age_list)):
            if str(age_list[x].__class__.__name__) == 'str':
                try:
                    age_list[x] = float(age_list[x])
                except:
                    try:
                        age_split = str(age_list[x]).split('-')
                        # print(age_split)
                        average_age = float(age_split[0]) + (float(age_split[1]) - float(age_split[0])) / 2
                        age_list[x] = average_age
                    except:
                        age_list[x] = -1

        print(age_list)

        age_list=pd.Series(age_list).dropna().tolist()
        result = np.array(age_list).reshape(-1, 1)
        kmeans = KMeans(n_clusters=3, random_state=0).fit(result)

        cluster_value_count = pd.Series(kmeans.labels_).value_counts(normalize=True)
        cluster_value_count_index = list(cluster_value_count.index)
        cluster_count_values = cluster_value_count.values
        #print(cluster_value_count_index)
        #print(cluster_count_values)
        result_clusters = kmeans.labels_
        percentage_death_rate = []
        for x in range(len(result_clusters)):
            if result_clusters[x] == cluster_value_count_index[0]:
                percentage_death_rate.append(cluster_value_count[0] * 100)

            elif result_clusters[x] == cluster_value_count_index[1]:
                percentage_death_rate.append(cluster_value_count[1] * 100)
            else:
                percentage_death_rate.append(cluster_value_count[2] * 100)

        #print(percentage_death_rate)
        # print(kmeans.labels_)
        #print(pd.Series(kmeans.labels_).value_counts(normalize=True))
        #print(kmeans.cluster_centers_)


        df=pd.DataFrame()
        df['age']=data['age']
        df['gender']=data['gender']
        df['percentage_death_rate']=percentage_death_rate

        json_converted = df.to_json(orient='records')
        return json_converted

        # gender_list_converted=enc.fit_transform(np.array(gender_list).reshape(-1,1))
        # gender_list_converted=gender_list_converted.tolist()
        # print(gender_list_converted)
