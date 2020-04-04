import json
#from fbprophet import Prophet

import dask.dataframe as dd
import pandas as pd
from sklearn.cluster import KMeans
import numpy as np
from sklearn import linear_model
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OrdinalEncoder, LabelBinarizer, PolynomialFeatures




class corona_dash_class():

    def __init__(self):


        df_deaths = dd.read_csv('data/dataset2/time_series_covid19_deaths_global.csv')
        self.df_deaths = df_deaths

        df_recovered = dd.read_csv('data/dataset2/time_series_covid19_recovered_global.csv')
        self.df_recovered = df_recovered

        df_confirmed = dd.read_csv('data/dataset2/time_series_covid19_confirmed_global.csv')
        self.df_confirmed = df_confirmed

        df_stats=pd.read_csv('data/dataset2/COVID19_line_list_data.csv')
        self.df_stats=df_stats

        df_webscaraped_table = pd.read_csv('data/dataset2/stat_table.csv')
        self.df_webscaraped_table = df_webscaraped_table
        print(df_webscaraped_table)
        print(df_webscaraped_table.columns)


    def stat_table_data(self):
        df=self.df_webscaraped_table
        result_df=df[['Country,Other','TotalCases','TotalDeaths','TotalRecovered']]


        result_df=result_df.fillna(0)
        result_df=result_df.iloc[1:,:]
        # result_df['TotalCases']=result_df['TotalCases'].apply(lambda x: str(x).replace(',',''))

        # result_df=result_df.sort_values(by=['TotalCases'],ascending=False)
        # print(result_df)

        json_converted = result_df.to_json(orient='records')
        return json_converted

    # def time_series_header_stat(self):
    #     df = self.df_webscaraped_table
    #     result_df = df[['Country,Other', 'TotalCases', 'TotalDeaths', 'TotalRecovered']]
    #     print(result_df)
    #
    #     json_converted = result_df.to_json(orient='records')
    #     return json_converted

    def world_wide_time_series(self):
        df_deaths=self.df_deaths.compute()
        df_confirmed = self.df_confirmed.compute()
        df_recovered = self.df_recovered.compute()

        df_deaths = df_deaths.iloc[:,4:]
        column_list=df_deaths.columns.tolist()
        daily_total_deaths=[]
        for x in range(len(df_deaths.columns)):
            df_deaths[column_list[x]]=df_deaths[column_list[x]].astype('float64')
            daily_total_deaths.append(df_deaths[column_list[x]].sum())

        df_confirmed = df_confirmed.iloc[:, 4:]
        column_list = df_confirmed.columns.tolist()
        daily_total_confirmed = []
        for x in range(len(df_confirmed.columns)):
            df_confirmed[column_list[x]] = df_confirmed[column_list[x]].astype('float64')
            daily_total_confirmed.append(df_confirmed[column_list[x]].sum())

        df_recovered = df_recovered.iloc[:, 4:]
        column_list = df_recovered.columns.tolist()
        daily_total_recovered = []
        for x in range(len(df_recovered.columns)):
            df_recovered[column_list[x]] = df_recovered[column_list[x]].astype('float64')
            daily_total_recovered.append(df_recovered[column_list[x]].sum())


        df=pd.DataFrame()
        df['date']=pd.to_datetime(df_recovered.columns.tolist())
        df['date']=df['date'].astype('str')
        df['deaths_world']=daily_total_deaths
        df['confirmed_world']=daily_total_confirmed
        df['recovered_world']=daily_total_recovered
        json_converted = df.to_json(orient='records')

        df.to_csv('worldwide_data_timeseries.csv')

        reg_death = LinearRegression()
        reg_confirmed = LinearRegression()
        reg_recovered = LinearRegression()

        data_death = df.groupby('date')['deaths_world'].sum().reset_index()
        date_ = np.array(data_death.index).reshape(-1, 1)

        data_death_np= np.array(data_death['deaths_world']).reshape(-1, 1)
        polynomial_features = PolynomialFeatures(degree=5)
        date_poly = polynomial_features.fit_transform(date_)
        reg_death.fit(date_poly, data_death_np)

        data_confirmed = df.groupby('date')['confirmed_world'].sum().reset_index()
        data_confirmed_np = np.array(data_confirmed['confirmed_world']).reshape(-1, 1)
        reg_confirmed.fit(date_poly, data_confirmed_np)

        data_recovered = df.groupby('date')['recovered_world'].sum().reset_index()
        data_recovered_np = np.array(data_recovered['recovered_world']).reshape(-1, 1)
        reg_recovered.fit(date_poly, data_recovered_np)


        test = np.arange(len(date_),len(date_)+14, dtype=float).reshape(-1, 1)
        polynomial_features = PolynomialFeatures(degree=5)

        date_list=df['date'].tolist()

        date_=pd.date_range(start=str(date_list[len(date_list)-1]), periods=14, freq='D')

        x_poly_test = polynomial_features.fit_transform(test)

        pred_death = reg_death.predict(x_poly_test)
        pred_confirmed = reg_confirmed.predict(x_poly_test)
        pred_recovered = reg_recovered.predict(x_poly_test)

        pred_df=pd.DataFrame()
        pred_df['pred_date'] = date_
        pred_df['pred_death']=pred_death
        pred_df['pred_confirmed'] = pred_confirmed
        pred_df['pred_recovered'] = pred_recovered

        print(pred_df)
        json_converted_pred = pred_df.to_json(orient='records')

        return json_converted,json_converted_pred



    def map_data(self):
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
        #print(data)
        data = data[data['death'] != '0']
        # data=data[data['sex']=='male']

        for i in range(len(data)):
            #print(data['death'].iloc[i])
            try:
                if float(data['death'].iloc[i]) != 0:
                    data['death'].iloc[i] = 1
            except:
                data['death'].iloc[i] = 1
        #print(data)

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

        #print(age_list)

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
        age_group=[]
        for x in range(len(result_clusters)):
            if result_clusters[x] == cluster_value_count_index[0]:
                percentage_death_rate.append(cluster_value_count[0] * 100)
                age_group.append('55>')

            elif result_clusters[x] == cluster_value_count_index[1]:
                percentage_death_rate.append(cluster_value_count[1] * 100)
                age_group.append('55-70')
            else:
                percentage_death_rate.append(cluster_value_count[2] * 100)
                age_group.append('70-90')

        #print(percentage_death_rate)
        # print(kmeans.labels_)
        # print(pd.Series(kmeans.labels_).value_counts(normalize=True))
        # print(kmeans.cluster_centers_)


        df=pd.DataFrame()
        df['age']=data['age']
        df['gender']=data['gender']
        df['percentage_death_rate']=percentage_death_rate
        df['age_group']=age_group


        json_converted = df.to_json(orient='records')
        return json_converted

        # gender_list_converted=enc.fit_transform(np.array(gender_list).reshape(-1,1))
        # gender_list_converted=gender_list_converted.tolist()
        # print(gender_list_converted)

    def time_seires(self,country_name):
        df_deaths = self.df_deaths.compute()
        df_confirmed = self.df_confirmed.compute()
        df_recovered = self.df_recovered.compute()
        #print(df_confirmed)

        death_column_names=df_deaths.columns
        df_deaths=df_deaths[df_deaths['Country/Region']==str(country_name)]
        time_series_death_df=df_deaths.iloc[:,4:len(death_column_names)-1]

        confirmed_column_names = df_confirmed.columns
        df_confirmed = df_confirmed[df_confirmed['Country/Region'] == str(country_name)]
        time_series_confirmed_df = df_confirmed.iloc[:, 4:len(confirmed_column_names) - 1]

        recovered_column_names = df_recovered.columns
        df_recovered = df_recovered[df_recovered['Country/Region'] == str(country_name)]
        time_series_recovered_df = df_recovered.iloc[:, 4:len(recovered_column_names) - 1]

        death_series=[]

        for x in range(len(time_series_death_df.columns)):
            death_series.append(time_series_death_df[time_series_death_df.columns[x]].sum())

        confirmed_series = []

        for x in range(len(time_series_confirmed_df.columns)):
            confirmed_series.append(time_series_confirmed_df[time_series_confirmed_df.columns[x]].sum())

        recovered_series = []

        for x in range(len(time_series_recovered_df.columns)):
            recovered_series.append(time_series_recovered_df[time_series_recovered_df.columns[x]].sum())


        new_df=pd.DataFrame()
        new_df['time_death']=pd.to_datetime(time_series_death_df.columns.tolist())
        new_df['time_death']=new_df['time_death'].astype('str')
        new_df['deaths'] = death_series

        new_df['time_confirm'] = pd.to_datetime(time_series_death_df.columns.tolist())
        new_df['time_confirm'] = new_df['time_confirm'].astype('str')
        new_df['confirm'] = confirmed_series

        new_df['time_recovered'] = pd.to_datetime(time_series_death_df.columns.tolist())
        new_df['time_recovered'] = new_df['time_recovered'].astype('str')
        new_df['recovered'] = recovered_series

        # print(new_df)
        json_converted = new_df.to_json(orient='records')
        return json_converted





