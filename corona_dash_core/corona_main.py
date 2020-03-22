
import dask.dataframe as dd


class corona_dash_class():

    def __init__(self):

        df = dd.read_csv('data/novel-corona-virus-2019-dataset/time_series_2019-ncov-Confirmed*.csv',dtype={'ID': 'float64',
       'data_moderator_initials': 'object',
       'wuhan(0)_not_wuhan(1)': 'float64'}
)
        self.df=df


    def get_location_data(self):
        df=self.df
        print(len(df))
       # df2 = df[['Lat', 'Long', 'Country/Region','3/21/20']]

        df.groupby('Country/Region').sum()
        df = df.compute()
        # df1=df[df['Country/Region']=='Japan']
        print(df)
        print(df.describe())

        json_converted = df.to_json(orient='records')
        return json_converted

    def get_confirmed_cases_time_series(self):
        df = self.df
        #df=df.transpose
       # df=df.compute()
        print(df)