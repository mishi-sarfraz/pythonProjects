import pandas as pd
import datetime
import time
def compiled_product_data(time_stamp,df_product):
    ''' 
    Function to get the product dataframe depending on the timespan
    --------------Parameters----------------
    time_stamp:
        type: datetime.timedelta
        df_product:
        type: pandas.DataFrame
        
        --------------Returns----------------
        GAdataFrame:
        type: pandas.DataFrame
        description: the product dataframe  within timespan'''
    
    df_product=df_product.drop(columns=['Unnamed: 0'])
    # drop columnn user_id
    df_product=df_product.drop(columns=['user_id'])
    # convert time_stamp to unix time
    # convert time_stamp to datetime    
    df_product['time_stamp']=pd.to_datetime(df_product['event_timestamp'])
    # df_product["time_stamp"] = [int(x.timestamp()) for x in df_product["event_timestamp"]]
    # df_product['time_stamp']=df_product['event_timestamp'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    # convert time_stamp to datetime    
    # df_product['time_stamp']=pd.to_datetime(df_product['event_timestamp'])
    df_product=df_product[df_product['time_stamp']>=time_stamp]
    df_product['unix_time']=df_product['time_stamp'].apply(lambda x: time.mktime(x.timetuple()))
    #   (time.mktime(timespan.timetuple())))
    # df_product['unix_time'] = df_product['time_stamp'].astype(int)

    return df_product
def main(timespan,df_product):
    '''  
    Main function to get the daily statistics of the product depending on cetain timespan
    --------------Parameters----------------
    timespan: 
        type: datetime.timedelta
    df_product: 
        type: pandas.DataFrame
        description: the product dataframe
    --------------Returns----------------
    NewGADataframe:
        type: pandas.DataFrame
        description: the daily statistics of the product
     
    '''

    print('Data will be filtered according to date',timespan)
    GAdataFrame=compiled_product_data(timespan,df_product)
    # count all th different event_type for every product id
    NewGAdataFrame=GAdataFrame.groupby(['product_id','event_type'])
    NewGAdataFrame=NewGAdataFrame.agg(count=("event_type","count"), latest_time=("unix_time","max"))
    # res = gr.agg(count=("event","count"), latest_time=("time_stamp","m
    # ax"))
    NewGAdataFrame.reset_index(inplace=True)
    
    
    # res.reset_index(inplace=True)
    NewGAdataFrame= NewGAdataFrame.pivot(index=["product_id","latest_time"], columns="event_type", values="count").reset_index(level=[0,1])   
    NewGAdataFrame= NewGAdataFrame.rename_axis(None, axis=1)
     # convert all the nan to 0
    NewGAdataFrame=NewGAdataFrame.fillna(0)
    # print(NewGAdataFrame.columns)
    # save file
    # NewGAdataFrame.to_csv('LatestGAdataFrame.csv')
    # print(NewGAdataFrame)
    return NewGAdataFrame
    # else:
    #     return TR,PY,LB


if __name__=="__main__":
    dd,hh,mm=7,13,12
    #  Take the data frame 
    df_product=pd.read_csv('finalDf.csv')
    #  date and time now 
    now=datetime.datetime.now()
    #  date and time days hh mm before now
    timespan=now-datetime.timedelta(days=dd,hours=hh,minutes=mm)
    # displaying unix timestamp after conversion
    
    
    # # convert to unix time
    # timespan=datetime.datetime.timestamp(timespan)
    df=main(timespan,df_product)
    print(df.head(10))
    # print(df.columns)
    
    '''product_id', 'latest_time', 'added_to_basket', 'product_sold',
       'recommendation_click', 'referral_click', 'saved_items',
       'search_click'''
    # # sort df by sales
    # df=df.sort_values(by=['product_sold'],ascending=False)
    # df=df.loc[:,['product_id','referral_click','product_sold']]
    # print(df)
    
