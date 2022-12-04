# import the required libraries and files
import pandas as pd
import datetime
import time
from nl_exception_handeling import *
db_server="mongodb+srv://-e.nsafb.mongodb.net"
internal_db_server= "heast-2.compute.amazonaws.com"
internalClient = pymongo.MongoClient(internal_db_server)
# internal_client=pymongo.MongoClient(internal_db_server)
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
    df_product=df_product[df_product['time_stamp']>=time_stamp]
    df_product['unix_time']=df_product['time_stamp'].apply(lambda x: time.mktime(x.timetuple()))
 
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
    # GAdataFrame=compiled_product_data(timespan,df_product)
    GAdataFrame = time_out_function(
                testing, #is this in testing mode
                max_retries, #max number of retries
                time_out, #time out in seconds before killing the function
                wait_time, #time to wait between retries
                compiled_product_data, #this is the function to call
                message_details,
                internalClient,
                collection_err,
                timespan,df_product)

    
    # group the different event_type for every product id
    NewGAdataFrame=GAdataFrame.groupby(['product_id','event_type'])
    # aggregate all the event_type for every product id and take the latest time against it
    NewGAdataFrame=NewGAdataFrame.agg(count=("event_type","count"), latest_time=("unix_time","max"))
    # reset the index
    NewGAdataFrame.reset_index(inplace=True) 
    # convert to extended dataframe
    NewGAdataFrame= NewGAdataFrame.pivot(index=["product_id","latest_time"], columns="event_type", values="count").reset_index(level=[0,1])   
    NewGAdataFrame= NewGAdataFrame.rename_axis(None, axis=1)
     # convert all the nans to 0
    NewGAdataFrame=NewGAdataFrame.fillna(0)
    return NewGAdataFrame
    

if __name__=="__main__":
    #  dd = days
    #  hh = hours
    #  mm = minutes
    #  dd,hh,mm  Time span to get the data from the collection i.e how many days,hh,mm back from
    # the current date and time data is required
    dd,hh,mm=7,13,12
    #  Take the data frame 
    df_product=pd.read_csv('product.csv')
    #  date and time now 
    now=datetime.datetime.now()
    #  get the user Id
    user_id='6342W0TJNWXJOO8060OMLX2DRU'
    #  get all the rows matching the user id using lambda function
    # df_product=df_product[df_product['user_id'].apply(lambda x: x==user_id)]
    #  get all the rows matching the user id using list comprehension
    df_product=df_product[df_product['user_id'].isin([user_id])]
    db_server = "mongodb+srv://neurallab:neruallab@cluster0.mzwbp9n.mongodb.net/?retryWrites=true&w=majority"
    internalClient=pymongo.MongoClient(db_server)
    # df_shopgram= read_from_db(db_server, "Shopgram_Data",**{'db_name','exampleDB'})
    # df_shopgram = read_from_db( #this is the function to call
    #    internalClient,'dummyDB',**{'db_name':'Shopgram_Data'})
    # print(df_shopgram.columns)
    df_shopgram = read_from_db(internalClient,'Shopgram_Data',**{'db_name':'dummyDB'})
    print(df_shopgram.columns)
    # get 2 columns from shopgram data
    df_shopgram=df_shopgram[['product_id','product_type']]

    # match the product id from shopgram data with the product id from product data and return the product type and product id and user id and event type and event_timestamp
    df_product=pd.merge(df_product,df_shopgram,how='left',on='product_id')[['product_id','product_type','user_id','event_type','event_timestamp']]
    # df_product=pd.merge(df_product,df_shopgram,how='left',on='product_id')[['product_id','product_type','user_id']]
    # sort the dataframe by latest time_stamp and unique product type
    df_product=df_product.sort_values(by=['product_type','event_timestamp'],ascending=False).drop_duplicates(subset=['product_type'])
    # sort the dataframe by latest time_stamp
    df_product=df_product.sort_values(by=['event_timestamp'],ascending=False)
    # df_product=df_product['product_id'].apply(lambda x: df_shopgram[df_shopgram['product_id']==x]['product_type'].values[0])
    # get the latest event_timestamp for unique product_type 
    # df_product['product_type']=df_product.groupby(['product_type']).agg(latest_time=("event_timestamp","max"))
    # df_product=df_product[df_product['user_id']==user_id]
    #  date and time days hh mm before now
    # timespan=now-datetime.timedelta(days=dd,hours=hh,minutes=mm)
    # displaying unix timestamp after conversion
    
    
 
    # max_retries = 1# number of times to try and access the  function
    # wait_time= 1 # seconds wait if there is an error in the function
    # time_out = 10 # seconds to wait for a response from the server
    # collection_err = 'IRefer_function_stats'
    # testing = 1
    # client_token = 'xoxb-1488857659584-4290659464087-JXsbZQMZ98e0f50iwTspgKtq'
    # channel_id = 'C048H501G4X'
    # SUBJECT = 'dailyDf.py' 
    # message_details = {'type':'slack','client_token' : client_token,'channel_id' : channel_id,'SUBJECT': SUBJECT}
    # df = time_out_function(
    #             testing, #is this in testing mode
    #             max_retries, #max number of retries
    #             time_out, #time out in seconds before killing the function
    #             wait_time, #time to wait between retries
    #             main, #this is the function to call
    #             message_details,
    #             internalClient,
    #             collection_err,
    #             timespan,df_product)
    # # save to db
    # # save_to_db(df,client,collection, del_db = False,*args, **kwargs)
    # # SUBJECT='save to db, dailyDh.py'
    # #  dataframe length check
    # if len(df) > 0:
    #     '''Rename the present collection to the collection with timestamp , for this access the
    #      collection and rename it to the collection with timestamp'''
    #     #  get the collection name 
    #     # old_collection_name='Dailydf'
    #     old_collection_name='dailyProductDataFrame'
    #     #  get the collection if it exists 
    #     if old_collection_name in internalClient['IRefer'].list_collection_names():
    #         # rename the collection to the collection with timestamp
    #         internalClient['IRefer'][old_collection_name].rename('dailyProductDataFrame'+'_'+str(datetime.datetime.now()))
           
    #     # collection=internalClient['IRefer']['dailyDataframe']
    #     # #  get the collection name with datetime
    #     # new_collection_name=oldc_ollection_name+'_'+str(datetime.datetime.now())
    #     # #  rename the collection
    #     # collection.rename(new_collection_name)
        
    #     # Save the latest dataframe with the collection name : dailyDf
    #     time_out_function(
    #         testing, #is this in testing mode
    #         max_retries, #max number of retries
    #         time_out, #time out in seconds before killing the function
            
    #         wait_time, #time to wait between retries
    #         save_to_db, #this is the function to call
    #         # {'type':'slack','client_token' : client_token,'channel_id' : channel_id,'SUBJECT': SUBJECT,},
    #         message_details,
    #         internalClient,
    #         collection_err,
    #         df,internalClient,collection='dailyProductDataFrame', del_db = False,**{'db_name':'IRefer',})

    # # df=main(timespan,df_product)

    
    # # print(df.columns)
    # #  hello
    # '''product_id', 'latest_time', 'added_to_basket', 'product_sold',
    #    'recommendation_click', 'referral_click', 'saved_items',
    #    'search_click'''
    # # # sort df by sales
    # # df=df.sort_values(by=['product_sold'],ascending=False)
    # # df=df.loc[:,['product_id','referral_click','product_sold']]
    df_product=df_product.head(5)
    print(df_product)
    
