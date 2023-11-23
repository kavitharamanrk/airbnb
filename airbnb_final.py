import pandas as pd
import geopandas as gpd
import pymongo
from pymongo.mongo_client import MongoClient
import mysql.connector 
import plotly.express as px
import streamlit as st
import matplotlib.pyplot as plt
import altair as alt
import numpy as np
import folium
from streamlit_folium import st_folium

def streamlit_config():

    # page configuration
    st.set_page_config(page_title='airbnb analysis - Kavitha',page_icon='airbnb.png', layout="wide")
    st.markdown(f'<h1 style="text-align: center; color:red ">Airbnb Analysis</h1>',unsafe_allow_html=True)

class Data_Collection:
    def get_data():
        try:
            client=MongoClient("mongodb+srv://user:@name.mongodb.net/?retryWrites=true&w=majority")
            db = client.sample_airbnb
            Collection=db.listingsAndReviews
            data=Collection.find({},{"_id":False})
            df=pd.DataFrame(data)
            client.close()
        except Exception as e:
            print(e)
        finally:
            print("Mongo data collected")                
        return df
    
    def mySqlConnection(query):
        try:
            mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            )
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            out=mycursor.fetchall()
            mydb.close()  
        except Exception as e:
            print("SQL DB Connection error:",{e})
        finally:
            print("DB Closed")
            return out


class Preprocessing:
    def data_cleaning(df):
        try:
        # null value handling
            df['bedrooms'].fillna(0, inplace=True)
            df['beds'].fillna(0, inplace=True)
            df['bathrooms'].fillna(0, inplace=True)
            df['cleaning_fee'].fillna('Not Specified', inplace=True)
            df['weekly_price'].fillna(0, inplace=True)
            df['monthly_price'].fillna(0, inplace=True)
            df['reviews_per_month'].fillna(0, inplace=True)
            df['security_deposit'].fillna(0, inplace=True)

            # data types conversion
            df['guests_included'] = df['guests_included'].astype(str).astype(int)
            df['reviews_per_month'] = df['reviews_per_month'].astype(float).astype(int)
            df['weekly_price'] = df['weekly_price'].astype(str).astype(float)
            df['monthly_price'] = df['monthly_price'].astype(str).astype(float)
            df['bedrooms'] = df['bedrooms'].astype(float).astype(int)
            df['minimum_nights'] = df['minimum_nights'].astype(int)
            df['maximum_nights'] = df['maximum_nights'].astype(int)
            df['bedrooms'] = df['bedrooms'].astype(int)
            df['beds'] = df['beds'].astype(int)
            df['bathrooms'] = df['bathrooms'].astype(str).astype(float).astype(int)
            df['price'] = df['price'].astype(str).astype(float)
            df['cleaning_fee'] = df['cleaning_fee'].apply(lambda x: float(str(x)) if x != 'Not Specified' else 0)
            df['extra_people'] = df['extra_people'].astype(str).astype(float).astype(int)
            df['security_deposit'] = df['security_deposit'].astype(str).astype(float)
        
            # Fine tune
            df['images'] = df['images'].apply(lambda x: x['picture_url'])
            df['review_scores_rating'] = df['review_scores'].apply(lambda x: x.get('review_scores_rating', 0))
            df['review_scores_accuracy'] =df['review_scores'].apply(lambda x: x.get('review_scores_accuracy', 0))
            df['review_scores_cleanliness'] =df['review_scores'].apply(lambda x: x.get('review_scores_cleanliness', 0))
            df['review_scores_checkin'] =df['review_scores'].apply(lambda x: x.get('review_scores_checkin', 0))
            df['review_scores_location'] =df['review_scores'].apply(lambda x: x.get('review_scores_location', 0))
            df['review_scores_communication'] =df['review_scores'].apply(lambda x: x.get('review_scores_communication', 0))
            df['review_scores_value'] =df['review_scores'].apply(lambda x: x.get('review_scores_value', 0))
            df.drop('review_scores',axis=1, inplace=True)
        
            # df['reviewer_name'] = df['reviews'].apply(lambda x: x['reviewer_name'])
            # df['review_comment'] = df['reviews'].apply(lambda x: x['comments'])
        
            # host
            df['host_id'] = df['host'].apply(lambda x: x['host_id'])
            df['host_id'] = df['host'].apply(lambda x: x['host_response_time'] if 'host_response_time' in x else 'Not Specified')
            df['host_is_superhost'] = df['host'].apply(lambda x: x['host_is_superhost'])
            df['host_has_profile_pic'] = df['host'].apply(lambda x: x['host_has_profile_pic'])
            df['host_identity_verified'] = df['host'].apply(lambda x: x['host_identity_verified'])

            df.drop(columns=['host'], inplace=True)

            # address
            df['country'] = df['address'].apply(lambda x: x['country'])
            df['country_code'] = df['address'].apply(lambda x: x['country_code'])
            df['street'] = df['address'].apply(lambda x: x['street'])
            df['government_area'] = df['address'].apply(lambda x: x['government_area'])
            df['longitude'] = df['address'].apply(lambda x: x['location']['coordinates'][0])
            df['latitude'] = df['address'].apply(lambda x: x['location']['coordinates'][1])
            df['is_location_exact'] = df['address'].apply(lambda x: x['location']['is_location_exact'])
            df.drop(columns=['address'], inplace=True)

            # bool data conversion to string
            df['is_location_exact'] = df['is_location_exact'].map({False: 'No', True: 'Yes'})       
            df['host_has_profile_pic'] = df['host_has_profile_pic'].map({False: 'No', True: 'Yes'})
            df['host_identity_verified'] = df['host_identity_verified'].map({False: 'No', True: 'Yes'})
            df['host_is_superhost'] = df['host_is_superhost'].map({False: 'No', True: 'Yes'})
        
            # Availability
            df['availability_30'] = df['availability'].apply(lambda x: x['availability_30'])
            df['availability_60'] = df['availability'].apply(lambda x: x['availability_60'])
            df['availability_90'] = df['availability'].apply(lambda x: x['availability_90'])
            df['availability_365'] = df['availability'].apply(lambda x: x['availability_365'])
            
            df.drop(columns=['availability'], inplace=True)        
            df.drop(columns=['summary'], inplace=True)
            df.drop(columns=['space'], inplace=True)
            df.drop(columns=['description'], inplace=True)
            df.drop(columns=['neighborhood_overview'], inplace=True)
            df.drop(columns=['notes'], inplace=True)
            df.drop(columns=['transit'], inplace=True)
            df.drop(columns=['access'], inplace=True)
            df.drop(columns=['interaction'], inplace=True)
            df.drop(columns=['reviews'], inplace=True)
            df.drop(columns=['house_rules'], inplace=True)        
            
            df['amenities']=df['amenities'].apply(lambda x: ','.join(x))
        except Exception as e:
            print(e)
        finally:
            print("data cleaned")
        return df

class sql:
    def create_table():
        try:
            mydb =  mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="",
                )
            mycursor = mydb.cursor()
            mycursor.execute(f"""create table if not exists airbnb(
                                listing_url			text,
                                name				varchar(255),                        
                                property_type		varchar(255),
                                room_type			varchar(255),
                                bed_type			varchar(255),
                                minimum_nights		int,
                                maximum_nights		int,
                                cancellation_policy	varchar(255),
                                last_scraped        text,
                                calendar_last_scraped text,
                                first_review        text,
                                last_review         text,   
                                accommodates		int,
                                bedrooms			int,
                                beds				int,
                                number_of_reviews	int,
                                bathrooms			float,
                                amenities           text,
                                price				float,
                                security_deposit    float,     
                                cleaning_fee		varchar(20),
                                extra_people		int,
                                guests_included		int,
                                images				text,                                                           
                                weekly_price        float,
                                monthly_price       float,
                                reviews_per_month		int,
                                review_score_rating int,
                                review_scores_accuracy int,
                                review_scores_cleanliness int,
                                review_scores_checkin int,
                                review_scores_location int,
                                review_scores_communication int,
                                review_scores_value int,
                                host_id				varchar(255),
                                host_is_superhost	varchar(25),
                                host_has_profile_pic	varchar(25),  
                                host_identity_verified	varchar(25),                                                      
                                country				varchar(255),
                                country_code				varchar(10),                              
                                street				varchar(255),
                                government_area		varchar(255),
                                longitude			float,
                                latitude			float,
                                is_location_exact	varchar(25),
                                availability_30		int,
                                availability_60		int,
                                availability_90		int,
                                availability_365	int);""")
            mydb.close()
        except Exception as e:
            print(e)
        finally:
            print("Table created")

    def data_migration(df):
        try:
            mydb =  mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                )
            mycursor = mydb.cursor()
            for i,row in df.iterrows():
                try:

                    sqlquery = "insert into airbnb \
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    mycursor.execute(sqlquery, tuple(row))
                    mydb.commit()
                except ValueError as e:
                    print(e)
                finally:
                    print("Data inserted")   
            mydb.close()
        except Exception as e:
            print(e)


    def select_qry(query,collist):
        mydb =  mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="",
            )
        mycursor = mydb.cursor()
        mycursor.execute(query)
        query_res = mycursor.fetchall()
        data = pd.DataFrame(query_res,columns=collist)
        return data

class Data_Analysis:
    def user_charts():
        st.caption("Locationwise Analysis")
        col_list3=['country','government_area']
        df3 = sql.select_qry("select  country, government_area from  order by country",col_list3)
        col7,col8 = st.columns(2)
        pcolor='reds'
        with col7:
            country_in = st.selectbox('Select Country',options=df3['country'].unique())
        with col8:
            df3 = df3.query(f'country == "{country_in}"')
            option_area = st.selectbox('Select area',options=df3['government_area'].unique())
        # df3 = df3.query(f'country == "{country_in}" and government_area=="{option_area}"')
        col_list4=['Max Price','Min Price', 'Max Monthly Price','Min Monthly Price' ,'Max Weekly Price','Min Weekly Price','Max Security Deposit', 'Max Availability in 30 days','Max Availability in 60 days','Max Availability in 90 days', 'Max Availability in 365 days']
        df4=sql.select_qry(f'select max(price),min(price),max(monthly_price),min(monthly_price),max(weekly_price),min(weekly_price),max(security_deposit),max(availability_30),max(availability_60),max(availability_90),max(availability_365) from airbnb where country="{country_in}" and government_area="{option_area}"',col_list4)
        st.dataframe(df4, hide_index=True)
        return
   
    def price_charts():
        col_list=['country','price','weekly_price','monthly_price' ,'security_deposit','room_type','bed_type','property_type','cleaning_fee']
        df1 = sql.select_qry("select country,price,weekly_price, monthly_price,security_deposit,room_type,bed_type,property_type,cleaning_fee from airbnb",col_list)
        col1,col2,col3 = st.columns(3)
        with col1:
            country_in = st.selectbox('Select a Country',options=df1['country'].unique())
        with col2:
            option_lis = st.selectbox('Select room/bed price',options=['room','bed','property type'])
        with col3:
            option_price = st.selectbox('Select any one',options=['price','monthly price','weekly price','security deposit'])
        if option_lis=='room':
            xaxis='room_type'    
        elif option_lis=='bed':
            xaxis='bed_type'
        else:
            xaxis='property_type'
        if option_price=='price':
            yaxis='price'
        elif option_price=='monthly price':
            yaxis='monthly_price'
        elif option_price=='weekly price':
            yaxis='weekly_price'
        elif option_price=='security deposit':
            yaxis='security_deposit'
        else:
            yaxis='cleaning_fee'
        df1 = df1.query(f'country == "{country_in}"  ')
        fig1 = px.bar(df1, x=xaxis, y=yaxis,color=xaxis,color_continuous_scale="reds",text=yaxis,title="Price Analysis")
        st.plotly_chart(fig1,use_container_width=True)
        return
    
    def availability_charts():
        col_list=['country','government_area','availability_30','availability_60' ,'availability_90','availability_365','price']
        df2 = sql.select_qry("select country,government_area,availability_30,availability_60,availability_90,availability_365,price from airbnb order by country",col_list)
        col4,col5,col6 = st.columns(3)
        pcolor='reds'
        with col4:
            country_in = st.selectbox('Country',options=df2['country'].unique())
        with col5:
            df2 = df2.query(f'country == "{country_in}"')
            option_area = st.selectbox('Area',options=df2['government_area'].unique())
        with col6:
            option_price = st.selectbox('Select Availability in days',options=['30','60' ,'90','365'])
        if option_price=='30':
            yaxis='availability_30'
        elif option_price=='60':
            yaxis='availability_60'
        elif option_price=='90':
            yaxis='availability_90'
        elif option_price=='365':
            yaxis='availability_365'

        df2 = df2.query(f'country == "{country_in}" and government_area=="{option_area}"')
        fig2= px.pie(df2, names='price', values=yaxis,color_discrete_sequence=px.colors.sequential.Viridis, title='Rooms Availability')
        st.plotly_chart(fig2,use_container_width=True)
        return


# streamlit title, background color and tab configuration
streamlit_config()

tab1, tab2, tab3,tab4,tab5,tab6 = st.tabs(["Airbnb Data", "Migration", "Data Analysis","Geo Visualization","Dashboard","About"])  
with tab1:
    df=Data_Collection.get_data()
    df=Preprocessing.data_cleaning(df)
    st.dataframe(df, hide_index=True)

with tab2:
    sql.create_table()
    st.dataframe(df, hide_index=True)

    if (st.button("Migration")):
        sql.data_migration(df)
        st.success('Successfully Data Migrated to SQL Database')
        st.balloons() 

with tab3:
    Data_Analysis.user_charts()
    Data_Analysis.price_charts()
    Data_Analysis.availability_charts()

with tab4:
    cities=gpd.read_file(gpd.datasets.get_path("naturalearth_cities"))
    cities["long"]=cities["geometry"].x
    cities["lat"]=cities["geometry"].y
    mydb =  mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="",
    )
    mycursor = mydb.cursor()
    mycursor.execute("select  country,government_area,name,country_code,price,longitude,latitude,review_score_rating from airbnb")
    query_res = mycursor.fetchall()
    map_df = pd.DataFrame(query_res,columns=['country','area','airbnbname','country_code','price','longitude','latitude','review_score_rating'])
    map_df['cities']=cities['name']
    map_df["long"]=cities["long"]
    map_df["lat"]=cities["lat"]
    fig=px.scatter_mapbox(map_df,
                          lat="latitude",
                          lon="longitude",
                          hover_name='airbnbname',
                          hover_data='price',
                          color="cities",
                          zoom=5,
                          height=500,
                          width=1200
                          )
    fig.update_layout(mapbox_style='stamen-terrain')
    fig.update_layout(title_text='Airbnb')
    st.plotly_chart(fig, use_container_width=True)

with tab5:
    st.image("airbnb_dashboard.png")

with tab6:
    st.markdown("* :blue[This application is used to analyze Airbnb data using MongoDB Atlas, perform data cleaning and preparation, develop interactive geospatial visualizations, and create dynamic plots to gain insights into pricing variations, availability patterns, and location-based trends.]")
    st.markdown("* :blue[Used Tech: Python, Streamlit, MongoDb, PowerBI and MySQL.]")
  
