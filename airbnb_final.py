import pandas as pd
import geopandas as gpd
import pymongo
from pymongo.mongo_client import MongoClient
import mysql.connector 
import plotly.express as px
import streamlit as st

def streamlit_config():

    # page configuration
    st.set_page_config(page_title='airbnb analysis - Kavitha',page_icon='airbnb.png', layout="wide")
    st.markdown(f'<h1 style="text-align: center; color:red ">Airbnb Analysis</h1>',unsafe_allow_html=True)

class Data_Collection:
    def get_data():
        try:
            client=MongoClient("mongodb+srv://user:pwd@cluster.ygqmci0.mongodb.net/?retryWrites=true&w=majority")
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
            database="airbnb",
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
                database="airbnb",
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
                database="airbnb",
                )
            mycursor = mydb.cursor()
            for i,row in df.iterrows():
                try:
                    # print(i,row)
                    # print(tuple(row))
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

class plotly:

    def pie_chart(df, x, y, title, title_x=0.20):

        fig = px.pie(df, names=x, values=y, hole=0.5, title=title)

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(text=df[y], textinfo='percent+value',
                          textposition='outside',
                          textfont=dict(color='white'))

        st.plotly_chart(fig, use_container_width=True)

    def horizontal_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[x]) * 0.75 else 'outside' for val in df[x]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True)

    def vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{y}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)

    def line_chart(df, x, y, text, textposition, color, title, title_x=0.25):

        fig = px.line(df, x=x, y=y, labels={
                      x: '', y: ''}, title=title, text=df[text])

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(line=dict(color=color, width=3.5),
                          marker=dict(symbol='diamond', size=10),
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=13.5),
                          textposition=textposition,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)


class Data_Analysis:

    def charts_str(query,collist):
        mydb =  mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="airbnb",
            )
        mycursor = mydb.cursor()
        mycursor.execute(query)
        query_res = mycursor.fetchall()
        data = pd.DataFrame(query_res,columns=collist)
        return data

    def charts_structure(column_name, order='count desc'):
        mydb =  mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="airbnb",
            )
        mycursor = mydb.cursor()
        mycursor.execute(f"""select distinct {column_name}, count({column_name}) as count
                           from airbnb
                           group by {column_name} order by {order} limit 10;""")
        s = mycursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=[column_name, 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data[column_name].apply(lambda x: str(x)+'`')
        return data

    def cleaning_fee():
        mydb =  mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="airbnb",
            )
        mycursor = mydb.cursor()
        mycursor.execute(f"""select distinct cleaning_fee, count(cleaning_fee) as count
                           from airbnb
                           where cleaning_fee != 'Not Specified'
                           group by cleaning_fee
                           order by count desc
                           limit 10;""")
        s = mycursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=['cleaning_fee', 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data['cleaning_fee'].apply(lambda x: str(x)+'`')
        return data

    def location():
        mydb =  mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="airbnb",
            )
        mycursor = mydb.cursor()
        mycursor.execute(f"""select host_id, country, longitude, latitude
                           from airbnb
                           group by host_id, country, longitude, latitude""")
        s = mycursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(
            s, columns=['Host ID', 'Country', 
                        'Longitude', 'Latitude'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def charts():
        # vertical_bar chart
        col_list=['Property Type','Property Count']
        df1 = Data_Analysis.charts_str("select property_type, count(*) as count from airbnb group by property_type order by property_type",col_list)
        st.bar_chart(df1, x='Property Type', y='Property Count',color='#4933FF', width=0, height=0, use_container_width=True)


        # line & pie chart
        col1, col2 = st.columns(2)
        with col1:
            col_list=['Bed Type','Count']
            bed_type = Data_Analysis.charts_str("select bed_type, count(*) as count from airbnb group by bed_type order by bed_type",col_list)
            fig = px.pie(bed_type, values='Count', names='Bed Type',color_discrete_sequence=px.colors.sequential.Viridis, title='Bed Type')
            st.plotly_chart(fig)   
        with col2:
            room_type = Data_Analysis.charts_structure('room_type')
            plotly.pie_chart(df=room_type, x='room_type',y='count', title='Room Type', title_x=0.30)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Minimum Nights', 'Maximum Nights'])
        with tab1:
            minimum_nights = Data_Analysis.charts_structure('minimum_nights')
            plotly.vertical_bar_chart(df=minimum_nights, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Minimum Nights', title_x=0.43)
        with tab2:
            maximum_nights = Data_Analysis.charts_structure('maximum_nights')
            plotly.vertical_bar_chart(df=maximum_nights, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Maximum Nights', title_x=0.43)

        # line chart
        cancellation_policy = Data_Analysis.charts_structure('cancellation_policy')
        plotly.line_chart(df=cancellation_policy, y='cancellation_policy', x='count', text='percentage', color='#5D9A96',
                          textposition=['top center', 'top right',
                                        'top center', 'bottom center', 'middle right'],
                          title='Cancellation Policy', title_x=0.43)

        # vertical_bar chart
        accommodates = Data_Analysis.charts_structure('accommodates')
        plotly.vertical_bar_chart(df=accommodates, x='y', y='count', text='percentage',
                                  color='#5D9A96', title='Accommodates', title_x=0.43)

        # vertical_bar chart
        tab1, tab2, tab3 = st.tabs(['Bedrooms', 'Beds', 'Bathrooms'])
        with tab1:
            bedrooms = Data_Analysis.charts_structure('bedrooms')
            plotly.vertical_bar_chart(df=bedrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bedrooms', title_x=0.43)
        with tab2:
            beds = Data_Analysis.charts_structure('beds')
            plotly.vertical_bar_chart(df=beds, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Beds', title_x=0.43)
        with tab3:
            bathrooms = Data_Analysis.charts_structure('bathrooms')
            plotly.vertical_bar_chart(df=bathrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bathrooms', title_x=0.43)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(
            ['Price', 'Cleaning Fee', 'Extra People', 'Guests Included'])
        with tab1:
            price = Data_Analysis.charts_structure('price')
            plotly.vertical_bar_chart(df=price, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Price', title_x=0.43)
        with tab2:
            cleaning_fee = Data_Analysis.cleaning_fee()
            plotly.vertical_bar_chart(df=cleaning_fee, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Cleaning Fee', title_x=0.43)
        with tab3:
            extra_people = Data_Analysis.charts_structure('extra_people')
            plotly.vertical_bar_chart(df=extra_people, x='y', y='count', text='percentage',
                                      color='#5D9Ae96', title='Extra People', title_x=0.43)
        with tab4:
            guests_included = Data_Analysis.charts_structure('guests_included')
            plotly.vertical_bar_chart(df=guests_included, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Guests Included', title_x=0.43)

        # line chart
        host_response_time = Data_Analysis.charts_structure('host_response_time')
        plotly.line_chart(df=host_response_time, y='host_response_time', x='count', text='percentage', color='#5cb85c',
                          textposition=['top center', 'top right',
                                        'top right', 'bottom left', 'bottom left'],
                          title='Host Response Time', title_x=0.43)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Host Response Rate', 'Host Listings Count'])
        with tab1:
            host_response_rate = Data_Analysis.charts_structure('host_response_rate')
            plotly.vertical_bar_chart(df=host_response_rate, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Host Response Rate', title_x=0.43)
        with tab2:
            host_listings_count = Data_Analysis.charts_structure('host_listings_count')
            plotly.vertical_bar_chart(df=host_listings_count, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Host Listings Count', title_x=0.43)

        # pie chart
        tab1, tab2, tab3 = st.tabs(
            ['Host is Superhost', 'Host has Profile Picture', 'Host Identity Verified'])
        with tab1:
            host_is_superhost = Data_Analysis.charts_structure('host_is_superhost')
            plotly.pie_chart(df=host_is_superhost, x='host_is_superhost',
                             y='count', title='Host is Superhost', title_x=0.39)
        with tab2:
            host_has_profile_pic = Data_Analysis.charts_structure('host_has_profile_pic')
            plotly.pie_chart(df=host_has_profile_pic, x='host_has_profile_pic',
                             y='count', title='Host has Profile Picture', title_x=0.37)
        with tab3:
            host_identity_verified = Data_Analysis.charts_structure('host_identity_verified')
            plotly.pie_chart(df=host_identity_verified, x='host_identity_verified',
                             y='count', title='Host Identity Verified', title_x=0.37)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3 = st.tabs(['Market', 'Country', 'Location Exact'])
        with tab1:
            market = Data_Analysis.charts_structure('market', limit=12)
            plotly.vertical_bar_chart(df=market, x='market', y='count', text='percentage',
                                      color='#5D9A96', title='Market', title_x=0.43)
        with tab2:
            country = Data_Analysis.charts_structure('country')
            plotly.vertical_bar_chart(df=country, x='country', y='count', text='percentage',
                                      color='#5D9A96', title='Country', title_x=0.43)
        with tab3:
            is_location_exact = Data_Analysis.charts_structure('is_location_exact')
            plotly.pie_chart(df=is_location_exact, x='is_location_exact', y='count',
                             title='Location Exact', title_x=0.37)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3, tab4 = st.tabs(['Availability 30', 'Availability 60',
                                          'Availability 90', 'Availability 365'])
        with tab1:
            availability_30 = Data_Analysis.charts_structure('availability_30')
            plotly.vertical_bar_chart(df=availability_30, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 30', title_x=0.45)
        with tab2:
            availability_60 = Data_Analysis.charts_structure('availability_60')
            plotly.vertical_bar_chart(df=availability_60, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 60', title_x=0.45)
        with tab3:
            availability_90 = Data_Analysis.charts_structure('availability_90')
            plotly.vertical_bar_chart(df=availability_90, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 90', title_x=0.45)
        with tab4:
            availability_365 = Data_Analysis.charts_structure('availability_365')
            plotly.vertical_bar_chart(df=availability_365, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 365', title_x=0.45)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3 = st.tabs(
            ['Number of Reviews', 'Maximum Number of Reviews', 'Review Scores'])
        with tab1:
            number_of_reviews = Data_Analysis.charts_structure('number_of_reviews')
            plotly.vertical_bar_chart(df=number_of_reviews, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Number of Reviews', title_x=0.43)
        with tab2:
            max_number_of_reviews = Data_Analysis.charts_structure(
                'number_of_reviews', order='number_of_reviews desc')
            plotly.vertical_bar_chart(df=max_number_of_reviews, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Maximum Number of Reviews', title_x=0.35)
        with tab3:
            review_scores = Data_Analysis.charts_structure('review_scores')
            plotly.vertical_bar_chart(df=review_scores, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Review Scores', title_x=0.43)


# streamlit title, background color and tab configuration
streamlit_config()

tab1, tab2, tab3,tab4,tab5 = st.tabs(["Airbnb Data", "Migration", "Analysis","Geo Visualization","About"])  
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
    Data_Analysis.charts()

with tab4:
    df = pd.DataFrame(np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],columns=['lat', 'lon'])
    st.map(d)
with tab5:
    st.markdown("* :blue[This application is used to analyze Airbnb data using MongoDB Atlas, perform data cleaning and preparation, develop interactive geospatial visualizations, and create dynamic plots to gain insights into pricing variations, availability patterns, and location-based trends.]")
    st.markdown("* :blue[Used Tech:Python , Streamlit, MongoDb, PowerBI and MySQL]")
  