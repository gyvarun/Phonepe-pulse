import pandas as pd
import json
import os


import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

import streamlit as st

import plotly.express as px
from urllib.request import urlopen

with urlopen("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson") as response:
    geojson_1 = json.load(response)

mapping = {'arunachal-pradesh':'Arunachal Pradesh', 'assam':'Assam', 'chandigarh':'Chandigarh', 'karnataka':'Karnataka', 'manipur':'Manipur',
'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland', 'punjab':'Punjab', 'rajasthan':'Rajasthan',
'sikkim':'Sikkim', 'tripura':'Tripura', 'uttarakhand':'Uttarakhand', 'telangana':'Telangana', 'bihar':'Bihar', 'kerala':'Kerala',
'madhya-pradesh':'Madhya Pradesh', 'andaman-&-nicobar-islands':'Andaman & Nicobar', 'gujarat':'Gujarat', 'lakshadweep':'Lakshadweep',
'odisha':'Odisha', 'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'ladakh':'Ladakh',
'jammu-&-kashmir':'Jammu & Kashmir', 'chhattisgarh':'Chhattisgarh', 'delhi':'Delhi', 'goa':'Goa', 'haryana':'Haryana',
'himachal-pradesh':'Himachal Pradesh', 'jharkhand':'Jharkhand', 'tamil-nadu':'Tamil Nadu', 'uttar-pradesh':'Uttar Pradesh',
'west-bengal':'West Bengal', 'andhra-pradesh':'Andhra Pradesh', 'puducherry':'Puducherry', 'maharashtra':'Maharashtra'}

# Configure Streamlit Interface
st.set_page_config(page_title='PhonePe Pulse',
                   layout="wide")

# Page Title
st.title(':violet[PhonePe Pulse]')
st.write(":red[**Unlocking PhonePe's Potential: Dive into Data Analysis for Actionable Insights!**]")


#Aggregated transactions
path="C:\\Users\\91916\\GUVI_DS\\pulse\\data\\aggregated\\transaction\\country\\india\\state"
Agg_state_list_t=os.listdir(path)

clm={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for state_t in Agg_state_list_t: #state
    p_state_t=path +'\\' + state_t 
    Agg_yr_t=os.listdir(p_state_t)
    for year_t in Agg_yr_t: #year
        p_year_t=p_state_t+"\\"+year_t
        Agg_yr_list_t=os.listdir(p_year_t)
        for file_t in Agg_yr_list_t:
            p_file_t=p_year_t+'\\'+ file_t #quarterly data(json)
            Data=open(p_file_t,'r')
            Trans_agg=json.load(Data)
            for a in Trans_agg['data']['transactionData']:
                name=a['name']
                count=a['paymentInstruments'][0]['count']
                amount=a['paymentInstruments'][0]['amount']
                clm['Transaction_type'].append(name)
                clm['Transaction_count'].append(count)
                clm['Transaction_amount'].append(amount)
                clm['State'].append(state_t)
                clm['Year'].append(year_t)
                clm['Quarter'].append(int(file_t.strip('.json')))
    #Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)
Agg_Trans.replace({'State': mapping}, inplace=True)

#Aggregated users 
path_1="C:\\Users\\91916\\GUVI_DS\\pulse\\data\\aggregated\\user\\country\\india\\state"
Agg_state_list_u=os.listdir(path_1)

clm_1={'State':[], 'Year':[],'Quarter':[],'Users':[], 'App_opens':[]}
for state_u in Agg_state_list_u: #state
    p_state_u=path_1 +'\\' + state_u 
    Agg_yr_u=os.listdir(p_state_u)
    for year_u in Agg_yr_u: #year
        p_year_u=p_state_u+"\\"+year_u
        Agg_yr_list_u=os.listdir(p_year_u)
        for file_u in Agg_yr_list_u:
            p_file_u=p_year_u+'\\'+ file_u #quarterly data(json)
            Data_1=open(p_file_u,'r')
            User=json.load(Data_1)
            users=User['data']['aggregated']['registeredUsers']
            app_opens=User['data']['aggregated']['appOpens']
            clm_1['Users'].append(users)
            clm_1['App_opens'].append(app_opens)
            clm_1['State'].append(state_u)
            clm_1['Year'].append(year_u)
            clm_1['Quarter'].append(int(file_u.strip('.json')))
#Succesfully created a dataframe
Agg_User=pd.DataFrame(clm_1)
Agg_User.replace({'State': mapping}, inplace=True)


#Top transactions
path_3="C:\\Users\\91916\\GUVI_DS\\pulse\\data\\top\\transaction\\country\\india\\state"
Top_state_list_t=os.listdir(path_3)

clm_2={'State':[], 'Year':[],'Quarter':[],'District':[], 'Transaction_count':[], 'Transaction_amount':[]}
clm_3={'State':[], 'Year':[],'Quarter':[],'Pincode':[], 'Transaction_count':[], 'Transaction_amount':[]}

for state_tt in Top_state_list_t: #state
    p_state_tt=path_3 +'\\' + state_tt 
    Top_yr_t=os.listdir(p_state_tt)
    for year_tt in Top_yr_t: #year
        p_year_tt=p_state_tt+"\\"+year_tt
        Top_yr_list_tt=os.listdir(p_year_tt)
        for file_tt in Top_yr_list_tt:
            p_file_tt=p_year_tt+'\\'+ file_tt #quarterly data(json)
            Data_3=open(p_file_tt,'r')
            Trans_top=json.load(Data_3)
            for c in Trans_top['data']['districts']:
                name_2=c['entityName']
                count_2=c['metric']['count']
                amount_2=c['metric']['amount']
                clm_2['District'].append(name_2)
                clm_2['Transaction_count'].append(count_2)
                clm_2['Transaction_amount'].append(amount_2)
                clm_2['State'].append(state_tt)
                clm_2['Year'].append(year_tt)
                clm_2['Quarter'].append(int(file_tt.strip('.json')))
            for d in Trans_top['data']['pincodes']:
                name_3=d['entityName']
                count_3=d['metric']['count']
                amount_3=d['metric']['amount']
                clm_3['Pincode'].append(name_3)
                clm_3['Transaction_count'].append(count_3)
                clm_3['Transaction_amount'].append(amount_3)
                clm_3['State'].append(state_tt)
                clm_3['Year'].append(year_tt)
                clm_3['Quarter'].append(int(file_tt.strip('.json')))
                
#Succesfully created a dataframe
Top_Trans_dis=pd.DataFrame(clm_2)
Top_Trans_dis.replace({'State': mapping}, inplace=True)

Top_Trans_pin=pd.DataFrame(clm_3)
Top_Trans_pin.replace({'State': mapping}, inplace=True)


#Top users

path_4="C:\\Users\\91916\\GUVI_DS\\pulse\\data\\top\\user\\country\\india\\state"
Top_state_list_u=os.listdir(path_4)

clm_4={'State':[], 'Year':[],'Quarter':[],'District':[], 'Users':[]}
clm_5={'State':[], 'Year':[],'Quarter':[],'Pincode':[],'Users':[]}

for state_tu in Top_state_list_u: #state
    p_state_tu=path_4 +'\\' + state_tu 
    Top_yr_u=os.listdir(p_state_tu)
    for year_tu in Top_yr_u: #year
        p_year_tu=p_state_tu+"\\"+year_tu
        Top_yr_list_tu=os.listdir(p_year_tu)
        for file_tu in Top_yr_list_tu:
            p_file_tu=p_year_tu+'\\'+ file_tu #quarterly data(json)
            Data_4=open(p_file_tu,'r')
            User_top=json.load(Data_4)
            name_4=User_top['data']['districts'][0]['name']
            count_4=User_top['data']['districts'][0]['registeredUsers']
            clm_4['District'].append(name_4)
            clm_4['Users'].append(count_4)
            clm_4['State'].append(state_tu)
            clm_4['Year'].append(year_tu)
            clm_4['Quarter'].append(int(file_tu.strip('.json')))

            name_5=User_top['data']['pincodes'][0]['name']
            count_5=User_top['data']['pincodes'][0]['registeredUsers']
            clm_5['Pincode'].append(name_5)
            clm_5['Users'].append(count_5)
            clm_5['State'].append(state_tu)
            clm_5['Year'].append(year_tu)
            clm_5['Quarter'].append(int(file_tu.strip('.json')))

#Succesfully created a dataframe
Top_user_dis=pd.DataFrame(clm_4)
Top_user_dis.replace({'State': mapping}, inplace=True)

Top_user_pin=pd.DataFrame(clm_5)
Top_user_pin.replace({'State': mapping}, inplace=True)

# Data Migrate to MySQL 

# Connect to the MySQL server
connect = mysql.connector.connect(
host = "localhost",
user = "root",
password = "root",
auth_plugin = "mysql_native_password")

# Create a new database and use
mycursor = connect.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")

# Close the cursor and database connection
mycursor.close()
connect.close()

# Connect to the new created database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/phonepe_db', echo=False)

#Data migration to SQL
Agg_Trans.to_sql('agg_trans', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "Transaction_type": sqlalchemy.types.VARCHAR(length=225),
                                "Transaction_count": sqlalchemy.types.BigInteger,
                                "Transaction_amount": sqlalchemy.types.BigInteger,})

Agg_User.to_sql('agg_user', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "Users": sqlalchemy.types.BigInteger,
                                "App_opens": sqlalchemy.types.BigInteger,})


Top_Trans_dis.to_sql('top_trans_dis', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "District": sqlalchemy.types.VARCHAR(length=225),
                                "Transaction_count": sqlalchemy.types.BigInteger,
                                "Transaction_amount": sqlalchemy.types.BigInteger,})

Top_Trans_pin.to_sql('top_trans_pin', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "Pincode": sqlalchemy.types.INT,
                                "Transaction_count": sqlalchemy.types.BigInteger,
                                "Transaction_amount": sqlalchemy.types.BigInteger,})


Top_user_dis.to_sql('top_user_dis', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "District": sqlalchemy.types.VARCHAR(length=225),
                                "Users": sqlalchemy.types.BigInteger,})

Top_user_pin.to_sql('top_user_pin', engine, if_exists='replace', index=False,
                 dtype={"State": sqlalchemy.types.VARCHAR(length=225),
                                "Year": sqlalchemy.types.INT,
                                "Quarter": sqlalchemy.types.INT,
                                "Pincode": sqlalchemy.types.INT,
                                "Users": sqlalchemy.types.BigInteger,})

tab1, tab2 = st.tabs([ "Data Visualization", "Data Analysis"])
#Streamlit dashboard

with tab1:
    with st.sidebar:
        year = st.selectbox(
            "Choose Year",
            ("Select here", "2018", "2019", "2020", "2021", "2022", "2023"))
    with st.sidebar:
        radio = (st.selectbox("Choose Quarter",("Select here","Q1", "Q2", "Q3", "Q4", "Top States")))

        
    with st.sidebar:
        radio_1 = (st.selectbox("Choose Category",("Select here","Transactions", "Users" )))

        
    #SQL connection

    connect_for_data = pymysql.connect(host='localhost', user='root', password='root', db='phonepe_db')
    cursor = connect_for_data.cursor()


    # --------State vs Transactions---------

    if year=='2018' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df1.index += 1

        fig1 = px.choropleth_mapbox(df1, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig1.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig1,use_container_width=True)
        st.dataframe(df1)

    if year=='2018' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_2 = cursor.fetchall()
        df2 = pd.DataFrame(result_2, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df2.index += 1

        fig2 = px.choropleth_mapbox(df2, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig2.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig2,use_container_width=True)
        st.dataframe(df2)

    if year=='2018' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_3 = cursor.fetchall()
        df3 = pd.DataFrame(result_3, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df3.index += 1

        fig3 = px.choropleth_mapbox(df3, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig3.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig3,use_container_width=True)
        st.dataframe(df3)

    if year=='2018' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_4 = cursor.fetchall()
        df4 = pd.DataFrame(result_4, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df4.index += 1

        fig4 = px.choropleth_mapbox(df4, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig4.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig4,use_container_width=True)
        st.dataframe(df4)


    if year=='2019' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_5 = cursor.fetchall()
        df5 = pd.DataFrame(result_5, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df5.index += 1

        fig5 = px.choropleth_mapbox(df5, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig5.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig5,use_container_width=True)
        st.dataframe(df5)

    if year=='2019' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_6 = cursor.fetchall()
        df6 = pd.DataFrame(result_6, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df6.index += 1

        fig6 = px.choropleth_mapbox(df6, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig6.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig6,use_container_width=True)
        st.dataframe(df6)

    if year=='2019' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_7 = cursor.fetchall()
        df7 = pd.DataFrame(result_7, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df7.index += 1

        fig7 = px.choropleth_mapbox(df7, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig7.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig7,use_container_width=True)
        st.dataframe(df7)

    if year=='2019' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_8 = cursor.fetchall()
        df8 = pd.DataFrame(result_8, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df8.index += 1

        fig8 = px.choropleth_mapbox(df8, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig8.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig8,use_container_width=True)
        st.dataframe(df8)


    if year=='2020' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_9 = cursor.fetchall()
        df9 = pd.DataFrame(result_9, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df9.index += 1

        fig9 = px.choropleth_mapbox(df9, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig9.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig9,use_container_width=True)
        st.dataframe(df9)

    if year=='2020' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_10 = cursor.fetchall()
        df10 = pd.DataFrame(result_10, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df10.index += 1

        fig10 = px.choropleth_mapbox(df10, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig10.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig10,use_container_width=True)
        st.dataframe(df10)

    if year=='2020' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_11 = cursor.fetchall()
        df11 = pd.DataFrame(result_11, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df11.index += 1

        fig11 = px.choropleth_mapbox(df11, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig11.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig11,use_container_width=True)
        st.dataframe(df11)

    if year=='2020' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_12 = cursor.fetchall()
        df12 = pd.DataFrame(result_12, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df12.index += 1

        fig12 = px.choropleth_mapbox(df12, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig12.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig12,use_container_width=True)
        st.dataframe(df12)


    if year=='2021' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_13 = cursor.fetchall()
        df13 = pd.DataFrame(result_13, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df13.index += 1

        fig13 = px.choropleth_mapbox(df13, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig13.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig13,use_container_width=True)
        st.dataframe(df13)

    if year=='2021' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_14 = cursor.fetchall()
        df14 = pd.DataFrame(result_14, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df14.index += 1

        fig14 = px.choropleth_mapbox(df14, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig14.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig14,use_container_width=True)
        st.dataframe(df14)

    if year=='2021' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_15 = cursor.fetchall()
        df15 = pd.DataFrame(result_15, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df15.index += 1

        fig15 = px.choropleth_mapbox(df15, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig15.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig15,use_container_width=True)
        st.dataframe(df15)

    if year=='2021' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_16 = cursor.fetchall()
        df16 = pd.DataFrame(result_16, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df16.index += 1

        fig16 = px.choropleth_mapbox(df16, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig16.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig16,use_container_width=True)
        st.dataframe(df16)

    if year=='2022' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_17 = cursor.fetchall()
        df17 = pd.DataFrame(result_17, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df17.index += 1

        fig17 = px.choropleth_mapbox(df17, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig17.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig17,use_container_width=True)
        st.dataframe(df17)

    if year=='2022' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_18 = cursor.fetchall()
        df18 = pd.DataFrame(result_18, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df18.index += 1

        fig18 = px.choropleth_mapbox(df18, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig18.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig18,use_container_width=True)
        st.dataframe(df18)

    if year=='2022' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_19 = cursor.fetchall()
        df19 = pd.DataFrame(result_19, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df19.index += 1

        fig19 = px.choropleth_mapbox(df19, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig19.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig19,use_container_width=True)
        st.dataframe(df19)

    if year=='2022' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_20 = cursor.fetchall()
        df20 = pd.DataFrame(result_20, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df20.index += 1

        fig20 = px.choropleth_mapbox(df20, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig20.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig20,use_container_width=True)
        st.dataframe(df20)


    if year=='2023' and radio=='Q1' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 1 GROUP BY State;")
        result_21 = cursor.fetchall()
        df21 = pd.DataFrame(result_21, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df21.index += 1

        fig21 = px.choropleth_mapbox(df21, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig21.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig21,use_container_width=True)
        st.dataframe(df21)

    if year=='2023' and radio=='Q2' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 2 GROUP BY State;")
        result_22 = cursor.fetchall()
        df22 = pd.DataFrame(result_22, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df22.index += 1

        fig22 = px.choropleth_mapbox(df22, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig22.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig22,use_container_width=True)
        st.dataframe(df22)

    if year=='2023' and radio=='Q3' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 3 GROUP BY State;")
        result_23 = cursor.fetchall()
        df23 = pd.DataFrame(result_23, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df23.index += 1

        fig23 = px.choropleth_mapbox(df23, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig23.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig23,use_container_width=True)
        st.dataframe(df23)

    if year=='2023' and radio=='Q4' and radio_1=='Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM agg_trans where Year=2018 and Quarter = 4 GROUP BY State;")
        result_24 = cursor.fetchall()
        df24 = pd.DataFrame(result_24, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df24.index += 1

        fig24 = px.choropleth_mapbox(df24, 
                                geojson=geojson_1, 
                                locations='State', color='Transaction_amount',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_Trans['Transaction_amount'].iloc[4313]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig24.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig24,use_container_width=True)
        st.dataframe(df24)


    # --------State vs Users---------

    if year=='2018' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2018 and Quarter = 1 GROUP BY State;")
        result_25 = cursor.fetchall()
        df25 = pd.DataFrame(result_25, columns=['State', 'Users']).reset_index(drop=True)
        df25.index += 1

        fig25 = px.choropleth_mapbox(df25, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig25.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig25,use_container_width=True)
        st.dataframe(df25)

    if year=='2018' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2018 and Quarter = 2 GROUP BY State;")
        result_26 = cursor.fetchall()
        df26 = pd.DataFrame(result_26, columns=['State', 'Users']).reset_index(drop=True)
        df26.index += 1

        fig26 = px.choropleth_mapbox(df26, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig26.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig26,use_container_width=True)
        st.dataframe(df26)

    if year=='2018' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2018 and Quarter = 3 GROUP BY State;")
        result_27 = cursor.fetchall()
        df27 = pd.DataFrame(result_27, columns=['State', 'Users']).reset_index(drop=True)
        df27.index += 1

        fig27 = px.choropleth_mapbox(df27, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig27.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig27,use_container_width=True)
        st.dataframe(df27)

    if year=='2018' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2018 and Quarter = 4 GROUP BY State;")
        result_28 = cursor.fetchall()
        df28 = pd.DataFrame(result_28, columns=['State', 'Users']).reset_index(drop=True)
        df28.index += 1

        fig28 = px.choropleth_mapbox(df28, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig28.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig28,use_container_width=True)
        st.dataframe(df28)


    if year=='2019' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2019 and Quarter = 1 GROUP BY State;")
        result_29 = cursor.fetchall()
        df29 = pd.DataFrame(result_29, columns=['State', 'Users']).reset_index(drop=True)
        df29.index += 1

        fig29 = px.choropleth_mapbox(df29, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig29.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig29,use_container_width=True)
        st.dataframe(df29)

    if year=='2019' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2019 and Quarter = 2 GROUP BY State;")
        result_30 = cursor.fetchall()
        df30 = pd.DataFrame(result_30, columns=['State', 'Users']).reset_index(drop=True)
        df30.index += 1

        fig30 = px.choropleth_mapbox(df30, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig30.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig30,use_container_width=True)
        st.dataframe(df30)

    if year=='2019' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2019 and Quarter = 3 GROUP BY State;")
        result_31 = cursor.fetchall()
        df31 = pd.DataFrame(result_31, columns=['State', 'Users']).reset_index(drop=True)
        df31.index += 1

        fig31 = px.choropleth_mapbox(df31, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig31.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig31,use_container_width=True)
        st.dataframe(df31)

    if year=='2019' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2019 and Quarter = 4 GROUP BY State;")
        result_32 = cursor.fetchall()
        df32 = pd.DataFrame(result_32, columns=['State', 'Users']).reset_index(drop=True)
        df32.index += 1

        fig32 = px.choropleth_mapbox(df32, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig32.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig32,use_container_width=True)
        st.dataframe(df32)


    if year=='2020' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2020 and Quarter = 1 GROUP BY State;")
        result_33 = cursor.fetchall()
        df33 = pd.DataFrame(result_33, columns=['State', 'Users']).reset_index(drop=True)
        df33.index += 1

        fig33 = px.choropleth_mapbox(df33, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig33.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig33,use_container_width=True)
        st.dataframe(df33)

    if year=='2020' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2020 and Quarter = 2 GROUP BY State;")
        result_34 = cursor.fetchall()
        df34 = pd.DataFrame(result_34, columns=['State', 'Users']).reset_index(drop=True)
        df34.index += 1

        fig34 = px.choropleth_mapbox(df34, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig34.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig34,use_container_width=True)
        st.dataframe(df34)

    if year=='2020' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2020 and Quarter = 3 GROUP BY State;")
        result_35 = cursor.fetchall()
        df35 = pd.DataFrame(result_35, columns=['State', 'Users']).reset_index(drop=True)
        df35.index += 1

        fig35 = px.choropleth_mapbox(df35, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig35.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig35,use_container_width=True)
        st.dataframe(df35)

    if year=='2020' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2020 and Quarter = 4 GROUP BY State;")
        result_36 = cursor.fetchall()
        df36 = pd.DataFrame(result_36, columns=['State', 'Users']).reset_index(drop=True)
        df36.index += 1

        fig36 = px.choropleth_mapbox(df36, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig36.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig36,use_container_width=True)
        st.dataframe(df36)


    if year=='2021' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2021 and Quarter = 1 GROUP BY State;")
        result_37 = cursor.fetchall()
        df37 = pd.DataFrame(result_37, columns=['State', 'Users']).reset_index(drop=True)
        df37.index += 1

        fig37 = px.choropleth_mapbox(df37, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig37.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig37,use_container_width=True)
        st.dataframe(df37)

    if year=='2021' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2021 and Quarter = 2 GROUP BY State;")
        result_38 = cursor.fetchall()
        df38 = pd.DataFrame(result_38, columns=['State', 'Users']).reset_index(drop=True)
        df38.index += 1

        fig38 = px.choropleth_mapbox(df38, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig38.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig38,use_container_width=True)
        st.dataframe(df38)

    if year=='2021' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2021 and Quarter = 3 GROUP BY State;")
        result_39 = cursor.fetchall()
        df39 = pd.DataFrame(result_39, columns=['State', 'Users']).reset_index(drop=True)
        df39.index += 1

        fig39 = px.choropleth_mapbox(df39, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig39.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig39,use_container_width=True)
        st.dataframe(df39)

    if year=='2021' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2021 and Quarter = 4 GROUP BY State;")
        result_40 = cursor.fetchall()
        df40 = pd.DataFrame(result_40, columns=['State', 'Users']).reset_index(drop=True)
        df40.index += 1

        fig40 = px.choropleth_mapbox(df40, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig40.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig40,use_container_width=True)
        st.dataframe(df40)

    if year=='2022' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2022 and Quarter = 1 GROUP BY State;")
        result_41 = cursor.fetchall()
        df41 = pd.DataFrame(result_41, columns=['State', 'Users']).reset_index(drop=True)
        df41.index += 1

        fig41 = px.choropleth_mapbox(df41, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig41.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig41,use_container_width=True)
        st.dataframe(df41)

    if year=='2022' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2022 and Quarter = 2 GROUP BY State;")
        result_42 = cursor.fetchall()
        df42 = pd.DataFrame(result_42, columns=['State', 'Users']).reset_index(drop=True)
        df42.index += 1

        fig42 = px.choropleth_mapbox(df42, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig42.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig42,use_container_width=True)
        st.dataframe(df42)

    if year=='2022' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2022 and Quarter = 3 GROUP BY State;")
        result_43 = cursor.fetchall()
        df43 = pd.DataFrame(result_43, columns=['State', 'Users']).reset_index(drop=True)
        df43.index += 1

        fig43 = px.choropleth_mapbox(df43, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig43.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig43,use_container_width=True)
        st.dataframe(df43)

    if year=='2022' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2022 and Quarter = 4 GROUP BY State;")
        result_44 = cursor.fetchall()
        df44 = pd.DataFrame(result_44, columns=['State', 'Users']).reset_index(drop=True)
        df44.index += 1

        fig44 = px.choropleth_mapbox(df44, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig44.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig44,use_container_width=True)
        st.dataframe(df44)


    if year=='2023' and radio=='Q1' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2023 and Quarter = 1 GROUP BY State;")
        result_45 = cursor.fetchall()
        df45 = pd.DataFrame(result_45, columns=['State', 'Users']).reset_index(drop=True)
        df45.index += 1

        fig45 = px.choropleth_mapbox(df45, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig45.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig45,use_container_width=True)
        st.dataframe(df45)

    if year=='2023' and radio=='Q2' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2023 and Quarter = 2 GROUP BY State;")
        result_46 = cursor.fetchall()
        df46 = pd.DataFrame(result_46, columns=['State', 'Users']).reset_index(drop=True)
        df46.index += 1

        fig46 = px.choropleth_mapbox(df46, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig46.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig46,use_container_width=True)
        st.dataframe(df46)

    if year=='2023' and radio=='Q3' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2023 and Quarter = 3 GROUP BY State;")
        result_47 = cursor.fetchall()
        df47 = pd.DataFrame(result_47, columns=['State', 'Users']).reset_index(drop=True)
        df47.index += 1

        fig47 = px.choropleth_mapbox(df47, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig47.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig47,use_container_width=True)
        st.dataframe(df47)

    if year=='2023' and radio=='Q4' and radio_1=='Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM agg_user where Year=2023 and Quarter = 4 GROUP BY State;")
        result_48 = cursor.fetchall()
        df48 = pd.DataFrame(result_48, columns=['State', 'Users']).reset_index(drop=True)
        df48.index += 1

        fig48 = px.choropleth_mapbox(df48, 
                                geojson=geojson_1, 
                                locations='State', color='Users',
                                featureidkey="properties.ST_NM",
                                color_continuous_scale="Viridis",
                                range_color=(0, Agg_User['Users'].iloc[863]),
                                mapbox_style="open-street-map",
                                zoom=3, center = {"lat": 20.5937, "lon": 78.9629},
                                opacity=1)
        fig48.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig48,use_container_width=True)
        st.dataframe(df48)


    #States vs Top transactions

    if year=='2018' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2018 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_49 = cursor.fetchall()
        df49 = pd.DataFrame(result_49, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df49.index += 1

        fig49 = px.bar(df49, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2018", pattern_shape_sequence=["."])
        st.plotly_chart(fig49,use_container_width=True)
        st.dataframe(df49)

    if year=='2019' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2019 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_50 = cursor.fetchall()
        df50 = pd.DataFrame(result_50, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df50.index += 1

        fig50 = px.bar(df50, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2019", pattern_shape_sequence=["."])
        st.plotly_chart(fig50,use_container_width=True)
        st.dataframe(df50)


    if year=='2020' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2020 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_51 = cursor.fetchall()
        df51 = pd.DataFrame(result_51, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df51.index += 1

        fig51 = px.bar(df51, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2020", pattern_shape_sequence=["."])
        st.plotly_chart(fig51,use_container_width=True)
        st.dataframe(df51)

    if year=='2021' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2021 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_52 = cursor.fetchall()
        df52 = pd.DataFrame(result_52, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df52.index += 1

        fig52 = px.bar(df52, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2021", pattern_shape_sequence=["."])
        st.plotly_chart(fig52,use_container_width=True)
        st.dataframe(df52)

    if year=='2022' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2022 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_53 = cursor.fetchall()
        df53 = pd.DataFrame(result_53, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df53.index += 1

        fig53 = px.bar(df53, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2022", pattern_shape_sequence=["."])
        st.plotly_chart(fig53,use_container_width=True)
        st.dataframe(df53)

    if year=='2023' and radio =='Top States' and radio_1 == 'Transactions':
        cursor.execute("SELECT State, SUM(Transaction_amount) AS Transaction_amount FROM top_trans_dis where Year=2023 GROUP BY State order by Transaction_amount desc LIMIT 10;")
        result_54 = cursor.fetchall()
        df54 = pd.DataFrame(result_54, columns=['State', 'Transaction_amount']).reset_index(drop=True)
        df54.index += 1

        fig54 = px.bar(df54, x='State', y='Transaction_amount', color = 'Transaction_amount', title = "Transactions: Top 10 States in 2023", pattern_shape_sequence=["."])
        st.plotly_chart(fig54,use_container_width=True)
        st.dataframe(df54)

    #States vs Top users

    if year=='2018' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2018 GROUP BY State order by Users desc LIMIT 10;")
        result_55 = cursor.fetchall()
        df55 = pd.DataFrame(result_55, columns=['State', 'Users']).reset_index(drop=True)
        df55.index += 1

        fig55 = px.bar(df55, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2018", pattern_shape_sequence=["+"])
        st.plotly_chart(fig55,use_container_width=True)
        st.dataframe(df55)

    if year=='2019' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2019 GROUP BY State order by Users desc LIMIT 10;")
        result_56 = cursor.fetchall()
        df56 = pd.DataFrame(result_56, columns=['State', 'Users']).reset_index(drop=True)
        df56.index += 1

        fig56 = px.bar(df56, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2019", pattern_shape_sequence=["+"])
        st.plotly_chart(fig56,use_container_width=True)
        st.dataframe(df56)

    if year=='2020' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2020 GROUP BY State order by Users desc LIMIT 10;")
        result_57 = cursor.fetchall()
        df57 = pd.DataFrame(result_57, columns=['State', 'Users']).reset_index(drop=True)
        df57.index += 1

        fig57 = px.bar(df57, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2020", pattern_shape_sequence=["+"])
        st.plotly_chart(fig57,use_container_width=True)
        st.dataframe(df57)

    if year=='2021' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2021 GROUP BY State order by Users desc LIMIT 10;")
        result_58 = cursor.fetchall()
        df58 = pd.DataFrame(result_58, columns=['State', 'Users']).reset_index(drop=True)
        df58.index += 1

        fig58 = px.bar(df58, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2021", pattern_shape_sequence=["+"])
        st.plotly_chart(fig58,use_container_width=True)
        st.dataframe(df58)

    if year=='2022' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2022 GROUP BY State order by Users desc LIMIT 10;")
        result_59 = cursor.fetchall()
        df59 = pd.DataFrame(result_59, columns=['State', 'Users']).reset_index(drop=True)
        df59.index += 1

        fig59 = px.bar(df59, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2022", pattern_shape_sequence=["+"])
        st.plotly_chart(fig59,use_container_width=True)
        st.dataframe(df59)

    if year=='2023' and radio =='Top States' and radio_1 == 'Users':
        cursor.execute("SELECT State, SUM(Users) AS Users FROM top_user_dis where Year=2023 GROUP BY State order by Users desc LIMIT 10;")
        result_60 = cursor.fetchall()
        df60 = pd.DataFrame(result_60, columns=['State', 'Users']).reset_index(drop=True)
        df60.index += 1

        fig60 = px.bar(df60, x='State', y='Users', color = 'Users', title = "Users: Top 10 States in 2023", pattern_shape_sequence=["+"])
        st.plotly_chart(fig60,use_container_width=True)
        st.dataframe(df60)

with tab2:

    question_to_sql = st.selectbox('**Select your Question**',
        ('Select here',
        '1. Which state is using the PhonePe most?',
        '2. Which state is having highest transaction count?',
        '3. Which state is having highest Recharge & bill payments?',
        '4. Which state is having highest Peer-to-peer payments?',
        '5. Which state is having highest Merchant payments?',
        '6. Which state is using PhonePe for Financial services?')
    
    connect_for_question = pymysql.connect(host='localhost', user='root', password='root', db='phonepe_db')
    cursor = connect_for_question.cursor()

    #Q1
    if question_to_sql == '1. Which state is using the PhonePe most?':
        cursor.execute("SELECT top_user_dis.State, (SUM(top_user_dis.Users) + SUM(top_user_pin.Users)) AS Total_Users FROM top_user_dis JOIN top_user_pin ON top_user_dis.State = top_user_pin.State GROUP BY top_user_dis.State ORDER BY Total_Users desc LIMIT 1;")
        result_61 = cursor.fetchall()
        df61 = pd.DataFrame(result_61, columns=['State', 'Users']).reset_index(drop=True)
        df61.index += 1
        st.dataframe(df61)

    #Q2
    if question_to_sql == '2. Which state is having highest transaction count?':
        cursor.execute("SELECT top_trans_dis.State, (SUM(top_trans_dis.Transaction_count) + SUM(top_trans_pin.Transaction_count)) AS Total_transaction_count FROM top_trans_dis JOIN top_trans_pin ON top_trans_dis.State = top_trans_pin.State GROUP BY top_trans_dis.State ORDER BY Total_transaction_count desc LIMIT 1;")
        result_62 = cursor.fetchall()
        df62 = pd.DataFrame(result_62, columns=['State', 'Transaction count']).reset_index(drop=True)
        df62.index += 1
        st.dataframe(df62)

    #Q3
    if question_to_sql == '3. Which state is having highest Recharge & bill payments?':
        cursor.execute("select State, SUM(Transaction_count) AS Transaction_count from agg_trans where Transaction_type = 'Recharge & bill payments' GROUP BY State ORDER BY Transaction_count DESC LIMIT 1;")
        result_63 = cursor.fetchall()
        df63 = pd.DataFrame(result_63, columns=['State', 'Transaction count']).reset_index(drop=True)
        df63.index += 1
        st.dataframe(df63)

    #Q4
    if question_to_sql == '4. Which state is having highest Peer-to-peer payments?':
        cursor.execute("select State, SUM(Transaction_count) AS Transaction_count from agg_trans where Transaction_type = 'Peer-to-peer payments' GROUP BY State ORDER BY Transaction_count DESC LIMIT 1;")
        result_64 = cursor.fetchall()
        df64 = pd.DataFrame(result_64, columns=['State', 'Transaction count']).reset_index(drop=True)
        df64.index += 1
        st.dataframe(df64)

    #Q5
    if question_to_sql == '5. Which state is having highest Merchant payments?':
        cursor.execute("select State, SUM(Transaction_count) AS Transaction_count from agg_trans where Transaction_type = 'Merchant payments' GROUP BY State ORDER BY Transaction_count DESC LIMIT 1;")
        result_65 = cursor.fetchall()
        df65 = pd.DataFrame(result_65, columns=['State', 'Transaction count']).reset_index(drop=True)
        df65.index += 1
        st.dataframe(df65)

     #Q6
    if question_to_sql == '6. Which state is using PhonePe for Financial services?':
        cursor.execute("select State, SUM(Transaction_count) AS Transaction_count from agg_trans where Transaction_type = 'Financial Services' GROUP BY State ORDER BY Transaction_count DESC LIMIT 1;")
        result_66 = cursor.fetchall()
        df66 = pd.DataFrame(result_66, columns=['State', 'Transaction count']).reset_index(drop=True)
        df66.index += 1
        st.dataframe(df66)


connect_for_data.commit()
# SQL DB connection close
connect_for_data.close()

    ##########-----------------------------------COMPLETED--------------------------------------#########
