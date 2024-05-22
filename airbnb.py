import json
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as pe
import pandas as pd
import mysql.connector
import sqlite3

#Data Extract
A = open('sample_airbnb.json','r')
airb_data = json.load(A)

#sql connection:
mydb = mysql.connector.connect(host="localhost",user="root",password="",database="phonepe")
#print(mydb)
mycursor = mydb.cursor(buffered=True)

#Correlation Heatmap Visualization
def corr_info():
    c_data = {'Listing_id':[], 'Host_id':[],'Longitude':[],'Latitude':[],'Price':[],'Number_of_review':[],
            'Minimum_nights':[],'Availability_365':[],'Host_response_rate':[]}

    for i in airb_data:
        ids = i['_id']
        c_data['Listing_id'].append(ids)
        hostid = i['host']['host_id']
        c_data['Host_id'].append(hostid)
        longitude = i['address']['location']['coordinates'][0]
        c_data['Longitude'].append(longitude)
        latitude= i['address']['location']['coordinates'][1]
        c_data['Latitude'].append(latitude)
        price = i['price']
        c_data['Price'].append(price)
        review = i['number_of_reviews']
        c_data['Number_of_review'].append(review)
        min = i['minimum_nights']
        c_data['Minimum_nights'].append(min)
        avail365 = i['availability']['availability_365']
        c_data['Availability_365'].append(avail365)
        hostrate = i['host'].get('host_response_rate')
        c_data['Host_response_rate'].append(hostrate)
        
    corr_data=pd.DataFrame(c_data) 

    corr_data['Price']=corr_data['Price'].astype(dtype='int64')
    corr_data['Host_response_rate']=corr_data['Host_response_rate'].fillna(0)
    corr_data['Host_response_rate']=corr_data['Host_response_rate'].astype(dtype='int64')

    return corr_data

corr_data=corr_info()

#list of country:
def country_list():
    mycursor.execute("SELECT DISTINCT Country FROM airb.hotel_details ORDER BY Country;")
    s = mycursor.fetchall()
    cou = [i[0] for i in s]
    return cou

    list_coun=pd.DataFrame(cou)
    
country=country_list()

#list of property type:
def property_type():
    mycursor.execute("SELECT DISTINCT Property_type FROM airb.room_details;")
    s1 = mycursor.fetchall()
    pro = [i[0] for i in s1]
    return pro

property=property_type()

#list of room type:
def room_type():
    mycursor.execute("SELECT DISTINCT Room_type FROM airb.room_details;")
    s2 = mycursor.fetchall()
    room = [i[0] for i in s2]
    return room

room=room_type()

#MAXIMUM:
def price_max():
    mycursor.execute("SELECT MAX(Price) AS Max_price FROM airb.hotel_details;")
    s3 = mycursor.fetchall()
    max = [i[0] for i in s3]
    return max

maximum=price_max()

#MINIMUM:
def price_min():
    mycursor.execute("SELECT MIN(Price) AS Min_price FROM airb.hotel_details;")
    s4 = mycursor.fetchall()
    min = [i[0] for i in s4]
    return min

minimum=price_min()

#cleaning fee:
def cleaning():
    mycursor.execute('''SELECT DISTINCT Cleaning_fee, COUNT(Cleaning_fee) AS Count
                        FROM airb.hotel_details
                        WHERE cleaning_fee != 'Not Specified'
                        GROUP BY cleaning_fee
                        ORDER BY Count DESC
                        LIMIT 20;''')
    a=mycursor.fetchall()
    b=pd.DataFrame(a,columns=("Cleaning_fee","Count"))
    return b

cl_fee=cleaning()

#Average price for apartment/hotel name:
def avg_p():
    mycursor.execute('''SELECT Name AS Apartment_hotel_name, AVG(Price) AS Avg_pr FROM airb.hotel_details 
                        WHERE Country = %s GROUP BY Apartment_hotel_name ORDER BY Avg_pr DESC''', (country,))
    a2=mycursor.fetchall()
    b2=pd.DataFrame(a2,columns=("Apartment_hotel_name","Avg_pr"))
    return b2

#avg_price=avg_p()

#Top 10 Property Types:
def top_prop():
    mycursor.execute('SELECT Property_type, COUNT(*) AS Count FROM airb.room_details GROUP BY Property_type ORDER BY Count DESC LIMIT 10;')
    a3=mycursor.fetchall()
    b3=pd.DataFrame(a3,columns=("Property_type","Count"))
    return b3

#t_pro=top_prop()

#Total count of Room type:
def room_ty():
    mycursor.execute('SELECT Room_type, COUNT(*) AS Count FROM airb.room_details GROUP BY Room_type ORDER BY Count;')
    a4=mycursor.fetchall()
    b4=pd.DataFrame(a4,columns=("Room_type","Count"))
    return b4

#r_ty=room_ty()

#Top 10 Hosts with Highest number of Listings:
def host_list():
    mycursor.execute('SELECT Host_name, COUNT(*) AS Total_count FROM airb.host_details GROUP BY Host_name ORDER BY Total_count DESC LIMIT 20;')
    a5=mycursor.fetchall()
    b5=pd.DataFrame(a5,columns=("Host_name","Total_count"))
    return b5

#h_li=host_list()

#Total Review for apartment/hotel name:
def rev_c():
    mycursor.execute('''SELECT rd.Number_of_review as No_of_reviews, hd.Country, hd.Name FROM airb.review_details rd 
                        JOIN airb.hotel_details hd ON rd.id = hd.id 
                        where Country = %s GROUP BY Name 
                        ORDER BY No_of_reviews DESC LIMIT 20;''',(country,))
    a6=mycursor.fetchall()
    b6=pd.DataFrame(a6,columns=("No_of_reviews","Country","Name"))
    return b6

#review_c=rev_c()

#Countries ranked by their average booking price:
def avg_p_c():
    mycursor.execute('SELECT Country, AVG(Price) AS Average_price FROM airb.hotel_details GROUP BY Country;')
    a7=mycursor.fetchall()
    b7=pd.DataFrame(a7,columns=("Country","Average_price"))
    return b7

#average_p_c=avg_p_c()

#Average Availability_365:
def avg_ro_c():
    mycursor.execute('''SELECT AVG(Availability_365) AS Availability365, hd.Country FROM airb.room_details ro 
                    JOIN airb.hotel_details hd ON ro.id = hd.id GROUP BY Country ORDER BY Availability365 DESC;''')
    a8=mycursor.fetchall()
    b8=pd.DataFrame(a8,columns=("Availability365","Country"))
    return b8

#average_ro_c=avg_ro_c()

#Average Availability_90:
def avg_ro_c3():
    mycursor.execute('''SELECT AVG(Availability_90) AS Availability90, hd.Country FROM airb.room_details ro 
    JOIN airb.hotel_details hd ON ro.id = hd.id GROUP BY Country ORDER BY Availability90 DESC;''')
    a11=mycursor.fetchall()
    b11=pd.DataFrame(a11,columns=("Availability90","Country"))
    return b11

#average_ro_c3=avg_ro_c3()

#Average Availability_60:
def avg_ro_c2():
    mycursor.execute('''SELECT AVG(Availability_60) AS Availability60, hd.Country FROM airb.room_details ro 
    JOIN airb.hotel_details hd ON ro.id = hd.id GROUP BY Country ORDER BY Availability60 DESC;''')
    a10=mycursor.fetchall()
    b10=pd.DataFrame(a10,columns=("Availability60","Country"))
    return b10

#average_ro_c2=avg_ro_c2()

#Average Availability_30:
def avg_ro_c1():
    mycursor.execute('''SELECT AVG(Availability_30) AS Availability30, hd.Country FROM airb.room_details ro 
    JOIN airb.hotel_details hd ON ro.id = hd.id GROUP BY Country ORDER BY Availability30 DESC;''')
    a9=mycursor.fetchall()
    b9=pd.DataFrame(a9,columns=("Availability30","Country"))
    return b9

#average_ro_c1=avg_ro_c1()

#top 10 Property count in Each Neighborhood Group 
def top_subu():
    mycursor.execute('''SELECT Suburb AS Neighborhood_Groups, COUNT(*) AS Listing_Counts FROM airb.hotel_details 
                        GROUP BY Suburb ORDER BY Listing_Counts DESC LIMIT 10;''')
    a16=mycursor.fetchall()
    b16=pd.DataFrame(a16,columns=("Neighborhood_Groups","Listing_Counts"))
    return b16

#top_neigh=top_subu()

#Average Price Of Each Neighborhood Groups:
def avg_subu():
    mycursor.execute('''SELECT Suburb AS Neighborhood_Groups, AVG(price) AS Avg_price FROM airb.hotel_details GROUP BY Suburb;''')
    a17=mycursor.fetchall()
    b17=pd.DataFrame(a17,columns=("Neighborhood_Groups","Avg_price"))
    return b17

#avg_s=avg_subu()

#Counts by Minimum Nights:
def min_co():
    mycursor.execute('SELECT Minimum_nights,COUNT(*) AS Total_count FROM airb.room_details GROUP BY Minimum_nights;')
    a18=mycursor.fetchall()
    b18=pd.DataFrame(a18,columns=("Minimum_nights","Total_count"))
    return b18

#min_cou=min_co()

#Average Reviews by Neighborhood groups:
def avg_r():
    mycursor.execute('''SELECT hd.Suburb AS Neighborhood_Groups, AVG(Number_of_review) AS Total_reviews FROM airb.review_details rd 
                    JOIN airb.hotel_details hd ON rd.id = hd.id GROUP BY Neighborhood_Groups;''')
    a19=mycursor.fetchall()
    b19=pd.DataFrame(a19,columns=("Neighborhood_Groups","Total_reviews"))
    return b19

#review_r=avg_r()

#Guest included:
def guest():
    mycursor.execute('''SELECT hd.Name AS Apartment_Hotel_Name, ro.Room_type, ro.Guest_included AS Total_guest FROM airb.room_details ro JOIN airb.hotel_details hd ON ro.id = hd.id GROUP BY Guest_included ORDER BY Room_type;''')
    a14=mycursor.fetchall()
    b14=pd.DataFrame(a14,columns=("Apartment_Hotel_Name","Room_type"," Total_guest"))
    return b14

#guest_included=guest()

#top 25 apartment/hotel name with their prices:
def top_apartment():
    mycursor.execute('SELECT Name AS Apartment_hotel, Price FROM airb.hotel_details ORDER BY Price DESC LIMIT 25;')
    a1=mycursor.fetchall()
    b1=pd.DataFrame(a1,columns=("Apartment_hotel","Price"))
    return b1

#t_apart=top_apartment()

#max no of night stays in all three room type
def ro_mx():
    mycursor.execute('''SELECT Room_type, SUM(Maximum_nights) as Max_night_stay FROM airb.room_details GROUP BY Room_type;''')
    a12=mycursor.fetchall()
    b12=pd.DataFrame(a12,columns=("Room_type","Max_night_stay"))
    return b12

#room_mx_st=ro_mx()

#min no of night stays in all three room type
def ro_mi():
    mycursor.execute('''SELECT Room_type, SUM(Minimum_nights) as Min_night_stay FROM airb.room_details GROUP BY Room_type;''')
    a13=mycursor.fetchall()
    b13=pd.DataFrame(a13,columns=("Room_type","Min_night_stay"))
    return b13

#room_mi_st=ro_mi()

#Streamlit part:
st.set_page_config(
    page_title="AirBnb Analysis",
    page_icon="🏨",
    layout="wide"
)

#sidebar for streamlit:
with st.sidebar:
    select = option_menu("Main Menu", ["Home", "About Airbnb", "Data Visualization", "Exploration"],
                        icons=["house", "gear", "tools", "bar-chart"],
                        styles={"nav-link": {"font": "sans serif", "font-size": "20px", "text-align": "centre"},
                        "nav-link-selected": {"font": "sans serif", "background-color": "#e5b252"},
                        "icon": {"font-size": "20px"}})


if select == "Home":
    st.markdown("# :orange[Airbnb Analysis]")
    st.markdown('<div style="height: 50px;"></div>', unsafe_allow_html=True)
    st.markdown("### :green[Technologies :] Python, Pandas, Plotly, Streamlit, Python scripting, "
                "Data Preprocessing, Visualization")
    st.markdown("### :green[Domain :] Travel Industry, Property Management and Tourism")
    st.markdown("### :green[Project Type :] Exploratory Data Analysis (EDA)")
    st.markdown("### :green[The purpose of the analysis :] To understanding the factors that influence Airbnb prices or identifying patterns of all variables and Our analysis provides useful information for travelers and hosts in the city and also provides some best insights for Airbnb business.")
    

elif select == "About Airbnb":
    st.title(":black[Airbnb Analysis:]")
    st.write(" ")
    st.write(" ")
    st.image('airbnb.jpg')
    st.write(" ")
    st.write(" ")
    st.write("#### Airbnb is an online marketplace that connects people who want to rent out their property with people who are looking for accommodations, typically for short stays. ")
    st.write("#### Airbnb began in 2008 when two designers who had space to share hosted three travelers looking for a place to stay. Now, millions of Hosts and guests have created free Airbnb accounts to enjoy each other's unique view of the world. ")
    st.write("#### Airbnb offers hosts a relatively easy way to earn some income from their property.")

elif select == "Data Visualization":
    st.markdown("# :orange[Data Analysis and Visualization]")
    st.sidebar.header("Visualization Options:")

    #Analysis of Numerical Data in the DataFrame
    st.sidebar.subheader("Analysis of Numerical Data in the DataFrame")
    if st.sidebar.checkbox("Display Statistics"):
        st.markdown('<h3 style="color:#5DBB63">Various Stats of the Numerical Data Columns of the DataFrame</h3>',
                    unsafe_allow_html=True)
        st.write(corr_data.describe())


    #Display Histogram of Selected Column
    st.sidebar.subheader("Target Analysis")
    if st.sidebar.checkbox("Display Histogram of Selected Column"):
        all_columns = st.sidebar.multiselect("Select a column :", corr_data.columns)
        for column0 in all_columns:
            st.markdown(f'<h3 style="color:#5DBB63">Histogram of {column0}</h3>',
                        unsafe_allow_html=True)
            fig = pe.histogram(corr_data, x=column0, color_discrete_sequence=['#E1E6E1'])
            st.plotly_chart(fig)


    #Display Histogram for Numerical Columns Only
    st.sidebar.subheader("Distribution of Numerical Columns")
    if st.sidebar.checkbox("Display Histogram for Numerical Columns Only"):
        numerical_columns = corr_data.select_dtypes(exclude='object').columns
        selected_numerical_columns = st.sidebar.multiselect("Select numerical columns for the histogram plot :",
                                                            numerical_columns)
        for column1 in selected_numerical_columns:
            st.markdown(f'<h3 style="color:#5DBB63">Histogram for {column1}</h3>',
                        unsafe_allow_html=True)
            fig = pe.histogram(corr_data, x=column1, color_discrete_sequence=['#E1E6E1'])
            st.plotly_chart(fig)

    #Display Histogram for Categorical Columns
    st.sidebar.subheader("Count Plots of Categorical Columns")
    if st.sidebar.checkbox("Display Histogram for Categorical Columns"):
        categorical_columns = corr_data.select_dtypes(include='object').columns
        selected_categorical_columns = st.sidebar.multiselect("Select categorical columns for count plots:",
                                                            categorical_columns)
        for column2 in selected_categorical_columns:
            st.markdown(f'<h3 style="color:#5DBB63">Histogram for {column2}</h3>',
                        unsafe_allow_html=True)
            fig = pe.histogram(corr_data, x=column2, color_discrete_sequence=['#E1E6E1'])
            st.plotly_chart(fig)

    #Display Box Plots
    st.sidebar.subheader("Box Plots")
    if st.sidebar.checkbox("Display Box Plots"):
        numerical_columns = corr_data.select_dtypes(exclude='object').columns
        selected_numerical_columns = st.sidebar.multiselect("Select numerical columns for box plots:",
                                                            numerical_columns)
        for column3 in selected_numerical_columns:
            st.markdown(f'<h3 style="color:#5DBB63">Box Plot of {column3}</h3>',
                        unsafe_allow_html=True)
            fig = pe.box(corr_data, y=column3)
            st.plotly_chart(fig)

    #Display Outlier Counts
    st.sidebar.subheader("Outlier Analysis")
    if st.sidebar.checkbox("Display Outlier Counts"):
        st.markdown(f'<h3 style="color:#5DBB63">Outlier Analysis</h3>',
                    unsafe_allow_html=True)
        outlier_df = corr_data.select_dtypes(exclude='object').apply(lambda x: sum((x - x.mean()) > 2 * x.std())).reset_index(
            name="outliers")
        st.write(outlier_df)

#Exploration Section:
if select == "Exploration":
    st.markdown("# :orange[Data Exploration]")
    tab1, tab2 = st.tabs(["Insights","Overall Airbnb Analysis"])

    with tab1:

        st.title(":blue[Insights]")
    
        Query = ['Select Your Query',
                "Average price for apartment/hotel name",
                "Top 10 Property Types",
                "Total count of Room type",
                "Top 10 Hosts with Highest number of Listings",
                "Total Review for apartment/hotel name",
                "Countries ranked by their average booking price",
                "Average availability 365 days,90 days,60 days & 30 days",
                "Top 10 Property count in Each Neighborhood Groups",
                "Average Price Of Each Neighborhood Groups",
                "Counts by Minimum Nights",
                "Average Reviews by Neighborhood groups"]
        
        Selected_Query = st.selectbox(' ',options = Query)

        if Selected_Query == "Average price for apartment/hotel name":

            country = st.selectbox("Select a country:", ["Australia", "Brazil", "Canada", "China", "Hong Kong", "Portugal", "Spain", "Turkey", "United States"], index=None)

            if country != None:
                avg_price=avg_p()

                #bar chart for average price for apartment/hotel name
                fig_a= pe.bar(avg_price, x="Apartment_hotel_name", y="Avg_pr", title=f'AVERAGE PRICE FOR APARTMENT/HOTEL',color_discrete_sequence=pe.colors.sequential.ice, height= 850,width= 650)
                st.plotly_chart(fig_a)
                #dataframe
                st.dataframe(avg_price)

        if Selected_Query == "Top 10 Property Types":

            t_pro=top_prop()

            fig_a1 = pe.bar(t_pro, title='Top 10 Property Types', x='Property_type', y='Count', color='Property_type',
                            color_discrete_sequence=pe.colors.sequential.ice, height= 700,width= 650)
            st.plotly_chart(fig_a1)

            st.dataframe(t_pro)

        
        if Selected_Query == "Total count of Room type":

            r_ty=room_ty()

            figa2 = pe.bar(r_ty, x="Room_type", y="Count", title= "Total count of Room Types",color='Room_type',color_discrete_sequence=pe.colors.sequential.Aggrnyl_r,height= 600,width= 500)

            st.plotly_chart(figa2)

            st.dataframe(r_ty)


        if Selected_Query == "Top 10 Hosts with Highest number of Listings":

            h_li=host_list()

            figa3 = pe.pie(h_li, values='Total_count', names='Host_name', color_discrete_sequence= pe.colors.sequential.tempo, title='Top 10 Hosts with Highest number of Listings',hole= 0.5)

            st.plotly_chart(figa3)

            st.dataframe(h_li)

        
        if Selected_Query == "Total Review for apartment/hotel name":

            country = st.selectbox("Select a country:", ["Australia", "Brazil", "Canada", "China", "Hong Kong", "Portugal", "Spain", "Turkey", "United States"], index=None)

            if country != None:

                review_c=rev_c()

                figa4 = pe.bar(review_c, x="Name", y="No_of_reviews", title=f'No_of_reviews for Apartments/Hotels',color='No_of_reviews',
                            color_discrete_sequence=pe.colors.sequential.Aggrnyl_r,labels={'Name': 'Hotel Name', 'Number_of_reviews': 'No of Reviews'},height= 900)
                
                st.plotly_chart(figa4)

                st.dataframe(review_c)

        if Selected_Query == "Countries ranked by their average booking price":

            average_p_c=avg_p_c()

            fig = pe.scatter_geo(average_p_c, locations='Country',locationmode='country names', color='Average_price',
                                color_continuous_scale='Plasma', hover_name='Country',title="Average_price for country ", height=700)
            st.plotly_chart(fig)

            st.dataframe(average_p_c)

        if Selected_Query == "Average availability 365 days,90 days,60 days & 30 days":

            average_ro_c=avg_ro_c()
            
            fig1 = pe.scatter_geo(average_ro_c, locations='Country',locationmode='country names', color='Availability365',color_continuous_scale='Plasma', 
                                hover_name='Country', title="Average Availability 365 ", height=700)
            
            st.plotly_chart(fig1)

            st.dataframe(average_ro_c)

            average_ro_c3=avg_ro_c3()

            fig4 = pe.scatter_geo(average_ro_c3, locations='Country',locationmode='country names', color='Availability90',color_continuous_scale='agsunset', 
                                hover_name='Country', title="Average Availability 90 ",height=700)

            st.plotly_chart(fig4)

            st.dataframe(average_ro_c3)

            average_ro_c2=avg_ro_c2()

            fig3 = pe.scatter_geo(average_ro_c2, locations='Country',locationmode='country names', color='Availability60',color_continuous_scale='Plasma',
                                hover_name='Country', title="Average Availability 60 ", height=700)
            
            st.plotly_chart(fig3)

            st.dataframe(average_ro_c2)

            average_ro_c1=avg_ro_c1()

            fig2 = pe.scatter_geo(average_ro_c1, locations='Country',locationmode='country names', color='Availability30',color_continuous_scale='Plasma', 
                                hover_name='Country', title="Average Availability 30 ", height=700)
            
            st.plotly_chart(fig2)

            st.dataframe(average_ro_c1)


        if Selected_Query == "Top 10 Property count in Each Neighborhood Groups":

            top_neigh=top_subu()

            figa5 = pe.bar(top_neigh, x="Neighborhood_Groups", y="Listing_Counts", title= "Top 10 Property count og Neighborhood Group",color='Neighborhood_Groups',
                        color_discrete_sequence=pe.colors.sequential.Aggrnyl_r,height= 600,width= 500)
            
            st.plotly_chart(figa5)

            st.dataframe(top_neigh)

        if Selected_Query == "Average Price Of Each Neighborhood Groups":

            avg_s=avg_subu()

            figa6 = pe.sunburst(avg_s, path=['Avg_price', 'Neighborhood_Groups'], values='Avg_price',
                    title=f'Average Price Of Each Neighborhood Groups', color_discrete_sequence=pe.colors.qualitative.Pastel,
                    labels={'HotelName': 'Hotel Name', 'Average_price': 'Average Price'}, height=600)
            
            st.plotly_chart(figa6)

            st.dataframe(avg_s)

        if Selected_Query == "Counts by Minimum Nights":

            min_cou=min_co()

            figa8 = pe.scatter(min_cou, x='Minimum_nights', y='Total_count', 
                    title='Total count of Minimum night stay',
                    labels={'Minimum_nights': 'Night stay', 'Total_count': 'Total count'})
            
            st.plotly_chart(figa8)

            st.dataframe(min_cou)


        if Selected_Query == "Average Reviews by Neighborhood groups":

            review_r=avg_r()

            # Create a scatter plot
            figa7 = pe.scatter(review_r, x='Neighborhood_Groups', y='Total_reviews', title='Average Total Reviews per Neighborhood',
                                labels={'Neighborhood_Groups': 'Neighborhood Groups', 'Total_reviews': 'Total Reviews'})

            # Show the plot
            st.plotly_chart(figa7)

            st.dataframe(review_r)


    with tab2:

        st.title(":blue[Guest included ]")

        guest_included=guest()

        st.dataframe(guest_included)

        st.title(":blue[Cleaning fee]")

        cl_fee=cleaning()

        st.dataframe(cl_fee)

        st.title(":blue[Top 25 apartment with their prices]")

        t_apart=top_apartment()

        st.dataframe(t_apart)

        st.title(":blue[Max no of night stays in all three room type]")

        room_mx_st=ro_mx()

        st.dataframe(room_mx_st)

        st.title(":blue[Min no of night stays in all three room type]")

        room_mi_st=ro_mi()

        st.dataframe(room_mi_st)






