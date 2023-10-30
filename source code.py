#source code of main program which will be responsible for running nd analysing data


#importing all the necessary libraies 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st 
import mysql.connector as ms
import math
import warnings
from decimal import Decimal
import seaborn as sns
warnings.filterwarnings("ignore")

#asking user to eneter if he want to analysis default file or new one 


which_file=input('Do u want to analysis the default file(dumb_sql_2020). choose the answer as per question and answer in the option\ni)Y\nii)N\n')
if which_file=='Y':
    print('we are heading towords to analysis dumb_sql_2020')
    mycom=ms.connect(host='localhost',user='root',passwd='000001',database='sales')
    if mycom.is_connected():
        print('wow! we successfully connected to dumb_sql_2020')

#after successfully connected to our sql file we mare linking sql with python by making sql tables in database using pandas

        slecting_tables_1='select * from customers;'
        slecting_tables_2='select * from date;'
        slecting_tables_3='select * from markets;'
        slecting_tables_4='select * from products;'
        slecting_tables_5='select * from transactions;'
        customers=pd.read_sql(slecting_tables_1,mycom)
        date=pd.read_sql(slecting_tables_2,mycom)
        markets=pd.read_sql(slecting_tables_3,mycom)
        products=pd.read_sql(slecting_tables_4,mycom)
        transactions=pd.read_sql(slecting_tables_5,mycom)
        #successfully made 5 database of sql tables for our data analysis
        #print(transactions.info(),'\n',customers.info(),'\n',date.info(),'\n',markets.info(),'\n',products.info())
        # after doing df.info() we get to know there are data column in object type so will change it into datetime type
        if transactions['order_date'].dtypes == 'object':
            transactions['order_date']=pd.to_datetime(transactions['order_date'])
        if date['date'].dtype=='object':
            date['date']=pd.to_datetime(date['date'])
        #print(transactions,'\n',date)
        #print(transactions.isnull().sum())
        #no null value 

        transactions=transactions[transactions['profit_margin']>=0]
        transactions=transactions[transactions['profit_margin_percentage']>=0]
        transactions.loc[transactions['currency']=='USD',['sales_amount']]=transactions['sales_amount']*80
        #done with cleaning now moving towards analysis
        markets.rename(columns={'markets_code':'market_code'},inplace=True)
        #no markets_code named column in transcation df so to merge them we jhave to make it linkable
        transactions_for_market =pd.merge(transactions, markets[['market_code','markets_name']],on='market_code', how='left')
        date.rename(columns={'date':'order_date','date_yy_mmm':'date_mm'},inplace=True)
        transactions_by_date=pd.merge(transactions,date[['order_date','year','month_name']] ,on='order_date',how='left')
        
        transactions_for_market_grp_avg=(transactions_for_market.groupby('markets_name')).mean()
        
        transactions_for_market_grp_total=(transactions_for_market.groupby('markets_name')).sum()
        
        transactions_by_date_grp_avg_year=(transactions_by_date.groupby('year')).mean()

        transactions_by_date_grp_avg_month=(transactions_by_date.groupby('month_name')).mean()
        
        transactions_by_date_grp_year_total=(transactions_by_date.groupby('year')).sum()
        
        transactions_by_date_grp_avg_month_for_graph=(transactions_by_date.groupby('month_name')).sum()
        #new_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        transactions_by_date_grp_year_total_for_GRAPH=(transactions_by_date.groupby('year')).sum()
        transactions_by_date_grp_year_total.reset_index(inplace=True)
        transactions_by_date_grp_year_total.index=transactions_by_date_grp_year_total.index.astype(str)
        transactions_for_market_cus=(transactions_for_market.groupby('market_code')).size()
        customers_size=transactions_for_market.groupby('customer_code').size()

        customers_count_in_market = transactions_for_market.groupby('markets_name')['customer_code'].count()
        grouped_transactions = transactions_for_market.groupby(['markets_name', 'customer_code'])
        customer_transactions = grouped_transactions.size().reset_index(name='transactions')
        
        average_transactions_per_customer_in_market =customer_transactions.groupby('markets_name')['transactions'].mean()
        markets_with_unique_customers = transactions_for_market.groupby('markets_name')['customer_code'].nunique()
        
        total_revenue_per_market = transactions_for_market_grp_total['sales_amount']
        # Convert the pandas Series to a float before performing the division
        #normal_total_cost_spent_per_market = (total_cost_spent_per_market.apply(lambda x: x / math.pow(10, math.floor(math.log10(x)))))*10
        #print(normal_total_cost_spent_per_market)
        #****(IMP) the above answer is in the lahks
        total_profit_margin_per_market=transactions_for_market_grp_total['profit_margin']
        #print(total_profit_margin_per_market)
        def readable_number_converter(num):
            if num >= 10**6:
                return f"₹{num / 10**7:.2f}Crore"
            elif num >= 10**3:
                return f"₹{num / 10**5:.2f}Lakh"
            else:
                return str(num)

        #if case it show 0 in front of the number is because the number is too large to be represented as a float64. In Python, the float64 data type can represent numbers with approximately 15 decimal digits of precision.

        #The number ₹52.5458Crore is too large to be represented as a float64, so Python is rounding it to the nearest number that can be represented. In this case, Python is rounding it to 0.

        #To avoid this issue, we can use the `Decimal` data type from the `decimal` module, which can represent numbers with arbitrary precision.


        total_profit_margin=transactions_for_market_grp_total['profit_margin'].sum()
        
        total_revenue_per_market=transactions_for_market_grp_total['sales_amount']

        total_profit_margin=(pd.Series(total_profit_margin).apply(readable_number_converter)).to_string(index=False)
        
        total_revenue=transactions_for_market_grp_total['sales_amount'].sum()

        #The error message we are seeing is because you're trying to call the `apply()` method on a numpy float64 object, which doesn't have that method.
        #To fix this, you can convert the numpy float64 object to a pandas Series object before applying the `readable_number_converter()` function.
        total_revenue=(pd.Series(total_revenue).apply(readable_number_converter)).to_string(index=False)

        avg_revenue_per_month=transactions_by_date_grp_avg_month['sales_amount'].mean()
        # Assuming transactions_by_date_grp_avg_month is a DataFrame with a DatetimeIndex
        #transactions_by_date_grp_avg_month.index = pd.to_datetime(transactions_by_date_grp_avg_month.index)
        #transactions_by_date_grp_avg_year = transactions_by_date_grp_avg_year.assign(year=lambda x: x.index.year)
        sales_amount_2019 = transactions_by_date_grp_year_total[transactions_by_date_grp_year_total['year'] == 2019]['sales_amount'].values[0]
        sales_amount_2020 = transactions_by_date_grp_year_total[transactions_by_date_grp_year_total['year'] == 2020]['sales_amount'].values[0]
        percentage_increase = ((((sales_amount_2020 - sales_amount_2019) / sales_amount_2019) * 100).round(2))
        avg_revenue_per_month_1 = (avg_revenue_per_month).round(2)
        
        best_place=total_profit_margin_per_market.idxmax()
        
        best_month=transactions_by_date_grp_avg_month_for_graph['profit_margin'].idxmax()
        
        total_profit_margin_2019 = transactions_by_date_grp_year_total[transactions_by_date_grp_year_total['year'] == 2019]['profit_margin'].sum()
        total_profit_margin_2020 = transactions_by_date_grp_year_total[transactions_by_date_grp_year_total['year'] == 2020]['profit_margin'].sum()

        # Calculate the percentage decrease in total profit margin from 2019 to 2020
        percentage_decrease = (((total_profit_margin_2020 - total_profit_margin_2019) / total_profit_margin_2019) * 100).round(2)
        #we are taking sample size of 2020 and 2019 data only
        # Convert the percentage decrease to a readable number
        readable_percentage_decrease = readable_number_converter(percentage_decrease)

        transactions_by_date_grp_year_total_for_GRAPH.index=transactions_by_date_grp_year_total_for_GRAPH.index.astype(str)
        #avg_decrease = transactions_by_date_grp_avg_year.groupby('year')['customers'].agg(['count', 'sum']).reset_index()
        #avg_decrease['avg_decrease'] = avg_decrease['sum'] / avg_decrease['count']
        #The `.values[0]` is used to extract the value from a pandas Series. In this case, it is used to extract the sales amount for the year 2019 and 2020 from the `transactions_by_date_grp_avg_year` DataFrame.

        #Here's a step-by-step breakdown of the code:

        #1. `transactions_by_date_grp_avg_year[transactions_by_date_grp_avg_year['year'] == 2019]['sales_amount']` selects the sales amount for the year 2019.

        #2. `.values` converts the pandas Series into a numpy array.

        #3. `[0]` extracts the first element from the numpy array, which is the sales amount for the year 2019.

        #The same logic is applied for the year 2020.

        #So, `sales_amount_2019` and `sales_amount_2020` are the sales amounts for the years 2019 and 2020, respectively.

        #The `percentage_increase` is calculated by subtracting the sales amount for 2019 from the sales amount for 2020, dividing the result by the sales amount for 2019, and multiplying the result by 100 to get the percentage increase.
        own_brand_str=str('Own Brand\r')
        distribution_str=str("Distribution\r")

        # Count the number of products in each category
        own_brand_count = products.loc[products['product_type'] == own_brand_str].count()
        distribution_count = products.loc[products['product_type'] == distribution_str].count()
        # Create a dashboard with Streamlit

        print('copy thiscommand nd paste it here(vs terminal) or in cmd after defining path ')
        st. set_page_config(layout="wide")

    #css styling for better looks 
        st.markdown("""
        <style>
        .big-font {
            font-size:150px !important;
            text-decoration: underline;
            text-align: center;
            display: inline-block;  
            background-color: #212A3E;
            border: 10px solid #394867;
            padding: 2.5% 17% 2.5% 17%;
            border-radius: 18px;
            color:#F1F6F9
            
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown('<p class="big-font"<b>Sales Analysis</b></p>', unsafe_allow_html=True)


        st.write('---')
        st.container()
        st.empty()
        st.markdown(
            """
        <style>
        div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
        overflow-wrap: break-word;
        white-space: break-spaces;
        text-align: center;
        background: #012445;
        border: 2.5px solid #CCCCCC;
            padding: 5% 7% 5% 10%;
            border-radius: 7px;
            
            
            
        }
        div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div p {
        font-size: 175% !important;
        r

        </style>
        """,
            unsafe_allow_html=True,
        )


        col1, col2,col3, col4, col5 = st.columns(5,gap='large')

        col1.metric(label='Total Revenue', value=total_revenue, delta=f"{percentage_increase}%")

        col2.metric(label='Total Profit Margin',value=total_profit_margin,delta=f"{readable_percentage_decrease}%")

        col3.metric(label='Total Customers',value=(customers_size).sum())

        col4.metric(label='Best place',value=(best_place))

        col5.metric(label='Best month',value=(best_month))

        st.title('GRAPHS')
        gh1,gh2=st.columns([1,1])

        with gh1:
            st.title('Profit per Year')
            st.line_chart(transactions_by_date_grp_year_total_for_GRAPH['profit_margin'],color='#bc5090',)
        with gh2:
            st.title('Profit per month for last 4 years')
            st.text('This graph tells the sales in each month for last 4 years')
            st.line_chart(transactions_by_date_grp_avg_month_for_graph['profit_margin'],color='#ffa600')
            

        gh3,gh4=st.columns([1,1])

        with gh3:
            st.title('Avg Transactions Per Market')
            st.bar_chart(average_transactions_per_customer_in_market,color='#FF9671')

        with gh4:
            st.title('Profit Margin Per Market')
            st.bar_chart(transactions_for_market_grp_total['profit_margin'],color='#845EC2')

        st.write('---')
        pie1,pie2=st.columns([1,1])

        with pie1:
            st.title('Customers count')
            fig1, ax1 = plt.subplots()
            patches, texts, pcts = ax1.pie(customers_count_in_market,labels=customers_count_in_market.index, autopct='%1.1f%%',
            shadow=True, startangle=90,pctdistance=0.80)
            fig1.set_facecolor('black')
            ax1.axis('equal')
            for i, patch in enumerate(patches):
                texts[i].set_color(patch.get_facecolor())
            plt.setp(pcts, color='white')
            plt.setp(texts, fontweight=600)
            plt.tight_layout()
            hole = plt.Circle((0, 0), 0.65, facecolor='black')
            plt.gcf().gca().add_artist(hole)
            st.pyplot(fig1)

        with pie2:
            st.title('Product types share')
            fig2, ax2 = plt.subplots()
            patches1, texts1, pcts1 =ax2.pie([own_brand_count['product_type'],distribution_count['product_type']],labels=['Own Brand','Distribution'], autopct='%1.1f%%',
            shadow=True, startangle=90)
            fig2.set_facecolor('black')
            ax2.axis('equal')
            for i1, patch1 in enumerate(patches1):
                texts1[i1].set_color(patch1.get_facecolor())
            plt.setp(pcts1, color='white')
            plt.setp(texts1, fontweight=600)
            plt.tight_layout()
            st.pyplot(fig2)

        st.write('---')
    else:
        print('oop! we are fail to connect pls try later after sometime')
elif which_file=='N':
    print('You can updata that SQL file with same column, it will work fyn for whole life')
    