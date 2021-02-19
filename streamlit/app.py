import streamlit as st
import pandas as pd
from PIL import Image
import altair as alt

import psycopg2
import psycopg2.extras
import pandas.io.sql as sqlio


import sys
#sys.path.append("../")
import config

@st.cache
def load_data():
    connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

    sql_stock = "SELECT * FROM stock"
    sql_mention = "SELECT * FROM mention"

    stocks = sqlio.read_sql_query(sql_stock, connection)
    stocks = stocks[stocks.symbol.str.len() > 2]

    mention_df = sqlio.read_sql_query(sql_mention, connection)

    df_ = pd.merge(mention_df, stocks, how='left', left_on='stock_id', right_on='id')
    df_['date'] = df_['dt'].dt.date
    return df_

df = load_data()

aggregation_df = aggregation_df = df.groupby(['date', 'symbol']).agg({'message': 'count'}).rename(columns={'message': 'mention_count'})
aggregation_df = aggregation_df.reset_index()
aggregation_df.sort_values(by='date', inplace=True)
aggregation_df['date']= pd.to_datetime(aggregation_df['date'])


ticker_list = list(aggregation_df.symbol.unique())

for ticker in ticker_list:

    # Daily percent change
    aggregation_df.loc[aggregation_df.symbol == ticker, 'daily_pct_change'] = \
    aggregation_df.mention_count[aggregation_df.symbol == ticker].pct_change() * 100

    # 3 day percent change
    aggregation_df.loc[aggregation_df.symbol == ticker, 'three_day_pct_change'] = \
    aggregation_df.mention_count[aggregation_df.symbol == ticker].pct_change(3) * 100

    # weekly percent change
    aggregation_df.loc[aggregation_df.symbol == ticker, 'weekly_pct_change'] = \
    aggregation_df.mention_count[aggregation_df.symbol == ticker].pct_change(7 ) * 100

trending_df_1 = aggregation_df[(aggregation_df.date == aggregation_df.date.max()) &
          (aggregation_df.daily_pct_change > 0)].sort_values(by='mention_count', ascending=False)

trending_df_3 = aggregation_df[(aggregation_df.date == aggregation_df.date.max()) &
          (aggregation_df.three_day_pct_change > 0)].sort_values(by='mention_count', ascending=False)


def plot_mentions_over_time(df):
    brush = alt.selection_interval()
    mention_chart = alt.Chart(df).mark_line(point=True).encode(
                alt.X('date:T', ),
                alt.Y('mention_count:Q'),
                alt.Color('symbol:N')
    ).properties(
                width=600,
                height=300,
    ).add_selection(
                brush,

    )
    aggregation = alt.Chart(df).mark_rect().encode(
                alt.X('sum(mention_count):Q', sort='ascending'),
                alt.Y('symbol:N'),
                alt.Color('symbol:N')
    ).properties(
                width=600,
                height=180,
    ).transform_filter(
                brush
            )

    return mention_chart & aggregation


# -------------------------------- APP -------------------------------------------

st.title('Wall Street Bets')

# ================================ side bar =========================
st.sidebar.title('Ticker Selection')



selected_stock = st.sidebar.multiselect('Select Ticker', ticker_list)
#st.multiselect()

#=====================================================================


# Summary
st.markdown("The dataset starts at **{}** and ends at **{}**".format(df.dt.min(), df.dt.max()))
st.markdown("The Total number of mentions we see: **{}**".format(df.shape[0]))
st.markdown("The Total number of stocks mentioned: **{}**".format(df.stock_id.nunique()))

# Most spoken about Tickers
aggs = df.symbol.value_counts().to_frame()
st.markdown("**Most Mentioned Stocks**")
st.bar_chart(aggs.iloc[:20])


# --------------------------------------------------------------------------


with st.beta_expander('Stock Mention Trends'):
    st.altair_chart(plot_mentions_over_time(aggregation_df[aggregation_df.symbol.isin(list(aggs.index[:10]))]))

with st.beta_expander('Stock Mention Daily Percent Rises'):
    st.table(trending_df_1[['symbol', 'mention_count', 'daily_pct_change']])
    st.altair_chart(plot_mentions_over_time(aggregation_df[aggregation_df.symbol.isin(list(trending_df_1.symbol))]))

with st.beta_expander('Stock Mention 3 day Percent Rises'):
    st.table(trending_df_3[['symbol', 'mention_count', 'three_day_pct_change']])
    st.altair_chart(plot_mentions_over_time(aggregation_df[aggregation_df.symbol.isin(list(trending_df_3.symbol))]))

with st.beta_expander('Ticker Look Up'):
    try:
        st.markdown("Selected Stock: {}".format(selected_stock[0]))
        st.table(df[['name', 'sector', 'sector', 'industry', 'exchange']][df.symbol == selected_stock[0]].iloc[0])
        st.markdown("Most Recent Message")
        st.table(df[['dt','message']][df.symbol == selected_stock[0]].iloc[:10].sort_values(by='dt', ascending=False))
    except:
        pass
