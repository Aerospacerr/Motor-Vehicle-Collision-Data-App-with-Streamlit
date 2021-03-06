import numpy as np
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st


st.set_page_config(page_title="Motor Vehicle Collisions in NYC", page_icon='🚓',
                    layout='centered', initial_sidebar_state='collapsed')
st.title("Motor Vehicle Collisions in NYC")
st.markdown("This application is a Streamlit dashboard that can be used to analyze Motor Vehicle Collisions data from New York City.")

@st.cache(show_spinner=False)
def load_data():
    data = pd.read_parquet('crashes.parquet', engine='pyarrow')
    # drop rows with no lat/long values:
    data.dropna(subset=['latitude', 'longitude'], inplace=True)
    # drop rows outside bounding box of NYC:
    data = data[(data['latitude'] > 40.0) & (data['latitude'] < 42.0) &
                (data['longitude'] > -76.0) & (data['longitude'] < -70.0)]
    return data

@st.cache(show_spinner=False)
def query_data_by_persons_injured(data, injured_people):
    return data.query(f'number_of_persons_injured >= {injured_people}')[["latitude", "longitude"]].dropna(how="any")

@st.cache(show_spinner=False)
def filter_data_by_hour(data, hour):
    return data[data['timestamp'].dt.hour == hour]

@st.cache(show_spinner=False)
def filter_data_by_type_of_people(data, type_of_people, amount=10):
    return data[(data[type_of_people] > 0)][['on_street_name', 'off_street_name', type_of_people]].sort_values(
        by=[type_of_people], ascending=False).dropna(thresh=2).fillna('')[:amount]

@st.cache(show_spinner=False)
def filter_data_by_year(data, year):
    return data[data['timestamp'].dt.year == year]

@st.cache(show_spinner=False)
def get_all_contributing_factors():
    contrib_cols = [col for col in data.columns if col.startswith('contributing_factor')]
    contrib_sum = data[contrib_cols].apply(pd.Series.value_counts).fillna(0).sum(axis=1).sort_values(ascending=False).astype(int).to_frame()
    contrib_sum.index.rename('Contributing Factors', inplace=True)
    contrib_sum.columns = ['Frequency of mention']
    return contrib_sum

with st.spinner("Loading data..."):
    data = load_data()

#select_year = st.selectbox('Please Select The Year',
#            ['2020', '2021', '2022'])
#data = filter_data_by_year(data, select_year)

st.header("Where are the most people injured in NYC")
max_injured_people = int(data['number_of_persons_injured'].max())
injured_people = st.slider("Number of persons injured in vehicle collisions", 0, max_injured_people)
data_by_persons_injured = query_data_by_persons_injured(data, injured_people)
st.map(data=data_by_persons_injured)

st.header("How many collisions occur during a given time of day?")
hour = st.slider("Hour to look at", 0, 23)
filtered_by_hour = filter_data_by_hour(data, hour)

st.markdown("Vehicle collisions between %i:00 and %i:00" % (hour, (hour+1) % 24))
midpoint = (float(filtered_by_hour['latitude'].median()), float(filtered_by_hour['longitude'].median()))
st.write(pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state={
        "latitude": midpoint[0],
        "longitude": midpoint[1],
        "zoom": 11,
        "pitch": 50,
    },
    layers=[
        pdk.Layer(
            "HexagonLayer",
            data=filtered_by_hour[['timestamp', 'latitude', 'longitude']],
            get_position=['longitude', 'latitude'],
            radius=100,
            extruded=True,
            pickable=True,
            elevation_scale=4,
            elevation_range=[0, 1000],
        ),
    ],
))

st.subheader("Breakdown by minute between %i:00 and %i:00" % (hour, (hour+1) % 24))

hist = np.histogram(filtered_by_hour['timestamp'].dt.minute, bins=60, range=(0, 60))[0]
chart_data = pd.DataFrame({'minute': range(60), 'crashes': hist})
fig = px.bar(chart_data, x='minute', y='crashes', hover_data=['minute', 'crashes'], height=400)
st.write(fig)

st.header("Top 10 dangerous streets by affected type")
select = st.selectbox('Affected type of people injured or killed',
            ['Persons Injured', 'Persons Killed', 'Pedestrians Injured', 'Pedestrians Killed',
            'Cyclist Injured', 'Cyclist Killed', 'Motorist Injured', 'Motorist Killed',])

query_persons_string = f'number_of_{select.lower().split()[0]}_{select.lower().split()[1]}'
st.table(filter_data_by_type_of_people(data, query_persons_string))

st.subheader("Contributing Factors for accidents")
st.table(get_all_contributing_factors())

if st.checkbox("Show Raw Data", False):
    st.subheader('Raw Data')
    st.write(data.head(100).astype(pd.StringDtype()).fillna(''))  # limit to first 100 rows
