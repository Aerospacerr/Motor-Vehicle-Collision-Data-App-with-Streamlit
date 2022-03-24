import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import plotly.express as px


# DATA_URL = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv"

st.set_page_config(page_title="Motor Vehicle Collisions in NYC", page_icon='🚓',
                    layout='centered', initial_sidebar_state='collapsed')
st.title("Motor Vehicle Collisions in NYC")
st.markdown("This application is a Streamlit dashboard that can be used to analyze Motor Vehicle Collisions data from New York City.")

@st.cache(persist=True)
def load_data():
    data = pd.read_parquet('crashes.parquet', engine='pyarrow')
    # drop rows with no lat/long values:
    data.dropna(subset=['latitude', 'longitude'], inplace=True)
    return data

data = load_data()

st.header("Where are the most people injured in NYC")
max_injured_people = int(data['number_of_persons_injured'].max())
injured_people = st.slider("Number of persons injured in vehicle collisions", 0, max_injured_people)
st.map(data=data.query("number_of_persons_injured >= @injured_people")[["latitude", "longitude"]].dropna(how="any"))

st.header("How many collisions occur during a given time of day?")
hour = st.slider("Hour to look at", 0, 23)
filtered_by_hour = data[data['timestamp'].dt.hour == hour]

st.markdown("Vehicle collisions between %i:00 and %i:00" % (hour, (hour+1) % 24))
midpoint = (filtered_by_hour['latitude'].median(), filtered_by_hour['longitude'].median())
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
# filtered = data[
#     (data['timestamp'].dt.hour >= hour) & (
#         data['timestamp'].dt.hour <= (hour+1))
# ]

hist = np.histogram(filtered_by_hour['timestamp'].dt.minute, bins=60, range=(0, 60))[0]
chart_data = pd.DataFrame({'minute': range(60), 'crashes': hist})
fig = px.bar(chart_data, x='minute', y='crashes', hover_data=['minute', 'crashes'], height=400)
st.write(fig)

st.header("Top 5 dangerous streets by affected type")
select = st.selectbox('Affected type of people', ['Pedestrians', 'Cyclists', 'Motorists'])

if select == 'Pedestrians':
    st.write(data.query("number_of_pedestrians_injured >= 1")[["on_street_name", "number_of_pedestrians_injured"]].sort_values(
        by=['number_of_pedestrians_injured'], ascending=False).dropna(how='any')[:5])
elif select == 'Cyclist':
    st.write(data.query("number_of_cyclist_injured >= 1")[["on_street_name", "number_of_cyclist_injured"]].sort_values(
        by=['number_of_cyclist_injured'], ascending=False).dropna(how='any')[:5])
else:
    st.write(data.query("number_of_motorist_injured >= 1")[["on_street_name", "number_of_motorist_injured"]].sort_values(
        by=['number_of_motorist_injured'], ascending=False).dropna(how='any')[:5])

if st.checkbox("Show Raw Data", False):
    st.subheader('Raw Data')
    st.write(data.head(100))  # limit to first 100 rows
