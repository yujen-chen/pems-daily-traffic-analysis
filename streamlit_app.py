import streamlit as st
import boto3
import pandas as pd
import numpy as np
import plotly.express as px
import os
import certifi

# load data from R2

# R2_ENDPOINT_URL = st.secrets["cloudflare-keys"]["R2_ENDPOINT_URL"]
# R2_ACCESS_KEY = st.secrets["cloudflare-keys"]["R2_ACCESS_KEY"]
# R2_SECRET_KEY = st.secrets["cloudflare-keys"]["R2_SECRET_KEY"]
# R2_REGION = st.secrets["cloudflare-keys"]["R2_REGION"]


# @st.cache_data
# def load_data_r2():
#     file_path = ".tmp/d12_station_MLHV_5min_202310.parquet"

#     if not os.path.exists(".tmp"):
#         os.makedirs(".tmp")

#     # check if the file already exists
#     if not os.path.exists(file_path):
#         s3_client = boto3.client(
#             service_name="s3",
#             endpoint_url=R2_ENDPOINT_URL,
#             aws_access_key_id=R2_ACCESS_KEY,
#             aws_secret_access_key=R2_SECRET_KEY,
#             region_name=R2_REGION,
#             verify=certifi.where(),
#         )
#         s3_client.download_file(
#             Bucket="pems-data",
#             Key="d12_station_MLHV_5min_202310.parquet",
#             Filename=".tmp/d12_station_MLHV_5min_202310.parquet",
#         )

#     # read data
#     df = pd.read_parquet(file_path)
#     return df


# read file from uploaded file
# File uploader
uploaded_file = st.file_uploader("Upload a parquet file", type=["parquet"])


def extract_dates_from_filename(filename):
    """Extract start and end dates from the filename."""
    # Assuming the filename format is "d12_station_MLHV_5min_20231001_20231010.parquet"
    base_name = os.path.basename(filename)
    date_part = base_name.split("_")[-1].replace(".parquet", "")
    start_date_str, end_date_str = date_part.split("_")

    # Convert to datetime.date objects
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d").date()
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d").date()

    return start_date, end_date


@st.cache_data
def load_data(file):
    if file is not None:
        # Read the uploaded parquet file
        df = pd.read_parquet(file)
        return df
    else:
        st.warning("Please upload a parquet file.")
        return None


@st.cache_data
def load_route_data(df, route, direction, lane_type):
    filter_condition = (
        (df["fwy_num"] == route)
        & (df["direction"] == direction)
        & (df["lane_type"] == lane_type)
    )

    route_df = df[filter_condition]

    return route_df


@st.cache_data
def date_df(df, selected_date):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    date_df = df[df["timestamp"].dt.date == selected_date]
    return date_df


@st.cache_data
def route_hourly_flow(df):
    df["hour"] = df["timestamp"].dt.hour
    df_hourly_flow = df.pivot_table(
        values="total_flow", index="hour", columns="absPM", aggfunc="sum"
    ).reset_index()
    df_hourly_flow = df_hourly_flow.set_index("hour")
    return df_hourly_flow


if uploaded_file is not None:

    # extract start and end dates
    start_date, end_date = extract_dates_from_filename(uploaded_file.name)

    # load the file from uploaded file
    raw_df = load_data(uploaded_file)

    if raw_df is not None:
        st.warning("File uploaded.")
        ## ---sidebar--- ##
        st.sidebar.header("Select Route and Direction:")

        # select route
        route = st.sidebar.selectbox(
            "Select Route:",
            (sorted(raw_df["fwy_num"].unique())),
        )
        filtered_directions = raw_df[raw_df["fwy_num"] == route]["direction"].unique()

        # select direction
        direction = st.sidebar.selectbox(
            "Select Direction:",
            (filtered_directions),
        )
        filtered_lane_type = raw_df[
            (raw_df["fwy_num"] == route) & (raw_df["direction"] == direction)
        ]["lane_type"].unique()

        # select lane type
        lane_type = st.sidebar.selectbox(
            "Select Lane Type:",
            (filtered_lane_type),
        )

        route_df = load_route_data(raw_df, route, direction, lane_type)

        # select date
        selected_date = st.sidebar.date_input(
            "Start Date",
            value=start_date,
            min_value=start_date,
            max_value=end_date,
        )

        # route with selected date
        route_date_df = date_df(route_df, selected_date)

        # route with hourly flow
        df_hr_flow = route_hourly_flow(route_date_df)

        st.write("# Caltrans D12 Daily Traffic Flow Oct. 2023")
        st.write("## Raw Data")
        st.write(df_hr_flow.head(20))

        st.write(f"## Figure of Route {route} {direction} {lane_type}")

        # x_labels = sorted(df_hr_flow.columns)
        x_labels = [str(x) for x in sorted(df_hr_flow.columns)]

        fig = px.imshow(
            df_hr_flow.values,
            labels=dict(x="Post Mile", y="Hour", color="Traffic Flow"),
            x=x_labels,
            y=df_hr_flow.index,
            color_continuous_scale="YlOrRd",
            aspect="auto",
            origin="lower",
        )

        fig.update_layout(
            title=f"Heatmap of Hourly Traffic Flow along Route {route} {direction} {lane_type}",
            yaxis=dict(
                tickmode="array",
                tickvals=list(df_hr_flow.index),
                ticktext=list(df_hr_flow.index),
            ),
        )
        fig.update_xaxes(type="category")

        st.plotly_chart(fig, use_container_width=True)

else:
    st.error("No file uploaded.")
