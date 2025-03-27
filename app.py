import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"
)

# Custom CSS for styling
st.markdown("""
    <style>
        .css-18e3th9 {background-color: #F0F2F6;} /* Light grey background */
        .css-1kyxreq {border-radius: 12px; padding: 20px;}
        .css-1b36jdy {text-align: center;}
        .stButton>button {background-color: #1E88E5; color: white; border-radius: 5px; height: 40px;}
        .stSelectbox>label {font-size: 16px;}
        .stDataFrame {border: 1px solid #dddddd; border-radius: 8px;}
        .module-box {
            background-color: #D3E8FF; /* Ice-blue box for module */
            padding: 15px;
            border-radius: 8px;
            font-size: 20px;
            font-weight: bold;
            text-align: center;
        }
        /* Tooltip CSS for hover effect */
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: pointer;
        }

        .tooltip .tooltiptext {
            visibility: hidden;
            width: 250px;
            background-color: #6c757d;
            color: #fff;
            text-align: center;
            border-radius: 5px;
            padding: 5px;
            position: absolute;
            z-index: 1;
            bottom: 125%; /* Position above the button */
            left: 50%;
            margin-left: -125px; /* Centers the tooltip */
            opacity: 0;
            transition: opacity 0.3s;
        }

        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
    </style>
""", unsafe_allow_html=True)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Snowflake Connection Status using Streamlit secrets
try:
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    # ✅ Create a Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    st.success("✅ Successfully connected to Snowflake!")

except Exception as e:
    st.error(f"❌ Error connecting to Snowflake: {e}")
    st.stop()

# Function to fetch data based on the table name
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch last updated timestamp
def fetch_last_updated_timestamp():
    try:
        result = session.sql("""
            SELECT CURRENT_TIMESTAMP()
        """).collect()

        if result:
            last_updated_timestamp = result[0][0]
            return last_updated_timestamp.strftime('%B %d, %Y %H:%M:%S')
        else:
            return "No updates yet"
    except Exception as e:
        st.warning(f"Error fetching last updated timestamp: {e}")
        return "Error fetching timestamp"

# Function to insert into override table
def insert_into_override_table(target_table, row_data, old_value, new_value):
    try:
        as_of_date = row_data['AS_OF_DATE']
        asset_class = row_data['ASSET_CLASS']
        segment = row_data['SEGMENT']
        segment_name = row_data['SEGMENT_NAME']
        strategy = row_data['STRATEGY']
        strategy_name = row_data['STRATEGY_NAME']
        portfolio = row_data['PORTFOLIO']
        portfolio_name = row_data['PORTFOLIO_NAME']
        holding_fund_ids = row_data['HOLDING_FUND_IDS']
        unitized_owner_ind = row_data['UNITIZED_OWNER_IND']

        insert_sql = f"""
            INSERT INTO {target_table} (AS_OF_DATE, ASSET_CLASS, SEGMENT, SEGMENT_NAME, STRATEGY, STRATEGY_NAME, PORTFOLIO, PORTFOLIO_NAME, HOLDING_FUND_IDS, MARKET_VALUE_OLD, MARKET_VALUE_NEW, UNITIZED_OWNER_IND, AS_AT_DATE, RECORD_FLAG)
            VALUES ('{as_of_date}', '{asset_class}', '{segment}', '{segment_name}', '{strategy}', '{strategy_name}', '{portfolio}', '{portfolio_name}', '{holding_fund_ids}', {old_value}, {new_value}, {unitized_owner_ind}, CURRENT_TIMESTAMP(), 'O')
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Function to insert into source table
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()

        if editable_column.upper() in row_data_copy:
            del row_data_copy[editable_column.upper()]

        if 'RECORD_FLAG' in row_data_copy:
            del row_data_copy['RECORD_FLAG']

        if 'AS_AT_DATE' in row_data_copy:
            del row_data_copy['AS_AT_DATE']

        columns = ", ".join(row_data_copy.keys())
        formatted_values = []

        for col, val in row_data_copy.items():
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif val is None or pd.isna(val):
                formatted_values.append("NULL")
            elif isinstance(val, (int, float)):
                formatted_values.append(str(val))
            elif isinstance(val, pd.Timestamp):
                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(val, datetime):
                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                formatted_values.append(f"'{str(val)}'")

        values = ", ".join(formatted_values)

        insert_sql = f"""
            INSERT INTO {source_table} ({columns}, {editable_column}, record_flag, as_at_date)
            VALUES ({values}, '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

# Function to update record flag in source table
def update_source_table_record_flag(source_table, primary_key_values):
    try:
        where_clause_parts = []
        for col, val in primary_key_values.items():
            if val is None:
                where_clause_parts.append(f"{col} IS NULL")
            else:
                where_clause_parts.append(f"{col} = '{val}'")
        where_clause = " AND ".join(where_clause_parts)

        update_sql = f"""
            UPDATE {source_table}
            SET record_flag = 'D',
                as_at_date = CURRENT_TIMESTAMP()
            WHERE {where_clause} AND record_flag = 'A'
        """
        session.sql(update_sql).collect()
    except Exception as e:
        st.error(f"Error updating record flag in {source_table}: {e}")

# Main app
module_ref_df = fetch_data("Override_Ref")
if not module_ref_df.empty:
    # Fetch the first module name
    module_name = module_ref_df['MODULE_NAME'].iloc[0]

    # Display module name in Ice-blue box
    st.markdown(f"<div class='module-box'>{module_name}</div>", unsafe_allow_html=True)

    # Fetch table details for the selected module
    selected_module_df = module_ref_df[module_ref_df['MODULE_NAME'] == module_name]
    selected_table = selected_module_df['SOURCE_TABLE'].iloc[0]

    # Show dropdown for table selection
    table_names = selected_module_df['SOURCE_TABLE'].unique()
    selected_table = st.selectbox("Select Table", table_names, key="table_selector")

    table_info_df = selected_module_df[selected_module_df['SOURCE_TABLE'] == selected_table]
    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]

        # Fetch the description for the module from the Override_Ref table
        description = table_info_df['DESCRIPTION'].iloc[0] if 'DESCRIPTION' in table_info_df.columns else "No description available."

        primary_key_cols = ['AS_OF_DATE', 'ASSET_CLASS', 'SEGMENT', 'STRATEGY', 'PORTFOLIO', 'UNITIZED_OWNER_IND', 'HOLDING_FUND_IDS']

        tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

        with tab1:
            st.subheader(f"Source Data from {selected_table}")
            source_df = fetch_data(selected_table)
            if not source_df.empty:
                source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()

                # Apply styling for the editable column
                styled_df = source_df.style.apply(
                    lambda x: ['background-color: #FFFFE0' if col == editable_column else '' for col in source_df.columns],
                    axis=0
                )

                # Display the editable column, read-only
                st.markdown(f"Editable Column: {editable_column}")

                # Remove the index column from the data editor display
                source_df = source_df.reset_index(drop=True)

                # Use Streamlit's data editor with the editable column
                edited_df = st.data_editor(
                    source_df,
                    key=f"data_editor_{selected_table}_{editable_column}",
                    num_rows="dynamic",
                    use_container_width=True,
                    disabled=[col for col in source_df.columns if col != editable_column]
                )

                # Submit updates
                # Display the description on hover
                st.markdown(f'<div class="tooltip">Hover to see description<span class="tooltiptext">{description}</span></div>', unsafe_allow_html=True)

                if st.button("Submit Updates"):
                    try:
                        changed_rows = edited_df[edited_df[editable_column] != source_df[editable_column]]
                        if not changed_rows.empty:
                            for index, row in changed_rows.iterrows():
                                primary_key_values = {col: row[col] for col in primary_key_cols}
                                new_value = row[editable_column]
                                old_value = source_df.loc[index, editable_column]

                                # Update the source table record flag for the old row
                                update_source_table_record_flag(selected_table, primary_key_values)

                                # Insert the new row into the source table
                                insert_into_source_table(selected_table, row.to_dict(), new_value, editable_column)

                                # Insert into override table
                                insert_into_override_table(target_table_name, row.to_dict(), old_value, new_value)

                            # Update last updated timestamp in session state
                            st.session_state.last_update_time = datetime.now().strftime('%B %d, %Y %H:%M:%S')

                            st.success("👍 Data updated successfully!")
                        else:
                            st.info("No changes were made.")
                    except Exception as e:
                        st.error(f"Error during update/insert: {e}")
            else:
                st.info(f"No data available in {selected_table}.")
        with tab2:
            st.subheader(f"Overridden Values from {target_table_name}")
            override_df = fetch_data(target_table_name)
            if not override_df.empty:
                st.dataframe(override_df, use_container_width=True)
            else:
                st.info(f"No overridden data available in {target_table_name}.")
    else:
        st.warning("No table information found for the selected module.")
else:
    st.warning("No modules found in Override_Ref table.")

# Footer
if 'last_update_time' in st.session_state:
    last_update_time = st.session_state.last_update_time
    st.markdown("---")
    st.caption(f"Portfolio Performance Override System • Last updated: {last_update_time}")
else:
    st.markdown("---")
    st.caption("Portfolio Performance Override System • Last updated: N/A")
