import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration (centered layout maintained)
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"
)

# Custom CSS styling (as before)
st.markdown("""
    <style>
        .css-18e3th9 {background-color: #F0F2F6;}
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
            bottom: 125%;
            left: 50%;
            margin-left: -125px;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
    </style>
""", unsafe_allow_html=True)

# Title and Snowflake connection (as before)
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Snowflake connection function
def connect_to_snowflake():
    try:
        connection_parameters = {
            "account": st.secrets["SNOWFLAKE_ACCOUNT"],
            "user": st.secrets["SNOWFLAKE_USER"],
            "password": st.secrets["SNOWFLAKE_PASSWORD"],
            "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
            "database": st.secrets["SNOWFLAKE_DATABASE"],
            "schema": st.secrets["SNOWFLAKE_SCHEMA"],
        }
        session = Session.builder.configs(connection_parameters).create()
        st.success("✅ Successfully connected to Snowflake")
        return session
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()

session = connect_to_snowflake()

# Fetch Override_Ref data function (modified)
def fetch_override_ref_data(module_name):
    try:
        df = session.sql(f"SELECT * FROM override_ref WHERE MODULE_NAME = '{module_name}'").to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching Override_Ref data: {e}")
        return pd.DataFrame()

# Fetch data function (modified to include RECORD_FLAG filter)
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name} WHERE RECORD_FLAG = 'A'"
        df = session.sql(query).to_pandas()
        df.columns = [col.strip().upper() for col in df.columns]  # Normalize column names
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Retrieve module name from URL
query_params = st.query_params
module_name = query_params.get("module", None)

if not module_name:
    st.error("Module name not provided in URL. Please access this app from Power BI by clicking the override button.")
    st.stop()

# Fetch override reference data using the module name
override_ref_df = fetch_override_ref_data(module_name)

if override_ref_df.empty:
    st.warning(f"No configuration data found for module: {module_name} in Override_Ref.")
    st.stop()

# Retrieve source table dynamically based on module
source_table = override_ref_df['SOURCE_TABLE'].iloc[0]
target_table = override_ref_df['TARGET_TABLE'].iloc[0]
editable_column = override_ref_df['EDITABLE_COLUMN'].iloc[0].strip().upper()
join_keys = override_ref_df['JOINING_KEYS'].iloc[0].strip().upper().split(',')

# Display the module name in an ice-blue box
st.markdown(f"<div class='module-box'>{module_name.upper()}</div>", unsafe_allow_html=True)

# Dropdown to select the table
available_tables = override_ref_df['SOURCE_TABLE'].tolist()
selected_table = st.selectbox("Select Table", available_tables, index=available_tables.index(source_table) if source_table in available_tables else 0)

# Fetch data for selected table
source_df = fetch_data(selected_table)
if source_df.empty:
    st.warning("No data found in the source table.")
    st.stop()

# Tabs for Source Data and Overridden Values
tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

# Tab 1: Source Data
with tab1:
    st.header(f"Source Data from {selected_table}")
    st.markdown(f"Editable Column: {editable_column}")

    # Create a copy of the source data for editing
    editable_df = source_df.copy()

    # Data Editor component
    edited_data = st.data_editor(
        editable_df,
        column_config={
            editable_column: st.column_config.NumberColumn(f"{editable_column} (Editable)✏️")
        },
        disabled=[col for col in editable_df.columns if col != editable_column],
        use_container_width=True,
        hide_index=True  # Remove the index column
    )

    st.markdown('<div class="tooltip">Hover to see description<span class="tooltiptext">This action will update the data.</span></div>', unsafe_allow_html=True)

    # Function to insert into target table dynamically
    def insert_into_target_table(session, source_df, edited_data, target_table, editable_column, join_keys):
        try:
            # Identify rows where the editable column has changed
            changes_df = edited_data[edited_data[editable_column] != source_df[editable_column]]

            if changes_df.empty:
                st.info("No changes detected. No records to insert.")
                return

            st.write("🟢 Detected Changes:")
            st.dataframe(changes_df)

            # Fetch the target table columns dynamically
            target_columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table.upper()}'"
            target_columns = [row['COLUMN_NAME'].upper() for row in session.sql(target_columns_query).to_pandas().to_dict('records')]

            # Identify common columns (excluding SRC_INS_TS, editable_column_old, editable_column_new, record_flag, and as_at_date)
            common_columns = [col for col in source_df.columns if col in target_columns and col not in [editable_column, 'AS_AT_DATE', 'RECORD_FLAG','AS_OF_DATE']]

            for _, row in changes_df.iterrows():
                old_value = source_df.loc[source_df.index == row.name, editable_column].values[0]
                new_value = row[editable_column]
                as_at_date = row['AS_AT_DATE']
                as_of_date = row['AS_OF_DATE']

                # Forming the dynamic insert query
                columns_to_insert = ', '.join(common_columns + ['AS_OF_DATE','SRC_INS_TS', f'{editable_column}_OLD', f'{editable_column}_NEW', 'RECORD_FLAG', 'AS_AT_DATE'])
                values_to_insert = ', '.join([f"'{row[col]}'" if isinstance(row[col], str) else str(row[col]) for col in common_columns])

                insert_sql = f"""
                    INSERT INTO {target_table} ({columns_to_insert})
                    VALUES (
                        {values_to_insert},'{as_of_date}', '{as_at_date}', {old_value}, {new_value}, 'A', CURRENT_TIMESTAMP()
                    )
                """
                session.sql(insert_sql).collect()

        except Exception as e:
            st.error(f"❌ Error inserting into {target_table}: {e}")

    # Single 'Submit Changes' button
    def insert_into_source_table(session, target_table, source_table, editable_column, join_keys):
        try:
            # Generate common columns excluding record_flag, as_at_date, and editable_column
            target_columns_query = f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE UPPER(TABLE_NAME) = '{source_table.upper()}'
                  AND COLUMN_NAME NOT IN ('RECORD_FLAG', 'AS_AT_DATE', '{editable_column.upper()}')
            """
            common_columns = [row['COLUMN_NAME'].upper() for row in session.sql(target_columns_query).to_pandas().to_dict('records')]

            if not common_columns:
                st.error("No matching common columns found between target and source.")
                return

            # Formulate the insert SQL query
            columns_to_insert = ', '.join(common_columns + [editable_column, 'RECORD_FLAG', 'AS_AT_DATE'])

            insert_sql = f"""
                INSERT INTO {source_table} ({columns_to_insert})
                SELECT
                    {', '.join([f"src.{col}" for col in common_columns])},
                    src.{editable_column}_NEW,
                    'A',
                    CURRENT_TIMESTAMP(0)
                FROM {target_table} src
                JOIN {source_table} tgt
                ON {" AND ".join([f"tgt.{key} = src.{key}" for key in join_keys])}
                AND tgt.{editable_column} = src.{editable_column}_OLD
                WHERE tgt.RECORD_FLAG = 'A';
            """

            # Execute SQL
            session.sql(insert_sql).collect()
            #st.success(f"✅ Data inserted into {source_table} from {target_table}")

        except Exception as e:
            st.error(f"❌ Error inserting into {source_table}: {e}")

    # Function to update the old record in the source table
    def update_old_record(session, target_table, source_table, editable_column, join_keys):
        try:
            # Form the dynamic SQL query to update old records
            join_condition = " AND ".join([f"tgt.{key} = src.{key}" for key in join_keys])

            update_sql = f"""
                UPDATE {source_table} tgt
                SET record_flag = 'D'
                FROM {target_table} src
                WHERE {join_condition}
                  AND tgt.{editable_column} = src.{editable_column}_OLD
                  AND tgt.record_flag = 'A';
            """

            session.sql(update_sql).collect()
            #st.success(f"✅ Old records updated in {source_table} with record_flag = 'D'")

        except Exception as e:
            st.error(f"❌ Error updating old records in {source_table}: {e}")

    if st.button("Submit Updates"):
        # Step 1: Insert into target table (fact_portfolio_perf_override)
        insert_into_target_table(session, source_df, edited_data, target_table, editable_column, join_keys)

        # Step 2: Insert into source table (fact_portfolio_perf)
        insert_into_source_table(session, target_table, source_table, editable_column, join_keys)

        # Step 3: Update old records in source table (fact_portfolio_perf)
        update_old_record(session, target_table, source_table, editable_column, join_keys)

        st.success("✅ Data updated successfully!")
       
# Tab 2: Overridden Values
with tab2:
    st.header(f"Overridden Values in {target_table}")
    overridden_data = fetch_data(target_table)
    if overridden_data.empty:
        st.warning("No overridden data found in the target table.")
    else:
        st.dataframe(overridden_data, use_container_width=True)
