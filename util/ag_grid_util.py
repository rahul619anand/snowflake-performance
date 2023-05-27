from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode


def agGrid(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    gridOptions = gb.build()

    return AgGrid(df, gridOptions=gridOptions, enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED)