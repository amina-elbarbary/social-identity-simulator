from dataclasses import dataclass
from typing import List

import altair as alt
import duckdb
from jinja2 import Environment, FileSystemLoader
import streamlit as st

# edu | avg, low, high
# age | 1, 2, 3, 4, 5
# inc | avg, low, high
# nat | ger, nonger
# loc | east, west
# gen | male, female

jinja_env = Environment(loader=FileSystemLoader("."))


@dataclass
class DataIndicator:
    id: str
    name: str
    options: List[str]


@dataclass
class AppOptions:
    selected_indicators: List[DataIndicator]
    selected_metric: str


indicators: List[DataIndicator] = [
    DataIndicator(id="gen", name="Gender", options=["female", "male"]),
    DataIndicator(id="cit", name="Citizenship", options=["ger", "nonger"]),
    DataIndicator(id="edu", name="Education", options=["avg", "high", "low"]),
    DataIndicator(id="loc", name="Location", options=["east", "west"]),
    DataIndicator(id="inc", name="Income", options=["avg", "high", "low"]),
    DataIndicator(id="age", name="Age", options=["1", "2", "3", "4", "5"]),
]


def build_option_indicator_key(var: DataIndicator) -> str:
    return f"opt-indicator-{var.id}"


def render_indicator_options() -> List[DataIndicator]:
    st.write("**Choose at most 3 indicators:**")

    session_keys = {build_option_indicator_key(it): it for it in indicators}
    for session_key in session_keys.keys():
        if session_key not in st.session_state:
            st.session_state[session_key] = False

    values = [st.session_state[session_key] for session_key in session_keys]

    has_three = sum(values) >= 3
    for idx, indicator in enumerate(indicators):
        is_disabled = has_three and not values[idx]
        val = st.checkbox(
            key=build_option_indicator_key(indicator),
            label=indicator.name,
            disabled=is_disabled,
        )
        values[idx] = val

    return [session_keys[k] for k in session_keys.keys() if st.session_state[k]]


def render_option_mobilization() -> str:
    value = st.radio(
        "**Choose one metric:**",
        [
            "Homogeneity",  # no si
            "Social Identity",
        ],
    )
    if value == "Social Identity":
        value = st.radio(
            label="**Choose one mobilization:**",
            options=[
                "No mobilization",  # si 50
                "Class mobilization",  # si 25
                "Identity mobilization",  # si 75
            ],
        )
    return value


def render_sidebar():
    with st.sidebar:
        selected_indicators = render_indicator_options()
        selected_metric = render_option_mobilization()

    return AppOptions(
        selected_indicators=selected_indicators,
        selected_metric=selected_metric,
    )


def get_combinations(options: AppOptions):
    template = jinja_env.get_template("sql/test.sql")
    params = {
        "filters": [
            {"indicator": indicator.id, "option": option}
            for indicator in options.selected_indicators
            for option in indicator.options
        ],
        "indicator_count": len(options.selected_indicators),
    }
    sql = template.render(params)

    with duckdb.connect("duck.db", read_only=True) as con:
        df = con.sql(sql).to_df()
    return df


def query_data(options: AppOptions):
    template = jinja_env.get_template("sql/data.sql")
    params = {
        "filters": [
            {"indicator": indicator.id, "option": option}
            for indicator in options.selected_indicators
            for option in indicator.options
        ],
        "selected_metric": options.selected_metric,
        "indicator_count": len(options.selected_indicators),
    }
    sql = template.render(params)

    with duckdb.connect("duck.db", read_only=True) as con:
        df = con.sql(sql).to_df()
    return df


def render_charts(options: AppOptions):
    indicator_count = len(options.selected_indicators)
    if indicator_count == 0:
        st.markdown(
            "Please select at least one indicator from the sidebar on the left."
        )
        return

    # df = get_combinations(options)
    # st.dataframe(df, use_container_width=True, hide_index=True)

    df = query_data(options)
    df = df.drop(columns=["combination_id"])
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_order=["year", "combination", "indicator_type", "indicator", "value"],
        column_config={"year": st.column_config.TextColumn()},
    )

    column_count = max(len(it.options) for it in options.selected_indicators)

    chart = (
        alt.Chart(df)
        .mark_line(point=alt.OverlayMarkDef(filled=False, fill="white"))
        .encode(
            x=alt.X("year:N", title="Year"),
            y=alt.Y("value", title="Social Identity Score"),
            color="indicator:N",
            strokeWidth=alt.value(3),
        )
        .facet(
            facet="combination",
            columns=column_count,
        )
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)


def main():
    st.set_page_config(layout="wide")

    options = render_sidebar()

    render_charts(options)


if __name__ == "__main__":
    main()
