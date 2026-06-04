import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def main():
    df = pd.read_csv(
        "https://huggingface.co/api/resolve-cache/datasets/scikit-learn/adult-census-income/fbeef6ec0e6fd88a5028b94683144000a6b380d5/adult.csv?%2Fdatasets%2Fscikit-learn%2Fadult-census-income%2Fresolve%2Fmain%2Fadult.csv=&etag=%225cf74ede1a6de37d85c96a61d30819a694dee749%22",
        engine="pyarrow",
        dtype_backend="pyarrow",
    )
    print(df.head())
    print(df.shape)
    print(df.info())
    print(df.head().to_string())

    print("Before cleaning: ", len(df))
    df.replace("?", pd.NA, inplace=True)
    df.dropna(inplace=True)
    print("After cleaning: ", len(df))

    # Income Graph
    fig = px.histogram(df, x="income", title="Income Distribution")
    fig.show()

    # Education and Income Relationship
    result = df.groupby("education")["income"].value_counts().unstack()
    print(result)

    fig = go.Figure()
    for col in result.columns:
        fig.add_trace(go.Bar(x=result.index, y=result[col], name=str(col)))
    fig.update_layout(
        barmode="group",
        title="Income by Education",
        xaxis_title="Education Level",
        yaxis_title="Count",
    )
    fig.show()

    # Hours per week by Income
    fig = px.box(df, x="income", y="hours.per.week", title="Hours per Week by Income")
    fig.show()

    # Income by Gender
    result = df.groupby("sex")["income"].value_counts().unstack()

    fig = go.Figure()
    colors = {"<=50K": "skyblue", ">50K": "blue"}
    for col in result.columns:
        fig.add_trace(
            go.Bar(
                x=result.index,
                y=result[col],
                name=str(col),
                marker_color=colors.get(str(col), "gray"),
                text=result[col],
                textposition="auto",
            )
        )
    fig.update_layout(
        barmode="group",
        title="Income Distribution by Gender",
        xaxis_title="Gender",
        yaxis_title="Count",
    )
    fig.show()

    print(df["workclass"].unique())

    # Select workclasses
    top_workclasses = df["workclass"].value_counts().head(7).index.tolist()

    filtered_df = df.loc[df["workclass"].isin(top_workclasses)]

    fig = px.histogram(
        filtered_df,
        x="workclass",
        color="income",
        barmode="group",
        title="Income Distribution Across Workclass Categories",
        labels={"workclass": "Workclass", "count": "Number of Individuals"},
    )
    fig.show()

    result = df.loc[df["income"] == ">50K", "occupation"].value_counts().head(10)
    print(result)

    # Age vs Income
    fig = px.box(df, x="income", y="age", title="Age vs Income Pattern")
    fig.update_layout(xaxis_title="Income Category", yaxis_title="Age")
    fig.show()


if __name__ == "__main__":
    main()
