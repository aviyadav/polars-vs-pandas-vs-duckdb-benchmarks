import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


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
    sns.countplot(x="income", data=df)
    plt.show()

    # Education and Income Relationship
    result = df.groupby("education")["income"].value_counts().unstack()
    print(result)

    # Plot
    result.plot(kind="bar", figsize=(12, 6))
    # Labels and title
    plt.title("Income by Education")
    plt.xlabel("Education Level")
    plt.ylabel("Count")
    plt.xticks(rotation=45)

    plt.show()

    # Show graph
    sns.boxplot(x="income", y="hours.per.week", data=df)
    plt.show()

    result = df.groupby("sex")["income"].value_counts().unstack()

    # Plot with custom colors
    ax = result.plot(kind="bar", figsize=(10, 6), color=["skyblue", "blue"])

    # Add labels on bars
    for container in ax.containers:
        ax.bar_label(container)

    # Titles and labels
    plt.title("Income Distribution by Gender")
    plt.xlabel("Gender")
    plt.ylabel("Count")
    plt.xticks(rotation=0)

    # Show graph
    plt.show()

    print(df["workclass"].unique())

    # Select workclasses
    top_workclasses = df["workclass"].value_counts().head(7).index.tolist()

    filtered_df = df.loc[df["workclass"].isin(top_workclasses)]

    # Create chart
    plt.figure(figsize=(10, 6))

    sns.countplot(data=filtered_df, x="workclass", hue="income")

    # Titles and labels
    plt.title("Income Distribution Across Workclass Categories")
    plt.xlabel("Workclass")
    plt.ylabel("Number of Individuals")

    plt.xticks(rotation=15)

    plt.show()

    result = df.loc[df["income"] == ">50K", "occupation"].value_counts().head(10)
    print(result)

    # Create figure
    plt.figure(figsize=(10, 6))

    # Boxplot
    sns.boxplot(x="income", y="age", data=df)

    # Titles and labels
    plt.title("Age vs Income Pattern")
    plt.xlabel("Income Category")
    plt.ylabel("Age")

    # Show graph
    plt.show()


if __name__ == "__main__":
    main()
