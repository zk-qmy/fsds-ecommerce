from pathlib import Path

import pandas as pd
from deltalake.writer import write_deltalake

SOURCE = "a_data_generator/outputs/offline/orders.parquet"


def try_write(df, path):
    try:
        write_deltalake(path, df, mode="overwrite")
        print(f"✅ SUCCESS -> {path}")
        return True
    except Exception as e:
        print(f"❌ FAILED -> {path}")
        print(type(e).__name__, e)
        return False


def main():

    print("=" * 80)
    print("Loading parquet...")
    df = pd.read_parquet(SOURCE)

    print("\nShape")
    print(df.shape)

    print("\nDtypes")
    print(df.dtypes)

    print("\nNulls")
    print(df.isna().sum())

    print("\nPython types per column")
    for col in df.columns:
        print(f"\n{col}")
        print(df[col].map(type).value_counts())

    print("\nFirst rows")
    print(df.head())

    print("\nLast rows")
    print(df.tail())

    print("\n" + "=" * 80)
    print("Testing write with increasing sizes")

    sizes = [
        100,
        1000,
        10000,
        50000,
        100000,
        len(df),
    ]

    for size in sizes:
        print(f"\nTrying {size:,} rows")

        try_write(
            df.head(size),
            f"./debug_delta/orders_{size}",
        )

    print("\n" + "=" * 80)
    print("Converting object columns to pandas StringDtype")

    df2 = df.copy()

    string_cols = df2.select_dtypes(include="object").columns

    for col in string_cols:
        df2[col] = df2[col].astype("string")

    print(df2.dtypes)

    try_write(
        df2,
        "./debug_delta/orders_string_dtype",
    )

    print("\n" + "=" * 80)
    print("Checking timestamp precision")

    for col in df2.select_dtypes(include="datetime").columns:
        print(col, df2[col].dtype)

    print("\nFinished.")


if __name__ == "__main__":
    main()