import dask.dataframe as dd
import pandas as pd


# luca
# @pytest.mark.skipif(DASK_EXPR_ENABLED, reason="not important now")
def test_attrs_dataframe():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    df.attrs = {"date": "2020-10-16"}
    ddf = dd.from_pandas(df, 2)

    assert df.attrs == ddf.attrs
    assert df.abs().attrs == ddf.abs().attrs
    assert df.attrs == ddf.compute().attrs


# luca
# @pytest.mark.skipif(DASK_EXPR_ENABLED, reason="not important now")
def test_attrs_series():
    s = pd.Series([1, 2], name="A")
    s.attrs["unit"] = "kg"
    ds = dd.from_pandas(s, 2)

    assert s.attrs == ds.attrs
    assert s.fillna(1).attrs == ds.fillna(1).attrs
    assert s.attrs == ds.compute().attrs


def test_attrs_dataframe_not_shared_across_instances():
    df0 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    df0.attrs = {"date": "2020-10-16"}
    ddf0 = dd.from_pandas(df0, 2)

    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    ddf1 = dd.from_pandas(df1, 2)

    assert df0.attrs != df1.attrs
    assert ddf1.attrs != ddf0.attrs


def test_attrs_series_not_shared_across_instances():
    s0 = pd.Series([1, 2], name="A")
    s0.attrs["unit"] = "kg"
    ds0 = dd.from_pandas(s0, 2)

    s1 = pd.Series([1, 2], name="A")
    ds1 = dd.from_pandas(s1, 2)

    assert s0.attrs != s1.attrs
    assert ds1.attrs != ds0.attrs


def test_attrs_dataframe_optimize():
    df0 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    df0.attrs = {"date": "2020-10-16"}
    df = dd.from_pandas(df0)
    df.attrs["foo"] = "foo"
    df = df.fillna(100)
    df = df["A"]
    print(df.attrs)
    df.attrs["foo"] = "bar"
    print(df.attrs)

    assert df.optimize().attrs == df.attrs


def test_attrs_series_optimize():
    s0 = pd.Series([1, 2], name="A")
    s0.attrs["unit"] = "kg"
    ds = dd.from_pandas(s0, 2)
    ds.attrs["foo"] = "foo"
    ds = ds.fillna(100)
    print(ds.attrs)
    ds.attrs["foo"] = "bar"
    print(ds.attrs)

    assert ds.optimize().attrs == ds.attrs
