import pandas as pd
import pint_pandas  # noqa: F401 -- without this import statement the program fails.

df = pd.DataFrame(
    {
        "torque": pd.Series([1, 2, 2, 3], dtype="pint[lbf ft]"),
        "angular_velocity": pd.Series([1, 2, 2, 3], dtype="pint[rpm]"),
    }
)
df['power'] = df['torque'] * df['angular_velocity']
df.dtypes
