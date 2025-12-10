import pandas as pd
import pint_pandas
df = pd.DataFrame({
    "torque": pd.Series([1, 2, 2, 3], dtype="pint[lbf ft]"),
    "angular_velocity": pd.Series([1, 2, 2, 3], dtype="pint[rpm]"),
})
df['power'] = df['torque'] * df['angular_velocity']
df.dtypes
