%python
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm
import pandas as pd

# Specify the correct file path for the CSV file
file_path = r'C:\Users\10176902587257066791\Desktop\Projects - S&A\Errors\Result_207.csv'

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv(file_path)

# Count the number of rows
num_rows = df.shape[0]
print(f"Number of rows in the DataFrame: {num_rows}")

# Perform linear regression
X = sm.add_constant(df['box_size'])
model = OLS(df['error_rate'], X)
results = model.fit()

# Print the summary which includes p-values
print(results.summary())
