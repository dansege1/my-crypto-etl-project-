import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns # is a library for making statistical graphics in Python.

# 1. Load the cleaned data
df = pd.read_csv('crypto_market_data.csv')

# 2. Sort by Market Cap and take the top 10
top_10 = df.nlargest(10, 'market_cap')

# 3. Create the Visualization
plt.figure(figsize=(12, 6))
sns.set_theme(style="whitegrid")

# Create a bar chart
bar_plot = sns.barplot(
    x='market_cap', 
    y='name', 
    data=top_10, 
    palette='viridis'
)

# 4. Format the chart to look professional
plt.title('Top 10 Cryptocurrencies by Market Cap (USD)', fontsize=16, pad=20)
plt.xlabel('Market Cap (in Billions)', fontsize=12)
plt.ylabel('Cryptocurrency', fontsize=12)

# Make the X-axis easier to read (convert to Billions)
ticks = plt.xticks()[0]
plt.xticks(ticks, [f'${x/1e9:.0f}B' for x in ticks])

plt.tight_layout()

# 5. Save the chart for your LinkedIn post
plt.savefig('top_10_market_cap.png')
print("🚀 Chart saved as top_10_market_cap.png")
plt.show()
