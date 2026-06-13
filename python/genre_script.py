import pandas as pd

df = pd.read_csv("C:\\Users\\Cody\\Desktop\\Netflix genre cleaning\\genre data.csv")
df = pd.DataFrame(df)

genre_hierarchy = ['Documentary', 'Musical', 'Horror', 'Thriller', 'Action', 'Comedy', 'Drama', 'Romance', 'Crime', 'Mystery', 'Film-Noir', 'Western', 'Science Fiction', 'Fantasy', 'Adventure', 'War', 'History', 'Sport', 'Music', 'Animation', 'Children']


print(df.head())

import kagglehub

# Download latest version
path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")

print("Path to dataset files:", path)