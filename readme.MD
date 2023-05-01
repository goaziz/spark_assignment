![img.png](images/img.png)

### Spark is initialized with above configuration

![img_1.png](images/img_1.png)

### restaurant data is loaded into spark dataframe

![img_2.png](images/img_2.png)

![img_3.png](images/img_3.png)

### as you can see from the data there are some null values in the lat and lng columns

### now we will use the fillna function to fill the null values with opencage api

![img_4.png](images/img_4.png)

![img_5.png](images/img_5.png)

### we convert type of lat and lng from string to float type and apply geohash function to add geohash column

![img_6.png](images/img_6.png)


# Whether data exploration

![img_7.png](images/img_7.png)

### weather data is loaded into spark dataframe

![img_8.png](images/img_8.png)

![img_9.png](images/img_9.png)

### as you can see there are many duplicate values in the weather data now we will remove the duplicates

![img_10.png](images/img_10.png)

### duplicates are removed and lat and lng columns are renamed to weather_lat and weather_lng to avoid confusion and duplicate error

![img_11.png](images/img_11.png)

### we left-joined two dataframes based on lat and lng columns

![img_12.png](images/img_12.png)

### the result of join is stored as parquet file