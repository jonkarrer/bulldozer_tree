# Random Forrest Classifier

We will create some decision trees and use them to classify new data points. This project is based on lesson 6 of [Practical Deep Learning For Coders](https://course.fast.ai/Lessons/lesson6.html). It takes heavy equipment data and tries to predict the sale price of the equipment.

## Dataset

The dataset is found here: [Blue Book for Bulldozers](https://www.kaggle.com/datasets/farhanreynaldo/blue-book-for-bulldozer).

### Exploratory Data Analysis

There are 52 columns and 400000+ rows in the training set. Our dependant variable is **saleprice**. There could be a lot of missing values, and we could do a ton of feature engineering on this much data. However, this project is going to keep it simple.

Key fields:

- **SalesID**
- **MachineID**
- **saleprice**
- **saledate**

#### Validation Set

Since this is time series data, and we want to predict the future, the validation set would need to be the dates after the training set.

#### Dates

We need to give the date types more context. Dates are not just strings. They are embedded with information such as day of week, month of year, holiday, and weekend. So we will break each date into multiple columns that represent just that.

#### Missing Values

Depending on the data missing and how important that is, we have a few options:

- Drop missing values
- Fill missing values with mean
- Fill missing values with median
- Fill missing values with mode

In the case of filling up a column, we need to create another column marking that event. Usually this is a boolean column saying "hey we filled this record in".

#### Ordinal Data

Some categorical data is ordinal, meaning it has some sort of order. Size is an example of this. We already know that at the end of this data prep everything needs to be numerical. So if the data is ordinal, we need to convert it to numerical, and keep the ordering of the data.

#### Processing

1. Expand date columns into more columns, such as month (1-12), holiday (bool), day of week (1-7), weekend (bool).
2. Fill missing values with median and create a new column to mark the event (bool).
3. Convert ordinal data to numerical, but keeping the order
4. Convert non ordinal data to numerical.
