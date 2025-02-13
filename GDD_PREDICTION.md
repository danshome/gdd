# GDD Prediction and Bud Break Projection

This document explains the methodology and implementation details behind the bud break prediction in our project. In our model, we use historical regression to predict when each grapevine variety will break bud. This approach is based on the idea that bud break occurs when the vine’s cumulative Growing Degree Days (GDD) (calculated from January 1) reaches a specific threshold value (the "heat_summation" defined per variety). By analyzing historical data, we can derive a trend and then apply it to predict the current year’s bud break date.

## Overview

The function **`project_bud_break_regression()`** in `gdd.py` predicts the bud break date for each grape variety using a multi-year regression approach. For each variety, it:

1. **Collects Historical Bud Break Data:**  
   - For each year (from 2012 to the year prior to the current year), the function finds the first date when the cumulative GDD reaches or exceeds the variety’s required threshold (heat_summation).
   
2. **Converts Dates to Day-of-Year (DOY):**  
   - Each bud break date is converted to a numerical day-of-year (DOY) value (e.g., March 1 is approximately DOY 60 in a non-leap year).

3. **Performs Linear Regression:**  
   - A simple linear regression is performed with:
     - **x:** Year
     - **y:** Bud break DOY  
   - The regression determines a slope (the rate of change in DOY per year) and an intercept.

4. **Predicts the Current Year’s Bud Break DOY:**  
   - The current year is inserted into the regression equation to calculate the predicted DOY:
     $$\text{predicted\_DOY} = \text{slope} \times (\text{current year}) + \text{intercept}$$
   - The predicted DOY is bounded between 1 and 366.

5. **Converts the Predicted DOY Back to a Calendar Date:**  
   - Using January 1 of the current year as the base, the predicted DOY is converted to a calendar date.

6. **Updates the Database:**  
   - The predicted bud break date is stored in the `grapevine_gdd` table (in the `projected_bud_break` column) for the corresponding grape variety.

## Detailed Methodology

### 1. Historical Data Collection

For each grape variety, the function queries the `readings` table for each historical year (from 2012 until the previous year). It searches for the first reading (chronologically) where the cumulative GDD (accumulated from January 1) reaches or exceeds the variety’s defined threshold (stored in the `heat_summation` field).

### 2. Conversion to Day-of-Year (DOY)

Once the first date of bud break is determined for a given year, the function converts this date into a DOY value using Python’s datetime utilities. This standardizes the data so that we can compare bud break timing across different years.

### 3. Linear Regression

With a set of historical (year, bud_break_DOY) pairs, the function performs a simple linear regression. The formulas used are:

- **Slope:**
  $$\text{slope} = \frac{\sum (x - \bar{x})(y - \bar{y})}{\sum (x - \bar{x})^2}$$

- **Intercept:**
  $$\text{intercept} = \bar{y} - \text{slope} \times \bar{x}$$

Where:
-  $\bar{x}$ is the average of the years.
-  $\bar{y}$  is the average of the bud break DOY values.

A negative slope implies that bud break is occurring earlier over time, which is consistent with trends under climate warming.

### 4. Prediction for the Current Year

The current year is plugged into the regression equation:
$$\text{predicted\_DOY} = \text{slope} \times (\text{current year}) + \text{intercept}$$
This gives us the bud break DOY for the current year. The value is then clamped between 1 and 366 to ensure it is valid.

### 5. Conversion to Calendar Date

The numerical DOY is converted back into a calendar date by taking January 1 of the current year and adding the appropriate number of days (predicted_DOY - 1).

### 6. Updating the Database

The predicted calendar date is then stored in the `grapevine_gdd` table for the respective variety under the column `projected_bud_break`.

## Example Log Output

Example log messages produced by the function might be:
```
[2025-02-13 06:27:31.930] Predicted bud break for Chardonnay using regression: 2025-03-26 (slope: -0.41, intercept: 908.74)
[2025-02-13 06:27:35.947] Predicted bud break for Tempranillo using regression: 2025-03-27 (slope: -0.41, intercept: 920.59)
...
```
These outputs indicate the regression parameters and the final predicted bud break date for each variety.

## Rationale

The regression-based approach leverages long-term historical observations to capture trends in bud break timing. Rather than simply calculating a current accumulation rate, this method:
- Accounts for interannual variability and gradual shifts (e.g., due to climate change).
- Provides a more robust, trend-based prediction that aligns better with historical data.
- Helps adjust for the fact that simple GDD accumulation (starting from January 1) might not accurately capture the onset of bud break if early-year temperatures contribute little to effective heat accumulation.

## Future Considerations

- **Model Refinement:**  
  Future iterations could incorporate additional factors (e.g., chilling requirements, photoperiod) or weighted regression to further refine predictions.
- **Validation:**  
  Continually validating the regression predictions with new phenological observations will help adjust the model as conditions change.
- **Hybrid Approaches:**  
  Combining this regression trend with real-time GDD accumulation data could yield an even more robust prediction model.
