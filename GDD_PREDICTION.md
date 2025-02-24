# GDD Prediction and Bud Break Projection

This document explains the methodology and implementation details behind the bud break prediction in our project. We use three prediction models: **historical regression**, **hybrid modeling**, and an **enhanced EHML model** to determine when each grapevine variety will break bud. These models leverage historical Growing Degree Days (GDD) data, real-time accumulation, and machine learning techniques to improve prediction accuracy.

## Overview of Prediction Models

### 1. Regression-Based Projection
The function **`project_bud_break_regression()`** predicts the bud break date for each grape variety using a linear regression approach. The methodology follows these steps:

1. **Ensures the `regression_projected_bud_break` column exists** in the `grapevine_gdd` table. If missing, the function adds it automatically.
2. **Collects Historical Bud Break Data**  
   - For each year (from the oldest available year to the prior year), the function finds the first date when the cumulative GDD reaches or exceeds the variety’s required threshold (`heat_summation`).
3. **Converts Bud Break Dates to Day-of-Year (DOY):**  
   - Each bud break date is converted into a numerical day-of-year (DOY) value.
4. **Performs Linear Regression:**  
   - The regression equation is derived using:

     $$\text{slope} = \frac{\sum (x - \bar{x})(y - \bar{y})}{\sum (x - \bar{x})^2}$$

     $$\text{intercept} = \bar{y} - \text{slope} \times \bar{x}$$

     where:
     - $\bar{x}$ is the average of the years.
     - $\bar{y}$ is the average of the bud break DOY values.

5. **Predicts the Current Year’s Bud Break DOY:**  
   - Using the regression equation:

$$\text{predicted-DOY} = \text{slope} \times (\text{current year}) + \text{intercept}$$

   - The value is constrained between 1 and 366.
6. **Converts the Predicted DOY Back to a Calendar Date**
7. **Updates the Database** with the predicted bud break date under `regression_projected_bud_break`.

### 2. Hybrid Projection Model
The function **`project_bud_break_hybrid()`** refines predictions by integrating **historical and real-time GDD trends**. This method:

1. **Determines the Median-Based Target GDD Threshold**
   - The median GDD value at bud break from historical data is used as the target.
2. **Calculates Forecasted GDD Accumulation**  
   - The function retrieves accumulated GDD from the database.
   - It predicts **the next 14 days of GDD accumulation** and adds it to the current total.
   - The remaining GDD required to reach bud break is computed.
3. **Estimates the Bud Break Date** using the formula:

     $$\text{days-remaining} = \frac{\text{remaining-GDD}}{\text{avg-daily-GDD}}$$

   - Historical GDD rates determine the expected daily GDD accumulation.
   - The predicted date is calculated by adding `days_remaining` to the current date.
4. **Computes Confidence Range:**
   - The standard deviation of historical bud break DOY values ($\sigma$) is used:

     $$\text{range-start} = \max(1, \text{predicted-DOY} - \sigma)$$

     $$\text{range-end} = \min(366, \text{predicted-DOY} + \sigma)$$

5. **Updates the Database** with the projected bud break date and range under `hybrid_projected_bud_break` and `hybrid_bud_break_range`.

### 3. Enhanced EHML Model
The function **`project_bud_break_ehml()`** applies an **XGBoost regression model trained on 25 years of temperature data at 5-second intervals**. This method:

1. **Accounts for Variability in Chill Hours and GDD Accumulation**  
   - Full-season chill hours (from September 1 to March 1) are calculated.
   - Historical chill hours are averaged to estimate the current year's chill.
2. **Builds a Machine Learning Model with the Following Features:**
   - **Current accumulated GDD**
   - **Day of the year (DOY)**
   - **Estimated chill hours**
   - **Historical GDD statistics (mean and standard deviation)**
3. **Trains an XGBoost Regression Model**
   - The model is trained on historical data to predict the remaining GDD required for bud break.
4. **Predicts the Bud Break Date Using a Dynamic GDD Accumulation Rate:**
   - A **30-day rolling average GDD rate** is calculated from historical records.
   - The remaining GDD is divided by this rate to estimate the number of days required to reach bud break:

     $$\text{days-remaining} = \frac{\text{remaining-GDD}}{\text{avg-daily-GDD}}$$

   - The bud break date is determined accordingly.
5. **Updates the Database**
   - The predicted bud break date is stored under `ehml_projected_bud_break`.

## Key Updates Based on Code Review

1. **Regression Model Enhancements:**
   - Added handling for cases where there is insufficient historical data.
   - Now ensures predicted DOY values remain within valid date ranges (1-366).

2. **Hybrid Model Adjustments:**
   - Now factors in **real-time GDD accumulation and 14-day forecasted GDD trends**.
   - Uses **historical bud break DOY variance** to estimate confidence intervals.

3. **EHML Model Refinements:**
   - Introduces a **machine learning model trained on 25 years of temperature data**.
   - Uses **dynamic GDD accumulation rates instead of static estimates**.
   - Implements a **30-day rolling average for improved accuracy**.

## Future Considerations

- **Further model refinement** by incorporating additional environmental factors (e.g., precipitation, soil moisture).
- **Validation with new data points** to improve predictive accuracy.
- **Integration of real-time weather forecasts** to refine predictions dynamically.



