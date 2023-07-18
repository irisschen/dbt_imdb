# DBT_IMDB

### The Project Documentation Website
([URL](https://kkstream.github.io/dbt_imdb/#!/overview/dbt_imdb))

---
### Step 1: Environment Setup
* #### By pipenv
    1. Install pipenv ([URL](https://pipenv.pypa.io/en/latest/))
    2. Run `pipenv install` at the repo's root directory
    3. Run `pipenv shell` to set up the environment
    
* #### By pip 
  1. Run `pip install -r requirements.txt` at the repo's root directory

### Step 2: Preparing the Data
  1. Download the [IMDb dataset](https://datasets.imdbws.com/)
  2. Place all the files downloaded from IMDb into the `data` folder

### Step 3: Run the Project
```
dbt run
```

---
### More Information: Variables 
```
dbt run --vars '{not_run_models: ['reformat_persons']}'
```
1. **Skip some models**
    ```
    not_run_models: ['reformat_persons']
    ```

2. **Metadata**
    * Modify the content_rating: 
        ```
        content_rating: ['adult']
        ```

3.  **Interaction**
    * interaction_type
        ```
        interaction_type: ('click', 'play', 'purchase')
        ```

    * interaction start at 
        ```
        interaction_start_date: '2023-06-07'
        ```
    * interaction end at
        ```
        interaction_end_date: '2023-06-09'
        ```
    * duration of training dataset
        1. by number of days
            ```
            training_days: '2'
            ```
        2. by specific date
            ```
            target_date: '2023-06-09'
            ```
