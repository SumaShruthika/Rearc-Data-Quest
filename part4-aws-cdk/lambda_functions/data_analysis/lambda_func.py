import pandas as pd
import boto3
import json
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    BUCKET_NAME = os.environ['BUCKET_NAME']
    PR_PREFIX = os.environ['BLS_PREFIX'] + 'pr.data.0.Current'
    POPULATION_PREFIX = os.environ['POPULATION_PREFIX'] + os.environ['JSON_FILE_NAME']

    part1_data = s3.get_object(Bucket=BUCKET_NAME, Key=PR_PREFIX)
    part1_df = pd.read_csv(part1_data['Body'], sep='\t')
    print(part1_df.head())

    api_population = s3.get_object(Bucket=BUCKET_NAME, Key=POPULATION_PREFIX)
    population_json = json.load(api_population["Body"])
    part2_df = pd.json_normalize(population_json['data'])
    print(part2_df.head())
    print(f"Actual columns: {part2_df.columns.tolist()}")

   
    part2_df['Year'] = part2_df['Year'].astype(int)
    df_pop_2013_2018 = part2_df[(part2_df['Year'] >= 2013) & (part2_df['Year'] <= 2018)]
    mean_pop = round(df_pop_2013_2018['Population'].mean(),2)
    std_pop = round(df_pop_2013_2018['Population'].std(),2)
    print(f"Mean Population (2013–2018): {mean_pop}")
    print(f"Standard Deviation: {std_pop}")

    part1_df.columns = part1_df.columns.str.strip()
    part1_df['value'] = pd.to_numeric(part1_df['value'], errors='coerce')
    grouped = part1_df.groupby(['series_id', 'year'])['value'].sum().reset_index()
    best_years = grouped.loc[grouped.groupby('series_id')['value'].idxmax()].reset_index(drop=True)
    print("Best year by series_id:")
    print(best_years.to_string(index=False))

    part1_df['series_id'] = part1_df['series_id'].str.strip()
    part1_df['period'] = part1_df['period'].str.strip()
    filtered = part1_df[(part1_df['series_id'] == 'PRS30006032') & (part1_df['period'] == 'Q01')]

    pop_lookup = part2_df[['Year', 'Population']].drop_duplicates()
    pop_lookup.columns = ['year', 'Population']

    final = pd.merge(filtered, pop_lookup, on='year', how='right')
    final_result = final[['series_id', 'year', 'period', 'value', 'Population']]
    final_df = final_result[final_result['Population'].notna()]
    print("Final joined result:")
    print(final_df.to_string(index=False))
