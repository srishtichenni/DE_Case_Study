from Utilities.utils import get_spark_session, read_data
from pyspark.sql.functions import desc, row_number, sum as spark_sum, col, countDistinct, count, when, lit
from pyspark.sql.window import Window

def analysis_1(person):
    '''
    Approach: 
    Filter the dataframe for crashes where gender : Male, death count > 0
    Get the total death count for these crashes. Return crashes with death count > 2

    Args: Primary_person_df

    Returns: Result for query 
    '''
    person = person.dropDuplicates()
    person = person.dropna(subset = ['CRASH_ID'])
    males_killed = person.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 0))
    males_killed = males_killed.groupBy('CRASH_ID').agg(spark_sum('DEATH_CNT').alias('total_deaths'))
    num_crashes =  males_killed.filter(col('total_deaths') > 2).select('CRASH_ID').distinct().count()
    return num_crashes

def analysis_2(units):
    '''
    Approach: Filter for motorcyles and return number of crashes

    Args: units_df

    Returns: Result for query
    '''
    two_wheeler_crash_num = units.filter(col('VEH_BODY_STYL_ID').contains("MOTORCYCLE")).select('CRASH_ID').distinct().count()
    return two_wheeler_crash_num

def analysis_3(person, units):
    '''
    Approach: Filter for df with death count >0 and air bag not deployed
    Merge with person_df to get top 5 vehicle makes

    Args: Primary_person_df, Units_df

    Returns: Result for query 
    '''
    top_vehicle_makes = (
        person.filter((col('DEATH_CNT') > 0) & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
        .select('CRASH_ID', 'UNIT_NBR')
        .join(units.select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID'), 
              on=['CRASH_ID', 'UNIT_NBR'], 
              how='left')
        .groupBy('VEH_MAKE_ID')
        .count()
        .orderBy(desc('count'))
        .limit(5)  
        .select('VEH_MAKE_ID')
    )

    top_vehicle_makes_list = [row['VEH_MAKE_ID'] for row in top_vehicle_makes.collect()]
    return top_vehicle_makes_list

def analysis_4(person, units):
    '''
    Args: Primary_person_df, Units_person_df

    Returns: Result for query 
    '''
    unit_df = units.select('CRASH_ID', 'UNIT_NBR', 'VIN', 'VEH_HNR_FL')

    filtered_person_df = (
        person
        .filter((col('PRSN_TYPE_ID') == 'DRIVER') & (col('DRVR_LIC_TYPE_ID').contains('DRIVER LIC')))
        .select('CRASH_ID', 'UNIT_NBR', 'DRVR_LIC_TYPE_ID', 'PRSN_TYPE_ID')
    )
    merged_df = filtered_person_df.join(unit_df, on=['CRASH_ID', 'UNIT_NBR'], how='left')
    unit_count = merged_df.filter(col('VEH_HNR_FL') == 'Y').select('VIN').distinct().count()

    return unit_count

def analysis_5(person):
    '''
    Args: Primary_person_df

    Returns: Result for query 
    '''
    filtered_df = person.filter(col('PRSN_GNDR_ID') != 'FEMALE').dropDuplicates()
    grouped_df = filtered_df.groupBy('DRVR_LIC_STATE_ID').agg(count('*').alias('count')).orderBy(desc('count')).limit(1)
    top_state = grouped_df.select('DRVR_LIC_STATE_ID').collect()[0]['DRVR_LIC_STATE_ID']
    return top_state

def analysis_6(units):
    '''
    Args: Units_person_df

    Returns: Result for query 
    '''
    result_df = (
    units
    .select('CRASH_ID', 'UNIT_NBR', 'TOT_INJRY_CNT', 'DEATH_CNT', 'VEH_MAKE_ID')
    .withColumn('tot_injry_death', col('TOT_INJRY_CNT') + col('DEATH_CNT'))
    .groupBy('VEH_MAKE_ID')
    .agg(spark_sum('tot_injry_death').alias('count'))
    .orderBy(desc('count'))
    )

    window_spec = Window.orderBy(desc('count'))
    result_with_index = result_df.withColumn('row_num', row_number().over(window_spec))

    filtered_result = (
        result_with_index
        .filter(col('row_num').between(3, 5))
        .select('VEH_MAKE_ID')
    )

    vehicle_makes_list = [row['VEH_MAKE_ID'] for row in filtered_result.collect()]
    return vehicle_makes_list


def analysis_7(person, units):
    '''
    Args: Primary_person_df, Units_person_df

    Returns: Result for query 
    '''
    person = person.dropna(subset=['PRSN_ETHNICITY_ID'])
    filtered_df = (
    person
    .select('CRASH_ID', 'UNIT_NBR', 'PRSN_ETHNICITY_ID')
    .dropDuplicates()
    .dropna()
    .join(
        units
        .select('CRASH_ID', 'UNIT_NBR', 'VEH_BODY_STYL_ID')
        .dropDuplicates()
        .dropna(),
        on=['CRASH_ID', 'UNIT_NBR'],
        how='left'
        )
    )

    window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('count'))

    result_df = (
        filtered_df
        .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
        .count()
        .withColumn('rank', row_number().over(window_spec))
        .filter(col('rank') == 1)
        .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
    )

    result_list = [row.asDict() for row in result_df.collect()]
    return result_list


def analysis_8(person):
    '''
    Args: Primary_person_df

    Returns: Result for query 
    '''
    person = person.dropna(subset=['DRVR_ZIP'])
    alc_df = person.filter(col('PRSN_ALC_RSLT_ID') == 'Positive')
    top_zip_codes_df = (
        alc_df
        .groupBy('DRVR_ZIP')
        .agg(countDistinct('CRASH_ID').alias('unique_crash_count'))
        .orderBy(desc('unique_crash_count'))
        .limit(5)
        .select('DRVR_ZIP')
    )
    top_zip_codes_list = [row['DRVR_ZIP'] for row in top_zip_codes_df.collect()]
    return top_zip_codes_list

def analysis_9(units, damages):
    '''
    Args: Units_person_df, Damages_df

    Returns: Result for query 
    '''
    units = units.dropna(subset=['VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID','FIN_RESP_TYPE_ID'])
    damages_ = damages.select("CRASH_ID").distinct()
    damage_levels = ["DAMAGED 4", "DAMAGED 6", "DAMAGED 5", "DAMAGED 7 HIGHEST"]
    udf = units.withColumn("DAMAGE1", when(col("VEH_DMAG_SCL_1_ID").isin(damage_levels), 1).otherwise(0)).withColumn("DAMAGE2", when(col("VEH_DMAG_SCL_2_ID").isin(damage_levels), 1).otherwise(0))
    no_damage = udf.join(damages_, on="CRASH_ID", how="left_anti")
    no_damage = no_damage.filter((col("DAMAGE1") == 1) | (col("DAMAGE2") == 1)).filter(col("FIN_RESP_TYPE_ID").contains("INSURANCE"))
    unique_crash_count = no_damage.select("CRASH_ID").distinct().count()

    return unique_crash_count

def analysis_10(person, units, charges):
    '''
    Args: Primary_person_df, Units_person_df, Charges_df

    Returns: Result for query 
    '''
    top_10_colors_df = (
    units
    .dropna(subset=['VEH_COLOR_ID'])
    .groupBy('VEH_COLOR_ID')
    .count()
    .orderBy(desc('count'))
    .limit(10)
    .select('VEH_COLOR_ID')
    )

    top_10_colors = [row['VEH_COLOR_ID'] for row in top_10_colors_df.collect()]

    top_25_states_df = (
        person
        .groupBy('DRVR_LIC_STATE_ID')
        .agg(countDistinct('CRASH_ID').alias('offences'))
        .orderBy(desc('offences'))
        .limit(25)
        .select('DRVR_LIC_STATE_ID')
    )

    top_25_states = [row['DRVR_LIC_STATE_ID'] for row in top_25_states_df.collect()]

    filtered_person_df = person.filter(col('DRVR_LIC_STATE_ID').isin(top_25_states))

    person_charges_df = (
        filtered_person_df
        .select('CRASH_ID', 'UNIT_NBR', 'PRSN_NBR', 'DRVR_LIC_TYPE_ID')
        .join(charges, on=['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'], how='left')
    )

    filtered_person_charges_df = (
        person_charges_df
        .filter(col('CHARGE').contains('SPEED'))
        .filter(col('DRVR_LIC_TYPE_ID').contains('DRIVER LIC'))
    )

    filtered_units_df = units.filter(col('VEH_COLOR_ID').isin(top_10_colors))
    person_charges_units_df = (
        filtered_person_charges_df
        .join(filtered_units_df.select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID'), 
            on=['CRASH_ID', 'UNIT_NBR'], 
            how='left')
    )
    person_charges_units_df = person_charges_units_df.dropna(subset=['VEH_MAKE_ID'])

    top_5_makes_df = (
        person_charges_units_df
        .groupBy('VEH_MAKE_ID')
        .count()
        .orderBy(desc('count'))
        .limit(5)
        .select('VEH_MAKE_ID')
    )

    top_5_makes = [row['VEH_MAKE_ID'] for row in top_5_makes_df.collect()]
    return top_5_makes
