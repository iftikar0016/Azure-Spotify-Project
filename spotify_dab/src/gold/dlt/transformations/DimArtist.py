import dlt

expectations = {
    'rule no 1' : 'artist_id is NOT NULL'
}


@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimartist_stg():
    df = spark.readStream.table('spotify_cata.silver.dimartist')
    return df

dlt.create_streaming_table(name='dimartist', expect_all_or_drop= expectations)

dlt.create_auto_cdc_flow(
    target='dimartist',
    source='dimartist_stg',
    keys=['artist_id'],
    sequence_by='updated_at',
    stored_as_scd_type= 2,
    once= False
)