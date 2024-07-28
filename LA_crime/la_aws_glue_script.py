import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

# Script generated for node Custom Fill Values
def FillValues(glueContext, dfc) -> DynamicFrameCollection:
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # fill empty string will null value
    df = df.replace({'': None})

    # fill null value with value based on different column
    df_filled = df.na.fill({'Mocodes': '0000', 'Premis Desc': 'Unknown', 
                            'Weapon Used Cd': 500, 'Weapon Desc': 'UNKNOWN WEAPON/OTHER WEAPON'
    })

    # replace value with different its real name
    df_replaced = df_filled.withColumn('vict sex',
                                       when(df_filled['vict sex'] == 'M', 'Male')
                                       .when(df_filled['vict sex'] == 'F', 'Female')
                                       .when(df_filled['vict sex'] == 'H', 'Other')
                                       .otherwise('Unknown'))
    
    df_replaced = df_replaced.withColumn('vict descent',
                                         when(df_filled['vict descent'] == 'A', 'Other Asian')
                                         .when(df_filled['vict descent'] == 'B', 'Black')
                                         .when(df_filled['vict descent'] == 'C', 'Chinese')
                                         .when(df_filled['vict descent'] == 'D', 'Cambodian')
                                         .when(df_filled['vict descent'] == 'F', 'Filipino')
                                         .when(df_filled['vict descent'] == 'G', 'Guamanian')
                                         .when(df_filled['vict descent'] == 'H', 'Hispanic/Latin/Mexican')
                                         .when(df_filled['vict descent'] == 'I', 'American Indian/Alaskan Native')
                                         .when(df_filled['vict descent'] == 'J', 'Japanese')
                                         .when(df_filled['vict descent'] == 'K', 'Korean')
                                         .when(df_filled['vict descent'] == 'L', 'Laotian')
                                         .when(df_filled['vict descent'] == 'O', 'Other')
                                         .when(df_filled['vict descent'] == 'P', 'Pacific Islander')
                                         .when(df_filled['vict descent'] == 'S', 'Samoam')
                                         .when(df_filled['vict descent'] == 'U', 'Hawaiian')
                                         .when(df_filled['vict descent'] == 'V', 'Vietnamese')
                                         .when(df_filled['vict descent'] == 'W', 'White')
                                         .when(df_filled['vict descent'] == 'Z', 'Asian Indian')
                                         .otherwise('Unknown'))
    
    #print(df_replaced.select('vict sex', 'vict descent').show())

    dyf_filled = DynamicFrame.fromDF(df_replaced, glueContext, "dyf_filled")

    return DynamicFrameCollection({"Custom": dyf_filled}, glueContext)


def ConvertToDate(glueContext, dfc) -> DynamicFrameCollection:
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()
    
    df_replaced = df.withColumns({'date occ': substring(df['date occ'], 1, 11),
                                  'time occ': lpad(df['time occ'], 4, '0')})
    
    df_replaced = df_replaced.withColumn('date occ', 
                                         concat(df_replaced['date occ'], 
                                                substring(df_replaced['time occ'], 1, 2), 
                                                lit(':'), 
                                                substring(df_replaced['time occ'], 3, 2), 
                                                lit(':00')))

    df_converted = df_replaced.withColumn('date occ',
                                 to_timestamp(df_replaced['date occ'], "MM/dd/yyyy HH:mm:ss"))
    
    dyf_converted = DynamicFrame.fromDF(df_converted, glueContext, "dyf_converted")

    return DynamicFrameCollection({'ConvertToDate': dyf_converted}, glueContext)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(database="crimes", table_name="la")

# Script generated for node Change Schema
ChangeSchema = ApplyMapping.apply(frame=AWSGlueDataCatalog, mappings=[("dr_no", "long", "dr_no", "long"), 
                                                                        ("date occ", "string", "date occ", "string"), 
                                                                        ("time occ", "long", "time occ", "string"), 
                                                                        ("area", "long", "area", "long"), 
                                                                        ("area name", "string", "area name", "string"), 
                                                                        ("rpt dist no", "long", "rpt dist no", "long"), 
                                                                        ("crm cd", "long", "crm cd", "long"), 
                                                                        ("crm cd desc", "string", "crm cd desc", "string"), 
                                                                        ("mocodes", "string", "mocodes", "string"), 
                                                                        ("vict age", "long", "vict age", "long"), 
                                                                        ("vict sex", "string", "vict sex", "string"), 
                                                                        ("vict descent", "string", "vict descent", "string"), 
                                                                        ("premis cd", "long", "premis cd", "long"), 
                                                                        ("premis desc", "string", "premis desc", "string"), 
                                                                        ("weapon desc", "string", "weapon desc", "string"), 
                                                                        ("status", "string", "status", "string"), 
                                                                        ("status desc", "string", "status desc", "string"), 
                                                                        ("lat", "double", "lat", "double"), 
                                                                        ("lon", "double", "lon", "double"), 
                                                                        ("weapon used cd", "long", "weapon used cd", "long")], 
                                                                        )

# Script generated for node Drop Duplicates
DropDuplicates =  DynamicFrame.fromDF(ChangeSchema.toDF().dropDuplicates(), glueContext, "DropDuplicates")

# Script generated for node Custom Fill Null Values
CustomFillValues = FillValues(glueContext, DynamicFrameCollection({"DropDuplicates": DropDuplicates}, glueContext))

# Script generated for node Select From Collection
SelectFromCollections = SelectFromCollection.apply(dfc=CustomFillValues, key=list(CustomFillValues.keys())[0], transformation_ctx="SelectFromCollection")

# Script generated for node Custom Convert To Date
CustomConvertToDate = ConvertToDate(glueContext, DynamicFrameCollection({"SelectFromCollections": SelectFromCollections}, glueContext))

# Script generated for node Select From Collection
SelectFromCollections = SelectFromCollection.apply(dfc=CustomConvertToDate, key=list(CustomConvertToDate.keys())[0], transformation_ctx="SelectFromCollection")

print(SelectFromCollections.toDF().show(5))
# Script generated for node Amazon S3
AmazonS3 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollections, 
                                                        connection_type="s3", 
                                                        format="csv", 
                                                        connection_options={"path": "s3://edward-s3-crime-v1/la_transformed/"}, 
                                                        transformation_ctx="AmazonS3")

#job.commit()
