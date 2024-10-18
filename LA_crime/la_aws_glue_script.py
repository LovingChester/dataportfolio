import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import pyspark.pandas as ps

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# read from Glue catalog
CrimeDyf = glueContext.create_dynamic_frame.from_catalog(database="crimes", table_name="la")

# keep needed columns
CrimeDyf = ApplyMapping.apply(frame=CrimeDyf, mappings=[("dr_no", "long", "dr_no", "long"), 
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

# drop duplicates
CrimeDyf = DynamicFrame.fromDF(CrimeDyf.toDF().dropDuplicates(), glueContext, "DropDuplicates")

# fill null values
CrimeDf = ps.DataFrame(CrimeDyf.toDF())
CrimeDf.replace({'': None}, inplace=True)
CrimeDf.fillna({'mocodes': '0000', 
                'premis desc': 'Unknown', 
                'weapon used cd': 500, 
                'weapon desc': 'UNKNOWN WEAPON/OTHER WEAPON'
                }, inplace=True)

# convert date occ to datetime obj
CrimeDf['time occ'] = CrimeDf['time occ'].str.pad(width=4, side='left', fillchar='0')
CrimeDf['date occ'] = CrimeDf['date occ'].str.slice(start=0, stop=11)
CrimeDf['date occ'] = CrimeDf['date occ'] + CrimeDf['time occ'].str.slice(start=0, stop=2) + ':' + CrimeDf['time occ'].str.slice(start=2) + ':00'
CrimeDf['date occ'] = ps.to_datetime(CrimeDf['date occ'], format="%m/%d/%Y %H:%M:%S")

# replace values with it real name for better interpretation
CrimeDf['vict sex'] = CrimeDf['vict sex'].replace({'M': 'Male',
                                                    'F': 'Female',
                                                    'H': 'Other',
                                                    'X': 'Unknown',
                                                    '-': 'Unknown'})

CrimeDf['vict descent'] = CrimeDf['vict descent'].replace({'A': 'Other Asian',
                                                            'B': 'Black',
                                                            'C': 'Chinese',
                                                            'D': 'Cambodian',
                                                            'F': 'Filipino',
                                                            'G': 'Guamanian',
                                                            'H': 'Hispanic/Latin/Mexican',
                                                            'I': 'American Indian/Alaskan Native',
                                                            'J': 'Japanese',
                                                            'K': 'Korean',
                                                            'L': 'Laotian',
                                                            'O': 'Other',
                                                            'P': 'Pacific Islander',
                                                            'S': 'Samoam',
                                                            'U': 'Hawaiian',
                                                            'V': 'Vietnamese',
                                                            'W': 'White',
                                                            'Z': 'Asian Indian',
                                                            'X': 'Unknown',
                                                            '-': 'Unknown'})

# convert to dynamic frame
CrimeDyf = DynamicFrame.fromDF(CrimeDf.to_spark(), glueContext, "resultDyf")
#print(CrimeDyf.toDF().show(5))

# upload to AmazonS3
# AmazonS3csv = glueContext.write_dynamic_frame.from_options(frame=CrimeDyf, 
#                                                             connection_type="s3", 
#                                                             format="csv", 
#                                                             connection_options={"path": "s3://edward-s3-crime-v1/la_transformed/partitions/"}, 
#                                                             transformation_ctx="AmazonS3csv")

AmazonS3json = glueContext.write_dynamic_frame.from_options(frame=CrimeDyf, 
                                                            connection_type="s3", 
                                                            format="json", 
                                                            connection_options={"path": "s3://edward-s3-crime-v1/la_transformed_json"}, 
                                                            transformation_ctx="AmazonS3json")

job.commit()