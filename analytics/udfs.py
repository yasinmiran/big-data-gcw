"""
This file holds all the User-Defined-Functions for pyspark.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

from analytics.functions import get_browser_category_by_ua, \
    get_os_from_ua, get_device_from_ua, get_browser_vendor_name, \
    get_client_type

get_device_vendor = udf(get_device_from_ua, ArrayType(elementType=StringType()))
get_operating_system = udf(get_os_from_ua, StringType())
get_browser_category = udf(get_browser_category_by_ua, StringType())
get_browser_vendor = udf(get_browser_vendor_name, StringType())
get_client_device_type = udf(get_client_type, StringType())
