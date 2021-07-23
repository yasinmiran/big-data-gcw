"""
This file holds all the User-Defined-Functions for pyspark.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from analytics.functions import get_browser_category_by_ua, get_os_from_ua, get_device_from_ua, get_browser_vendor

toDevice = udf(get_device_from_ua, ArrayType(elementType=StringType()))
toOperatingSystem = udf(get_os_from_ua, StringType())
toBrowserType = udf(get_browser_category_by_ua, StringType())
toBrowserVendor = udf(get_browser_vendor, StringType())
