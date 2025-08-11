# utilities.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def full_name(first_name, last_name):
    """Combine first_name and last_name into a single full_name."""
    if first_name is None:
        first_name = ""
    if last_name is None:
        last_name = ""
    return (first_name + " " + last_name).strip()
