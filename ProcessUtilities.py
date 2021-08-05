from pyspark.sql.functions import mean, format_number
class ProcessUtilities:
    @staticmethod
    def get_average(dataframe):
        """
            This function computes the average Covid cases on each school unit according to each gender.
        :param dataframe: dataframe that contains the data about the school cases
        :return: dataframe with the average Covid cases on each school unit
        """
        ProcessedData = dataframe.groupBy('School Unit Name', 'Gender').agg(
            format_number(mean("Elementary School Cases"), 2).alias("Elementary School Average"),
            format_number(mean("Middle School Cases"), 2).alias("Middle School Average"),
            format_number(mean("High School Cases"), 2).alias("High School Average"))
        ProcessedData = ProcessedData.fillna(0)
        ProcessedData = ProcessedData.na.drop(subset=['Gender'])
        return ProcessedData