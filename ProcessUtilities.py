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
            format_number(mean("Elementary School Cases"), 2).alias("ElementarySchoolAverage"),
            format_number(mean("Middle School Cases"), 2).alias("MiddleSchoolAverage"),
            format_number(mean("High School Cases"), 2).alias("HighSchoolAverage"))
        ProcessedData = ProcessedData.withColumnRenamed("School Unit Name", "SchoolUnitName")
        ProcessedData = ProcessedData.fillna(0)
        ProcessedData = ProcessedData.na.drop(subset=['Gender'])
        return ProcessedData

    @staticmethod
    def unite_averages(dataframe):
        """
            This function computes the average of the existing averages of covid cases on each school unit according to
        each gender.
        :param dataframe: dataframe that contains the data about the school average cases
        :return: dataframe with the average Covid cases on each school unit
        """
        ProcessedData = dataframe.groupBy('SchoolUnitName', 'Gender').agg(
            format_number(mean("ElementarySchoolAverage"), 2).alias("ElementarySchoolAverage"),
            format_number(mean("MiddleSchoolAverage"), 2).alias("MiddleSchoolAverage"),
            format_number(mean("HighSchoolAverage"), 2).alias("HighSchoolAverage"))
        ProcessedData = ProcessedData.fillna(0)
        ProcessedData = ProcessedData.na.drop(subset=['Gender'])
        return ProcessedData
