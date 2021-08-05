class IOUtilities:
    @staticmethod
    def read_and_process(spark):
        """
            This function reads from a CSV file and creates a dataframe with its content.
        :param spark: current spark session
        :return: dataframe with the content of the CSV file
        """
        dataFr = spark.read.option('header', 'True').option('inferSchema', 'True').option('nullValue', 'null').option(
            'dataFrame', 'dd/mm/yy').csv(
            "C:\\Users\\LAPTOP_MIA\\PycharmProjects\\Covid\\CovidCases.csv")
        dataFr = dataFr.fillna(
            {'High School Cases': 0, 'Middle School Cases': 0, 'Elementary School Cases': 0, 'Gender': ''})
        return dataFr
    @staticmethod
    def save_dataframe(dataframe):
        """
            This function saves the data from a dataframe to a table in mysql
        :param dataframe:  dataframe that contains the data about the average cases on each unit
        """

        dataframe.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/covid',
                                               driver='com.mysql.cj.jdbc.Driver',
                                               dbtable='cases',
                                               user='root',
                                               ).mode('append').save()