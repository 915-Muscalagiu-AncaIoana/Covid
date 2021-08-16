import mysql.connector


class IOUtilities:
    def __init__(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="covid"
        )
        mycursor = mydb.cursor()
        self.conn = mydb;
        self.cursor = mycursor

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    @staticmethod
    def read_and_process(spark, name):
        """
            This function reads from a CSV file and creates a dataframe with its content.
        :param spark: current spark session
        :param name: name of the CSV file in which the data is loaded
        :return: dataframe with the content of the CSV file
        """
        filename = "C:\\Users\\LAPTOP_MIA\\PycharmProjects\\Covid\\" + name
        dataFr = spark.read.option('header', 'True').option('inferSchema', 'True').option('nullValue', 'null').option(
            'dataFrame', 'dd/mm/yy').csv(filename).cache()
        dataFr = dataFr.fillna(
            {'High School Cases': 0, 'Middle School Cases': 0, 'Elementary School Cases': 0, 'Gender': ''})
        dataFr.count()
        return dataFr

    @staticmethod
    def save_dataframe(dataframe, table_name):
        """
            This function saves the data from a dataframe to a table in mysql
        :param dataframe:  dataframe that contains the data about the average cases on each unit
        :param table_name: the name of the table in which the dataframe will be saved
        """
        dataframe.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/covid',
                                               driver='com.mysql.cj.jdbc.Driver',
                                               dbtable=table_name,
                                               user='root',
                                               truncate_table=True,
                                               usestagingtable=False
                                               ).mode('overwrite').save()

    def overwrite_table(self, data_frame, aux_table_name, table_name):
        """
           This function overwrites the data from a mysql table with the data from a dataframe
        :param data_frame: the dataframe containing the new data
        :param aux_table_name: the name of an intermediate table to store the data
        :param table_name: the name of the table to be updated
        """
        self.save_dataframe(data_frame, aux_table_name)
        self.cursor.execute('DROP TABLE ' + table_name)
        self.cursor.execute("ALTER TABLE " + aux_table_name + " RENAME " + table_name)

    @staticmethod
    def get_dataframe_from_db(spark, table_name):
        """
            This function retrieves the data from mysql and creates a dataframe containing it
        :param spark: current spark session
        :param table_name: the name of the table from which the data is extracted
        :return: dataframe with the content of the mysql table
        """
        dataframe = spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/covid',
                                                      driver='com.mysql.cj.jdbc.Driver',
                                                      dbtable=table_name,
                                                      user='root', ).load()

        return dataframe
