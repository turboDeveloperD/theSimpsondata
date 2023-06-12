from pyspark.sql import SparkSession
import Minsait.Constants.constants as c
from Minsait.Transform.transformations import Transformation

def main():
    spark = SparkSession.builder.appName(c.APP_NAME).master(c.MODE).getOrCreate()
    simsomps = spark.read.option(c.DELIMITER, "|").option(c.HEADER,c.TRUE_STRING).csv(c.FILE_1)
    t = Transformation()

    first_select = t.select_columns_df(simsomps) # primer ejercicio
    best_session = t.best_Season(first_select) # segundo ejercicio
    best_year = t.best_year(first_select) # tercer ejercicio
    score_df = t.select_add_score(first_select) # cuarto ejercicio

    group_W = t.group_Score(score_df)
    #group_W.show()
    #group_W.printSchema()
    fil_S = t.filterScore(group_W)

    print("Primer ejercicio")
    first_select.show() # ver
    print("Segundo ejercicio")
    best_session.show()
    print("Tercer ejercicio")
    best_year.show()
    print("Cuarto ejercicio")
    score_df.show()
    print("Quinto ejercicio")
    fil_S.show()
    fil_S.write.mode(c.MODE_OVER).parquet(c.FILE_2)

    #parquet1 = spark.read.parquet("Resources/Data/output/part-00000-5a09dc06-0409-4c36-81ba-0824e9f40666-c000.snappy.parquet")
    #parquet1.show()




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
