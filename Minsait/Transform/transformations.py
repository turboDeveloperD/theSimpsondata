from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
import Minsait.Constants.constants as c
from pyspark.sql.functions import max
import Minsait.Utils.utils as u
from pyspark.sql.types import *

class Transformation:

    def __int__(self):
        pass

    """
    Ejercicio 1
    """
    @staticmethod
    def select_columns_df(df: DataFrame) -> DataFrame:
        return df.select(f.col(c.SEASON).cast(IntegerType()),
                         f.col(c.TITLE),
                         f.col(c.NUMBER_SEASON).cast(FloatType()),
                         f.col(c.ORIGINAL_AIR_DATE),
                         f.col(c.ORIGINAL_AIR_YEAR),
                         f.col(c.PRODUCTION_CODE),
                         f.col(c.RATING).cast(FloatType()),
                         f.col(c.VOTES).cast(IntegerType()),
                         f.col(c.VIEWER_MILLI).cast(DoubleType())
                         ).filter(f.col(c.RATING).isNotNull() & f.col(c.VOTES).isNotNull() & f.col(c.VIEWER_MILLI).isNotNull() ).distinct()

    """
    Ejercicio 2
    """
    @staticmethod
    def best_Season(df: DataFrame)-> DataFrame:
        #names_Columns = [c.TITLE, c.NUMBER_SEASON, c.ORIGINAL_AIR_DATE, c.PRODUCTION_CODE, c.VOTES, c.VIEWER_MILLI ]

        best = df.groupBy(f.col(c.SEASON)).agg(
            f.sum(c.RATING).alias(c.RATING_TOTAL)
        )
        return best.orderBy(f.col(c.RATING_TOTAL).desc())

    """
        Ejecicio 4
    """
    @staticmethod
    def select_add_score(df : DataFrame) -> DataFrame:
        return df.select(*df.columns,
                         ( f.col(c.RATING) * f.col(c.VIEWER_MILLI) ).alias(c.SCORE) )\
            .orderBy(f.col(c.SCORE).desc())

    """
    Ejercicio 3
    """
    @staticmethod
    def best_year(df: DataFrame) -> DataFrame:
        best_y = df.groupBy(f.col(c.ORIGINAL_AIR_YEAR)).agg(
            f.sum(c.VIEWER_MILLI).alias(c.VIEWER_MILLI)
        )
        return best_y.orderBy(f.col(c.VIEWER_MILLI).desc())


    """
    Ejercicio 5 Parte 1
    """
    @staticmethod
    def group_Score(df: DataFrame) -> DataFrame:
        """ original"""
        windowScore = Window.partitionBy(f.col(c.SEASON)).orderBy(f.col(c.SCORE).desc())

        """ original"""
        return df.select(*df.columns,
                         #f.row_number().over(windowScore).alias("number"),
                         #f.rank().over(windowScore).alias("rank"),
                         f.dense_rank().over(windowScore).alias(c.TOP)
                         )


    """
    Ejercicio 5: Parte 2 
    """
    @staticmethod
    def filterScore(df: DataFrame) -> DataFrame:

        """ original
        filter1 = df.filter( (f.col(c.TOP) == 2) | (f.col(c.TOP) == 3)  | (f.col(c.TOP) == 1) )\
                    .orderBy(f.col(c.SEASON))
        """
        """
        Funcional
        """
        filter1 = df.filter( (f.col(c.TOP) == 2) | (f.col(c.TOP) == 3)  | (f.col(c.TOP) == 1) )
        return filter1

