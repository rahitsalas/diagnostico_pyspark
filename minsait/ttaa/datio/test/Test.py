import unittest
from pyspark.sql import DataFrame
from minsait.ttaa.datio.common.naming.PlayerOutput import player_cat, potential_vs_overall


class TestStringMethods(unittest.TestCase):

    # positive test function to test if values
    # are almost equal with place
    def test_filter_player_cat_potential_vs_overall(self, df: DataFrame):
        check1 = df.filter(player_cat.column() == 'C')
        check2 = df.filter(player_cat.column() == 'D')

        self.assertGreater(check1.select(potential_vs_overall.column()), 1.15)
        self.assertGreater(check2.select(potential_vs_overall.column()), 1.25)
        print("test done")




