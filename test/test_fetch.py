import unittest
import pju
import pandas as pd
import vcr
from pandas.api.types import is_integer_dtype, is_string_dtype, is_period_dtype

class TestFetch(unittest.TestCase):

    @vcr.use_cassette('test/fixtures/vcr_cassettes/test_fetch_payouts.yaml')
    def test_fetch_payouts(self):
        df = pju.fetch_payouts()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df.index, pd.PeriodIndex)
        self.assertIsInstance(df.index.freq, pd.offsets.MonthEnd)
        self.assertEqual(len(df), 155)

        for col in ['employees_by_hours', 'employees', 'employees_all', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'N', 'O']:
            self.assertTrue(is_integer_dtype(df[col]), f'{col} is not an integer dtype')

        row = df.iloc[0]
        self.assertEqual(row['employees_by_hours'], 157011)
        self.assertEqual(row['employees'], 160867)
        self.assertEqual(row['employees_all'], 165345)
        self.assertEqual(row['A'], 210004802)
        self.assertEqual(row['B'], 31121056)
        self.assertEqual(row['C'], 25266478)
        self.assertEqual(row['D'], 7125292)
        self.assertEqual(row['E'], 7529438)
        self.assertEqual(row['F'], 158712)
        self.assertEqual(row['G'], 7631308)
        self.assertEqual(row['H'], 5481991)
        self.assertEqual(row['I'], 26537)
        self.assertEqual(row['J'], 1862162)
        self.assertEqual(row['L'], 16607)
        self.assertEqual(row['N'], 52239)
        self.assertEqual(row['O'], 2420457)

    @vcr.use_cassette('test/fixtures/vcr_cassettes/test_fetch_payout_by_budget_user_group.yaml')
    def test_fetch_payout_by_budget_user_group(self):
        df = pju.fetch_payout_by_budget_user_group(2018, 1)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 23)

        self.assertIsInstance(df.index, pd.MultiIndex)
        for col in ['employees', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'N', 'O']:
            self.assertTrue(is_integer_dtype(df[col]), f'{col} is not an integer dtype')

        row = df.iloc[0]
        self.assertEqual(row['employees'], 270)
        self.assertEqual(row['A'], 254925)
        self.assertEqual(row['B'], 69788)
        self.assertEqual(row['C'], 37267)
        self.assertEqual(row['D'], 1226)
        self.assertEqual(row['E'], 4849)
        self.assertEqual(row['F'], 944)
        self.assertEqual(row['G'], 8559)
        self.assertEqual(row['H'], 5963)
        self.assertEqual(row['I'], 41941)
        self.assertEqual(row['J'], 1692)
        self.assertEqual(row['L'], 0)
        self.assertEqual(row['N'], 0)
        self.assertEqual(row['O'], 0)


    @vcr.use_cassette('test/fixtures/vcr_cassettes/test_fetch_payout_by_budget_user.yaml')
    def test_fetch_payout_by_budget_user(self):
        df = pju.fetch_payout_by_budget_user(2018, 1)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df.index, pd.MultiIndex)
        self.assertEqual(len(df), 1907)

        row = df.iloc[0]
        self.assertEqual(row['group_id'], '0.')
        self.assertEqual(row['group_name'], 'PRORAČUNSKI UPORABNIK NI ČLAN PODSKUPINE RPU')
        self.assertEqual(row['budget_user_name'], 'Javni gospodarski zavod, Protokolarne storitve Republike Slovenije')
        self.assertEqual(row['employees'], 191)
        self.assertEqual(row['gross_salary'], 246556)
        # self.assertEqual(row['A'], 246556)
        # self.assertEqual(row['B'], 0)
        self.assertEqual(row['C'], 23987)
        self.assertEqual(row['D'], 1226)
        self.assertEqual(row['E'], 4685)
        # self.assertEqual(row['F'], 0)
        # self.assertEqual(row['G'], 0)
        # self.assertEqual(row['H'], 0)
        self.assertEqual(row['I'], 30046)
        self.assertEqual(row['J'], 0)
        # self.assertEqual(row['L'], 0)
        # self.assertEqual(row['N'], 0)
        self.assertEqual(row['O'], 0)
        print(row)


    @vcr.use_cassette('test/fixtures/vcr_cassettes/test_fetch_payouts_job_title.yaml')
    def test_fetch_payouts_job_title(self):
        df = pju.fetch_payouts_job_title(2018, 1)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df.index, pd.MultiIndex)
        self.assertEqual(len(df), 33569)

        row = df.iloc[0]
        self.assertEqual(row['group_id'], '0.')
        self.assertEqual(row['group_name'], 'PRORAČUNSKI UPORABNIK NI ČLAN PODSKUPINE RPU')
        self.assertEqual(row['budget_user_id'], '00078')
        self.assertEqual(row['budget_user_name'], 'Javni gospodarski zavod, Protokolarne storitve Republike Slovenije')
        self.assertEqual(row['employees'], 1)
        self.assertEqual(row['gross_salary'], 3824)
        self.assertEqual(row['C'], 176)
        self.assertEqual(row['D'], 0)
        self.assertEqual(row['E'], 0)
        self.assertEqual(row['F'], 323)
        self.assertEqual(row['I'], 42)
        self.assertEqual(row['J'], 0)
        self.assertEqual(row['O'], 0)
